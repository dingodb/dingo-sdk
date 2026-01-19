// Copyright (c) 2023 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "dingosdk/client.h"

#include <unistd.h>

#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/logging.h"
#include "dingosdk/document.h"
#include "dingosdk/metric.h"
#include "dingosdk/status.h"
#include "dingosdk/vector.h"
#include "dingosdk/version.h"
#include "fmt/core.h"
#include "glog/logging.h"
#include "proto/common.pb.h"
#include "proto/coordinator.pb.h"
#include "sdk/client_internal_data.h"
#include "sdk/client_stub.h"
#include "sdk/common/helper.h"
#include "sdk/common/param_config.h"
#include "sdk/document/document_index.h"
#include "sdk/document/document_index_cache.h"
#include "sdk/document/document_index_creator_internal_data.h"
#include "sdk/rawkv/raw_kv_batch_compare_and_set_task.h"
#include "sdk/rawkv/raw_kv_batch_delete_task.h"
#include "sdk/rawkv/raw_kv_batch_get_task.h"
#include "sdk/rawkv/raw_kv_batch_put_if_absent_task.h"
#include "sdk/rawkv/raw_kv_batch_put_task.h"
#include "sdk/rawkv/raw_kv_compare_and_set_task.h"
#include "sdk/rawkv/raw_kv_delete_range_task.h"
#include "sdk/rawkv/raw_kv_delete_task.h"
#include "sdk/rawkv/raw_kv_get_task.h"
#include "sdk/rawkv/raw_kv_internal_data.h"
#include "sdk/rawkv/raw_kv_put_if_absent_task.h"
#include "sdk/rawkv/raw_kv_put_task.h"
#include "sdk/rawkv/raw_kv_scan_task.h"
#include "sdk/region_creator_internal_data.h"
#include "sdk/rpc/coordinator_rpc.h"
#include "sdk/sdk_version.h"
#include "sdk/transaction/txn_impl.h"
#include "sdk/transaction/txn_internal_data.h"
#include "sdk/transaction/txn_manager.h"
#include "sdk/utils/async_util.h"
#include "sdk/utils/net_util.h"
#include "sdk/vector/diskann/vector_diskann_status_by_index_task.h"
#include "sdk/vector/vector_index.h"
#include "sdk/vector/vector_index_cache.h"
#include "sdk/vector/vector_index_creator_internal_data.h"

namespace dingodb {
namespace sdk {

static const uint32_t kScanRegionLimit = 1000;

void ShowSdkVersion() { DingoSdkLogVersion(); }
std::vector<std::pair<std::string, std::string>> GetSdkVersion() { return DingoSdkVersion(); }

Status Client::BuildAndInitLog(std::string addrs, Client** client) {
  static std::once_flag init;
  std::call_once(init, [&]() { google::InitGoogleLogging("dingo_sdk"); });

  return BuildFromAddrs(addrs, client);
}

Status Client::BuildFromAddrs(std::string addrs, Client** client) {
  if (addrs.empty()) {
    return Status::InvalidArgument(fmt::format("addrs:{} is empty", addrs));
  };

  std::vector<EndPoint> endpoints = StringToEndpoints(addrs);
  if (endpoints.empty()) {
    return Status::InvalidArgument(fmt::format("invalid addrs:{}", addrs));
  }

  Client* tmp = new Client();
  Status s = tmp->Init(endpoints);
  if (!s.ok()) {
    delete tmp;
    return s;
  }

  *client = tmp;
  return s;
}

Status Client::BuildFromEndPoint(std::vector<EndPoint>& endpoints, Client** client) {
  if (endpoints.empty()) {
    return Status::InvalidArgument("endpoints is empty");
  }

  Client* tmp = new Client();
  Status s = tmp->Init(endpoints);
  if (!s.ok()) {
    delete tmp;
    return s;
  }

  *client = tmp;
  return s;
}

Status Client::Build(std::string naming_service_url, Client** client) {
  if (naming_service_url.empty()) {
    return Status::InvalidArgument("naming_service_url is empty");
  };

  if (!IsServiceUrlValid(naming_service_url)) {
    return Status::InvalidArgument(fmt::format("invalid naming_service_url:{}", naming_service_url));
  }

  std::vector<EndPoint> endpoints = FileNamingServiceUrlEndpoints(naming_service_url);

  Client* tmp = new Client();
  Status s = tmp->Init(endpoints);
  if (!s.ok()) {
    delete tmp;
    return s;
  }

  *client = tmp;
  return s;
}

Client::Client() : data_(new Client::Data()) {}

Client::~Client() { delete data_; }

Status Client::Init(const std::vector<EndPoint>& endpoints) {
  CHECK(!endpoints.empty());
  if (data_->init) {
    return Status::IllegalState("forbidden multiple init");
  }

  auto tmp = std::make_unique<ClientStub>();
  Status open = tmp->Open(endpoints);
  if (open.IsOK()) {
    data_->init = true;
    data_->stub = std::move(tmp);
  }
  return open;
}

Status Client::NewCoordinator(Coordinator** coordinator) {
  *coordinator = new Coordinator(*data_->stub);
  return Status::OK();
}

Status Client::NewVersion(Version** version) {
  *version = new Version(*data_->stub);
  return Status::OK();
}

Status Client::NewRawKV(RawKV** raw_kv) {
  *raw_kv = new RawKV(new RawKV::Data(*data_->stub));
  return Status::OK();
}

Status Client::NewTransaction(const TransactionOptions& options, Transaction** txn) {
  auto txn_impl = std::make_shared<TxnImpl>(*data_->stub, options, data_->stub->GetTxnManager());
  Transaction* tmp_txn = new Transaction(new Transaction::Data(*data_->stub, txn_impl));
  Status s = tmp_txn->Begin();
  if (!s.ok()) {
    delete tmp_txn;
    return s;
  }
  s = data_->stub->GetTxnManager()->RegisterTxn(std::move(txn_impl));
  if (!s.ok()) {
    delete tmp_txn;
    return s;
  }

  *txn = tmp_txn;
  return s;
}

Status Client::NewRegionCreator(RegionCreator** creator) {
  *creator = new RegionCreator(new RegionCreator::Data(*data_->stub));
  return Status::OK();
}

Status Client::IsCreateRegionInProgress(int64_t region_id, bool& out_create_in_progress) {
  return data_->stub->GetAdminTool()->IsCreateRegionInProgress(region_id, out_create_in_progress);
}

Status Client::DropRegion(int64_t region_id) {
  data_->stub->GetMetaCache()->RemoveRegion(region_id);
  return data_->stub->GetAdminTool()->DropRegion(region_id);
}

Status Client::DropRegion(const std::string& start_key, const std::string& end_key) {
  do {
    std::vector<int64_t> region_ids;
    auto status = ScanRegions(start_key, end_key, kScanRegionLimit, region_ids);
    if (!status.ok()) {
      DINGO_LOG(ERROR) << fmt::format("[sdk] scan region fail, status({} {}).", status.Errno(), status.ToString());
      return status;
    }

    for (const auto& region_id : region_ids) {
      status = DropRegion(region_id);
      if (!status.ok()) {
        DINGO_LOG(ERROR) << fmt::format("[sdk] drop region fail, status({} {}).", status.Errno(), status.ToString());
        return status;
      }
    }

    if (region_ids.size() < kScanRegionLimit) break;
  } while (true);

  return Status::OK();
}

Status Client::NewVectorClient(VectorClient** client) {
  *client = new VectorClient(*data_->stub);
  return Status::OK();
}

Status Client::NewVectorIndexCreator(VectorIndexCreator** index_creator) {
  *index_creator = new VectorIndexCreator(new VectorIndexCreator::Data(*data_->stub));
  return Status::OK();
}

Status Client::GetVectorIndexId(int64_t schema_id, const std::string& index_name, int64_t& out_index_id) {
  return data_->stub->GetVectorIndexCache()->GetIndexIdByKey(EncodeVectorIndexCacheKey(schema_id, index_name),
                                                             out_index_id);
}

Status Client::GetVectorIndex(int64_t schema_id, const std::string& index_name,
                              std::shared_ptr<VectorIndex>& out_vector_index) {
  return data_->stub->GetVectorIndexCache()->GetVectorIndexByKey(EncodeVectorIndexCacheKey(schema_id, index_name),
                                                                 out_vector_index);
}

Status Client::GetVectorIndexById(int64_t index_id, std::shared_ptr<VectorIndex>& out_vector_index) {
  return data_->stub->GetVectorIndexCache()->GetVectorIndexById(index_id, out_vector_index);
}

Status Client::DropVectorIndexById(int64_t index_id) {
  // check diskann region status
  std::shared_ptr<VectorIndex> tmp;
  DINGO_RETURN_NOT_OK(data_->stub->GetVectorIndexCache()->GetVectorIndexById(index_id, tmp));
  if (tmp->GetVectorIndexType() == kDiskAnn) {
    StateResult result;
    VectorStatusByIndexTask task(*data_->stub, index_id, result);
    DINGO_RETURN_NOT_OK(task.Run());
    for (auto& res : result.region_states) {
      if (res.state == kLoading || res.state == kBuilding) {
        std::string msg = "region id : " + std::to_string(res.region_id) +
                          " status : " + RegionStateToString(res.state) + " cannot delete index";
        DINGO_LOG(WARNING) << msg;
        return Status::Incomplete(msg);
      }
    }
  }

  data_->stub->GetVectorIndexCache()->RemoveVectorIndexById(index_id);
  data_->stub->GetAutoIncrementerManager()->RemoveIndexIncrementerById(index_id);
  return data_->stub->GetAdminTool()->DropIndex(index_id);
}

Status Client::DropVectorIndexByName(int64_t schema_id, const std::string& index_name) {
  int64_t index_id{0};

  Status s =
      data_->stub->GetVectorIndexCache()->GetIndexIdByKey(EncodeVectorIndexCacheKey(schema_id, index_name), index_id);

  if (!s.ok()) {
    return s.Errno() == pb::error::Errno::EINDEX_NOT_FOUND ? Status::OK() : s;
  }

  CHECK_GT(index_id, 0);
  return DropVectorIndexById(index_id);
}

Status Client::NewDocumentClient(DocumentClient** client) {
  *client = new DocumentClient(*data_->stub);
  return Status::OK();
}

Status Client::NewDocumentIndexCreator(DocumentIndexCreator** out_creator) {
  *out_creator = new DocumentIndexCreator(new DocumentIndexCreator::Data(*data_->stub));
  return Status::OK();
}

Status Client::GetDocumentIndexId(int64_t schema_id, const std::string& doc_name, int64_t& doc_index_id) {
  return data_->stub->GetDocumentIndexCache()->GetIndexIdByKey(EncodeDocumentIndexCacheKey(schema_id, doc_name),
                                                               doc_index_id);
}

Status Client::DropDocumentIndexById(int64_t index_id) {
  data_->stub->GetDocumentIndexCache()->RemoveDocumentIndexById(index_id);
  data_->stub->GetAutoIncrementerManager()->RemoveIndexIncrementerById(index_id);
  return data_->stub->GetAdminTool()->DropIndex(index_id);
}

Status Client::DropDocumentIndexByName(int64_t schema_id, const std::string& index_name) {
  int64_t index_id{0};

  Status s = data_->stub->GetDocumentIndexCache()->GetIndexIdByKey(EncodeDocumentIndexCacheKey(schema_id, index_name),
                                                                   index_id);

  if (!s.ok()) {
    return s.Errno() == pb::error::Errno::EINDEX_NOT_FOUND ? Status::OK() : s;
  }

  CHECK_GT(index_id, 0);
  return DropDocumentIndexById(index_id);
}

Status Client::GetDocumentIndex(int64_t schema_id, const std::string& index_name,
                                std::shared_ptr<DocumentIndex>& out_doc_index) {
  return data_->stub->GetDocumentIndexCache()->GetDocumentIndexByKey(EncodeDocumentIndexCacheKey(schema_id, index_name),
                                                                     out_doc_index);
}

Status Client::GetDocumentIndexById(int64_t index_id, std::shared_ptr<DocumentIndex>& out_doc_index) {
  return data_->stub->GetDocumentIndexCache()->GetDocumentIndexById(index_id, out_doc_index);
}

Status Client::GetStoreOwnMetrics(std::vector<int64_t> store_ids,
                                  std::map<std::int64_t, StoreOwnMetics>& store_id_to_store_own_metrics) {
  GetStoreOwnMetricsRpc rpc;
  rpc.MutableRequest()->mutable_store_ids()->Add(store_ids.begin(), store_ids.end());
  Status status = data_->stub->GetCoordinatorRpcController()->SyncCall(rpc);
  if (!status.IsOK()) {
    DINGO_LOG(ERROR) << fmt::format("Get store own metrics fail, error: {} {}", status.Errno(), status.ToString());
    return status;
  }
  if (!store_ids.empty()) {
    CHECK(rpc.Response()->store_own_metrics_size() == store_ids.size());
  }
  store_id_to_store_own_metrics.clear();
  for (const auto& store_own_metrics : rpc.Response()->store_own_metrics()) {
    CHECK(store_own_metrics.id() != 0);

    StoreOwnMetics store_metrics;
    store_metrics.store_id = store_own_metrics.id();
    store_metrics.process_used_memory = store_own_metrics.process_used_memory();
    store_metrics.system_available_memory = store_own_metrics.system_available_memory();
    store_metrics.system_total_memory = store_own_metrics.system_total_memory();
    store_metrics.system_total_swap = store_own_metrics.system_total_swap();
    store_metrics.system_free_swap = store_own_metrics.system_free_swap();
    store_metrics.system_free_memory = store_own_metrics.system_free_memory();
    store_metrics.system_cpu_usage = store_own_metrics.system_cpu_usage();
    store_metrics.system_total_capacity = store_own_metrics.system_total_capacity();
    store_metrics.system_free_capacity = store_own_metrics.system_free_capacity();
    store_metrics.system_capacity_usage =
        static_cast<float>(store_own_metrics.system_total_capacity() - store_own_metrics.system_free_capacity()) /
        static_cast<float>(store_own_metrics.system_total_capacity()) * 100;
    store_metrics.system_shared_memory = store_own_metrics.system_shared_memory();
    store_metrics.system_buffer_memory = store_own_metrics.system_buffer_memory();
    store_metrics.system_cached_memory = store_own_metrics.system_cached_memory();

    store_id_to_store_own_metrics[store_own_metrics.id()] = store_metrics;
  }
  return status;
}

Status Client::ScanRegions(std::string start_key, std::string end_key, uint64_t limit,
                           std::vector<int64_t>& region_ids) {
  ScanRegionsRpc rpc;
  rpc.MutableRequest()->set_key(start_key);
  rpc.MutableRequest()->set_range_end(end_key);
  rpc.MutableRequest()->set_limit(limit);
  Status status = data_->stub->GetCoordinatorRpcController()->SyncCall(rpc);
  if (!status.IsOK()) {
    DINGO_LOG(ERROR) << fmt::format("scan regions fail, error: {} {}", status.Errno(), status.ToString());
    return status;
  }

  region_ids.clear();

  for (const auto& region : rpc.Response()->regions()) {
    region_ids.push_back(region.region_id());
  }

  return status;
}

Status Client::GetRegionMap(int64_t tenant_id, std::vector<RegionPB>& regions) {
  regions.clear();

  GetRegionMapRpc rpc;
  rpc.MutableRequest()->set_tenant_id(tenant_id);
  Status status = data_->stub->GetCoordinatorRpcController()->SyncCall(rpc);
  if (!status.IsOK()) {
    DINGO_LOG(ERROR) << fmt::format("scan regions fail, error: {} {}", status.Errno(), status.ToString());
    return status;
  }
  for (const auto& region : rpc.Response()->regionmap().regions()) {
    RegionPB region_pb;
    region_pb.id = region.id();
    region_pb.epoch = region.epoch();
    region_pb.region_type = PBRegionTypeToRegionType(region.region_type());
    region_pb.leader_store_id = region.leader_store_id();
    regions.push_back(region_pb);
  }

  return status;
}

Status Client::GetStoreMap(const std::vector<StoreType>& store_types, std::vector<StorePB>& stores) {
  stores.clear();

  GetStoreMapRpc rpc;
  for (const auto& store_type : store_types) {
    rpc.MutableRequest()->add_filter_store_types(StoreTypeToPBStoreType(store_type));
  }

  Status status = data_->stub->GetCoordinatorRpcController()->SyncCall(rpc);
  if (!status.IsOK()) {
    DINGO_LOG(ERROR) << fmt::format("scan regions fail, error: {} {}", status.Errno(), status.ToString());
    return status;
  }
  for (const auto& store : rpc.Response()->storemap().stores()) {
    StorePB store_pb;
    store_pb.id = store.id();
    store_pb.store_type = PBStoreTypeToStoreType(store.store_type());
    store_pb.epoch = store.epoch();
    store_pb.leader_num_weight = store.leader_num_weight();
    store_pb.state = PBStoreStateToStoreState(store.state());
    store_pb.in_state = PBStoreInStateToStoreInState(store.in_state());
    stores.push_back(store_pb);
  }

  return status;
}

Status Client::TransferLeaderRegion(int64_t region_id, int64_t leader_store_id, bool is_force) {
  TransferLeaderRegionRpc rpc;
  rpc.MutableRequest()->set_region_id(region_id);
  rpc.MutableRequest()->set_leader_store_id(leader_store_id);
  rpc.MutableRequest()->set_is_force(is_force);

  Status status = data_->stub->GetCoordinatorRpcController()->SyncCall(rpc);
  if (!status.IsOK()) {
    DINGO_LOG(ERROR) << fmt::format("transfer leader region fail, error: {} {}", status.Errno(), status.ToString());
    return status;
  }

  return status;
}

RawKV::RawKV(Data* data) : data_(data) {}

RawKV::~RawKV() { delete data_; }

Status RawKV::Get(const std::string& key, std::string& out_value) {
  RawKvGetTask task(data_->stub, key, out_value);
  return task.Run();
}

Status RawKV::BatchGet(const std::vector<std::string>& keys, std::vector<KVPair>& out_kvs) {
  RawKvBatchGetTask task(data_->stub, keys, out_kvs);
  return task.Run();
}

Status RawKV::Put(const std::string& key, const std::string& value) {
  RawKvPutTask task(data_->stub, key, value);
  return task.Run();
}

Status RawKV::BatchPut(const std::vector<KVPair>& kvs) {
  RawKvBatchPutTask task(data_->stub, kvs);
  return task.Run();
}

Status RawKV::PutIfAbsent(const std::string& key, const std::string& value, bool& out_state) {
  RawKvPutIfAbsentTask task(data_->stub, key, value, out_state);
  return task.Run();
}

Status RawKV::BatchPutIfAbsent(const std::vector<KVPair>& kvs, std::vector<KeyOpState>& out_states) {
  RawKvBatchPutIfAbsentTask task(data_->stub, kvs, out_states);
  return task.Run();
}

Status RawKV::Delete(const std::string& key) {
  RawKvDeleteTask task(data_->stub, key);
  return task.Run();
}

Status RawKV::BatchDelete(const std::vector<std::string>& keys) {
  RawKvBatchDeleteTask task(data_->stub, keys);
  return task.Run();
}

Status RawKV::DeleteRangeNonContinuous(const std::string& start_key, const std::string& end_key,
                                       int64_t& out_delete_count) {
  if (start_key.empty() || end_key.empty()) {
    return Status::InvalidArgument("start_key and end_key must not empty, check params");
  }

  if (start_key >= end_key) {
    return Status::InvalidArgument("end_key must greater than start_key, check params");
  }

  RawKvDeleteRangeTask task(data_->stub, start_key, end_key, false, out_delete_count);
  return task.Run();
}

Status RawKV::DeleteRange(const std::string& start_key, const std::string& end_key, int64_t& out_delete_count) {
  if (start_key.empty() || end_key.empty()) {
    return Status::InvalidArgument("start_key and end_key must not empty, check params");
  }

  if (start_key >= end_key) {
    return Status::InvalidArgument("end_key must greater than start_key, check params");
  }

  RawKvDeleteRangeTask task(data_->stub, start_key, end_key, true, out_delete_count);
  return task.Run();
}

Status RawKV::CompareAndSet(const std::string& key, const std::string& value, const std::string& expected_value,
                            bool& out_state) {
  RawKvCompareAndSetTask task(data_->stub, key, value, expected_value, out_state);
  return task.Run();
}

Status RawKV::BatchCompareAndSet(const std::vector<KVPair>& kvs, const std::vector<std::string>& expected_values,
                                 std::vector<KeyOpState>& out_states) {
  if (kvs.size() != expected_values.size()) {
    return Status::InvalidArgument(
        fmt::format("kvs size:{} must equal expected_values size:{}", kvs.size(), expected_values.size()));
  }

  RawKvBatchCompareAndSetTask task(data_->stub, kvs, expected_values, out_states);
  return task.Run();
}

Status RawKV::Scan(const std::string& start_key, const std::string& end_key, uint64_t limit, std::vector<KVPair>& kvs) {
  if (start_key.empty() || end_key.empty()) {
    return Status::InvalidArgument("start_key and end_key must not empty, check params");
  }

  if (start_key >= end_key) {
    return Status::InvalidArgument("end_key must greater than start_key, check params");
  }

  RawKvScanTask task(data_->stub, start_key, end_key, limit, kvs);
  return task.Run();
}

Transaction::Transaction(Data* data) : data_(data) {}

Transaction::~Transaction() { delete data_; }

Status Transaction::Begin() { return data_->impl->Begin(); }

int64_t Transaction::ID() const { return data_->impl->ID(); }

Status Transaction::Get(const std::string& key, std::string& value) { return data_->impl->Get(key, value); }

Status Transaction::BatchGet(const std::vector<std::string>& keys, std::vector<KVPair>& kvs) {
  return data_->impl->BatchGet(keys, kvs);
}

Status Transaction::Put(const std::string& key, const std::string& value) { return data_->impl->Put(key, value); }

Status Transaction::BatchPut(const std::vector<KVPair>& kvs) { return data_->impl->BatchPut(kvs); }

Status Transaction::PutIfAbsent(const std::string& key, const std::string& value) {
  return data_->impl->PutIfAbsent(key, value);
}

Status Transaction::BatchPutIfAbsent(const std::vector<KVPair>& kvs) { return data_->impl->BatchPutIfAbsent(kvs); }

Status Transaction::Delete(const std::string& key) { return data_->impl->Delete(key); }

Status Transaction::BatchDelete(const std::vector<std::string>& keys) { return data_->impl->BatchDelete(keys); }

Status Transaction::Scan(const std::string& start_key, const std::string& end_key, uint64_t limit,
                         std::vector<KVPair>& kvs) {
  return data_->impl->Scan(start_key, end_key, limit, kvs);
}

Status Transaction::Commit() { return data_->impl->PreWriteAndCommit(); }

Status Transaction::Rollback() { return data_->impl->Rollback(); }

bool Transaction::IsOnePc() const { return data_->impl->IsOnePc(); }

bool Transaction::IsAsyncCommit() const { return data_->impl->IsAsyncCommit(); }

bool Transaction::IsConcurrentPreCommit() const { return data_->impl->IsConcurrentPreCommit(); }

void Transaction::GetTraceMetrics(TraceMetrics& metrics) { data_->impl->GetTraceMetrics(metrics); }

RegionCreator::RegionCreator(Data* data) : data_(data) {}

RegionCreator::~RegionCreator() { delete data_; }

RegionCreator& RegionCreator::SetRegionName(const std::string& name) {
  data_->region_name = name;
  return *this;
}

RegionCreator& RegionCreator::SetRange(const std::string& lower_bound, const std::string& upper_bound) {
  data_->lower_bound = lower_bound;
  data_->upper_bound = upper_bound;
  return *this;
}

RegionCreator& RegionCreator::SetEngineType(EngineType engine_type) {
  data_->engine_type = engine_type;
  return *this;
}

RegionCreator& RegionCreator::SetReplicaNum(int64_t num) {
  data_->replica_num = num;
  return *this;
}

RegionCreator& RegionCreator::Wait(bool wait) {
  data_->wait = wait;
  return *this;
}

static pb::common::RawEngine EngineType2RawEngine(EngineType engine_type) {
  switch (engine_type) {
    case kLSM:
      return pb::common::RawEngine::RAW_ENG_ROCKSDB;
    case kBTree:
      return pb::common::RawEngine::RAW_ENG_BDB;
    case kXDPROCKS:
      return pb::common::RawEngine::RAW_ENG_XDPROCKS;
    default:
      CHECK(false) << "unknow engine_type:" << engine_type;
  }
}

Status RegionCreator::Create(int64_t& out_region_id) {
  if (data_->region_name.empty()) {
    return Status::InvalidArgument("Missing region name");
  }
  if (data_->lower_bound.empty() || data_->upper_bound.empty()) {
    return Status::InvalidArgument("lower_bound or upper_bound must not empty");
  }
  if (data_->replica_num <= 0) {
    return Status::InvalidArgument("replica num must greater 0");
  }

  CreateRegionRpc rpc;
  rpc.MutableRequest()->set_region_name(data_->region_name);
  rpc.MutableRequest()->set_replica_num(data_->replica_num);
  rpc.MutableRequest()->mutable_range()->set_start_key(data_->lower_bound);
  rpc.MutableRequest()->mutable_range()->set_end_key(data_->upper_bound);

  // region_id <= 0 means auto create region id , region_id > 0 means create region with region_id
  rpc.MutableRequest()->set_region_id(out_region_id);

  rpc.MutableRequest()->set_raw_engine(EngineType2RawEngine(data_->engine_type));

  DINGO_RETURN_NOT_OK(data_->stub.GetCoordinatorRpcController()->SyncCall(rpc));

  CHECK(rpc.Response()->region_id() > 0) << "create region internal error, req:" << rpc.Request()->ShortDebugString()
                                         << ", resp:" << rpc.Response()->ShortDebugString();

  if (out_region_id > 0) {
    CHECK(out_region_id == rpc.Response()->region_id())
        << "create region internal error, req:" << rpc.Request()->ShortDebugString()
        << ", resp:" << rpc.Response()->ShortDebugString();
  }

  out_region_id = rpc.Response()->region_id();

  if (data_->wait) {
    int retry = 0;
    while (retry < FLAGS_coordinator_interaction_max_retry) {
      bool creating = false;
      DINGO_RETURN_NOT_OK(data_->stub.GetAdminTool()->IsCreateRegionInProgress(out_region_id, creating));

      if (creating) {
        retry++;
        SleepUs(FLAGS_coordinator_interaction_delay_ms * 1000);
      } else {
        return Status::OK();
      }
    }

    std::string msg =
        fmt::format("Fail query region:{} state retry:{} exceed limit:{}, delay ms:{}", out_region_id, retry,
                    FLAGS_coordinator_interaction_max_retry, FLAGS_coordinator_interaction_delay_ms);
    DINGO_LOG(INFO) << msg;
    return Status::Incomplete(msg);
  }

  return Status::OK();
}

Status RegionCreator::CreateRegionId(int64_t count, std::vector<int64_t>& out_region_ids) {
  CreateRegionIdRpc rpc;
  rpc.MutableRequest()->set_count(count);
  DINGO_RETURN_NOT_OK(data_->stub.GetCoordinatorRpcController()->SyncCall(rpc));
  CHECK(rpc.Response()->region_ids_size() == count)
      << "create region id internal error, req:" << rpc.Request()->ShortDebugString()
      << ", resp:" << rpc.Response()->ShortDebugString();
  out_region_ids.resize(rpc.Response()->region_ids_size());
  for (int i = 0; i < rpc.Response()->region_ids_size(); i++) {
    out_region_ids[i] = rpc.Response()->region_ids(i);
  }
  return Status::OK();
}

}  // namespace sdk
}  // namespace dingodb