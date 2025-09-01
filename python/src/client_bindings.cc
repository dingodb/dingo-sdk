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

#include "client_bindings.h"

#include <pybind11/functional.h>
#include <pybind11/stl.h>

#include <cstdint>
#include <tuple>

#include "dingosdk/client.h"
#include "sdk/document/document_index.h"
#include "sdk/vector/vector_index.h"

void DefineClientBindings(pybind11::module& m) {
  using namespace dingodb;
  using namespace dingodb::sdk;
  namespace py = pybind11;

  py::class_<Client>(m, "Client")
      .def_static("BuildAndInitLog",
                  [](std::string addrs) {
                    Client* ptr;
                    Status status = Client::BuildAndInitLog(addrs, &ptr);
                    return std::make_tuple(status, ptr);
                  })
      .def_static("BuildFromAddrs",
                  [](std::string addrs) {
                    Client* ptr;
                    Status status = Client::BuildFromAddrs(addrs, &ptr);
                    return std::make_tuple(status, ptr);
                  })
      .def_static("Build",
                  [](std::string naming_service_url) {
                    Client* ptr;
                    Status status = Client::Build(naming_service_url, &ptr);
                    return std::make_tuple(status, ptr);
                  })
      .def("NewRawKV",
           [](Client& client) {
             RawKV* ptr;
             Status status = client.NewRawKV(&ptr);
             return std::make_tuple(status, ptr);
           })
      .def("NewTransaction",
           [](Client& client, const TransactionOptions& options) {
             Transaction* ptr;
             Status status = client.NewTransaction(options, &ptr);
             return std::make_tuple(status, ptr);
           })
      .def("NewRegionCreator",
           [](Client& client) {
             RegionCreator* ptr;
             Status status = client.NewRegionCreator(&ptr);
             return std::make_tuple(status, ptr);
           })
      .def("IsCreateRegionInProgress",
           [](Client& client, int64_t region_id) {
             bool out_create_in_progress;
             Status status = client.IsCreateRegionInProgress(region_id, out_create_in_progress);
             return std::make_tuple(status, out_create_in_progress);
           })
      .def("DropRegion", [](Client& client, int64_t region_id) { return client.DropRegion(region_id); })
      .def("NewVectorClient",
           [](Client& client) {
             VectorClient* ptr;
             Status status = client.NewVectorClient(&ptr);
             return std::make_tuple(status, ptr);
           })
      .def("NewVectorIndexCreator",
           [](Client& client) {
             VectorIndexCreator* ptr;
             Status status = client.NewVectorIndexCreator(&ptr);
             return std::make_tuple(status, ptr);
           })
      .def("GetVectorIndexId",
           [](Client& client, int64_t schema_id, const std::string& index_name) {
             int64_t out_index_id;
             Status status = client.GetVectorIndexId(schema_id, index_name, out_index_id);
             return std::make_tuple(status, out_index_id);
           })
      .def("GetVectorIndex",
           [](Client& client, int64_t schema_id, const std::string& index_name) {
             std::shared_ptr<VectorIndex> out_vector_index;
             Status status = client.GetVectorIndex(schema_id, index_name, out_vector_index);
             return std::make_tuple(status, out_vector_index);
           })
      .def("GetVectorIndexById",
           [](Client& client, int64_t index_id) {
             std::shared_ptr<VectorIndex> out_vector_index;
             Status status = client.GetVectorIndexById(index_id, out_vector_index);
             return std::make_tuple(status, out_vector_index);
           })
      .def("DropVectorIndexById", &Client::DropVectorIndexById)
      .def("DropVectorIndexByName", &Client::DropVectorIndexByName)
      .def("NewDocumentClient",
           [](Client& client) {
             DocumentClient* ptr;
             Status status = client.NewDocumentClient(&ptr);
             return std::make_tuple(status, ptr);
           })
      .def("NewDocumentIndexCreator",
           [](Client& client) {
             DocumentIndexCreator* ptr;
             Status status = client.NewDocumentIndexCreator(&ptr);
             return std::make_tuple(status, ptr);
           })
      .def("GetDocumentIndexId",
           [](Client& client, int64_t schema_id, const std::string& doc_name) {
             int64_t doc_index_id;
             Status status = client.GetDocumentIndexId(schema_id, doc_name, doc_index_id);
             return std::make_tuple(status, doc_index_id);
           })
      .def("DropDocumentIndexById", &Client::DropDocumentIndexById)
      .def("DropDocumentIndexByName", &Client::DropDocumentIndexByName)
      .def("GetDocumentIndex",
           [](Client& client, int64_t schema_id, const std::string& index_name) {
             std::shared_ptr<DocumentIndex> out_doc_index;
             Status status = client.GetDocumentIndex(schema_id, index_name, out_doc_index);
             return std::make_tuple(status, out_doc_index);
           })
      .def("GetDocumentIndexById", [](Client& client, int64_t index_id) {
        std::shared_ptr<DocumentIndex> out_doc_index;
        Status status = client.GetDocumentIndexById(index_id, out_doc_index);
        return std::make_tuple(status, out_doc_index);
      });

  py::class_<KVPair>(m, "KVPair")
      .def(py::init<>())
      .def(py::init<const std::string&, const std::string&>(), py::arg("key"), py::arg("value"))
      .def_readwrite("key", &KVPair::key)
      .def_readwrite("value", &KVPair::value);

  py::class_<KeyOpState>(m, "KeyOpState")
      .def(py::init<>())
      .def(py::init<const std::string&, bool>(), py::arg("key"), py::arg("state"))
      .def_readwrite("key", &KeyOpState::key)
      .def_readwrite("state", &KeyOpState::state);

  py::class_<RawKV>(m, "RawKV")
      .def("Get",
           [](RawKV& rawkv, const std::string& key) {
             std::string out_value;
             Status status = rawkv.Get(key, out_value);
             return std::make_tuple(status, out_value);
           })
      .def("BatchGet",
           [](RawKV& rawkv, const std::vector<std::string>& keys) {
             std::vector<KVPair> out_kvs;
             Status status = rawkv.BatchGet(keys, out_kvs);
             return std::make_tuple(status, out_kvs);
           })
      .def("Put", &RawKV::Put)
      .def("BatchPut", &RawKV::BatchPut)
      .def("PutIfAbsent",
           [](RawKV& rawkv, const std::string& key, const std::string& value) {
             bool out_state;
             Status status = rawkv.PutIfAbsent(key, value, out_state);
             return std::make_tuple(status, out_state);
           })
      .def("BatchPutIfAbsent",
           [](RawKV& rawkv, const std::vector<KVPair>& kvs) {
             std::vector<KeyOpState> out_states;
             Status status = rawkv.BatchPutIfAbsent(kvs, out_states);
             return std::make_tuple(status, out_states);
           })
      .def("Delete", &RawKV::Delete)
      .def("BatchDelete", &RawKV::BatchDelete)
      .def("DeleteRangeNonContinuous",
           [](RawKV& rawkv, const std::string& start_key, const std::string& end_key) {
             int64_t out_delete_count;
             Status status = rawkv.DeleteRangeNonContinuous(start_key, end_key, out_delete_count);
             return status;
           })
      .def("DeleteRange",
           [](RawKV& rawkv, const std::string& start_key, const std::string& end_key) {
             int64_t out_delete_count;
             Status status = rawkv.DeleteRange(start_key, end_key, out_delete_count);
             return status;
           })
      .def("CompareAndSet",
           [](RawKV& rawkv, const std::string& key, const std::string& value, const std::string& expected_value) {
             bool out_state;
             Status status = rawkv.CompareAndSet(key, value, expected_value, out_state);
             return std::make_tuple(status, out_state);
           })
      .def("BatchCompareAndSet",
           [](RawKV& rawkv, const std::vector<KVPair>& kvs, const std::vector<std::string>& expected_values) {
             std::vector<KeyOpState> out_states;
             Status status = rawkv.BatchCompareAndSet(kvs, expected_values, out_states);
             return std::make_tuple(status, out_states);
           })
      .def("Scan", [](RawKV& rawkv, const std::string& start_key, const std::string& end_key, uint64_t limit) {
        std::vector<KVPair> out_kvs;
        Status status = rawkv.Scan(start_key, end_key, limit, out_kvs);
        return std::make_tuple(status, out_kvs);
      });

  py::enum_<TransactionKind>(m, "TransactionKind")
      .value("kOptimistic", TransactionKind::kOptimistic)
      .value("kPessimistic", TransactionKind::kPessimistic);

  py::enum_<TransactionIsolation>(m, "TransactionIsolation")
      .value("kSnapshotIsolation", TransactionIsolation::kSnapshotIsolation)
      .value("kReadCommitted", TransactionIsolation::kReadCommitted);

  py::class_<TransactionOptions>(m, "TransactionOptions")
      .def(py::init<>())
      .def_readwrite("kind", &TransactionOptions::kind)
      .def_readwrite("isolation", &TransactionOptions::isolation)
      .def_readwrite("keep_alive_ms", &TransactionOptions::keep_alive_ms);

  py::class_<Transaction>(m, "Transaction")
      .def("Get",
           [](Transaction& transaction, const std::string& key) {
             std::string value;
             Status status = transaction.Get(key, value);
             return std::make_tuple(status, value);
           })
      .def("BatchGet",
           [](Transaction& transaction, const std::vector<std::string>& keys) {
             std::vector<KVPair> kvs;
             Status status = transaction.BatchGet(keys, kvs);
             return std::make_tuple(status, kvs);
           })
      .def("Put", &Transaction::Put)
      .def("BatchPut", &Transaction::BatchPut)
      .def("PutIfAbsent", &Transaction::PutIfAbsent)
      .def("BatchPutIfAbsent", &Transaction::BatchPutIfAbsent)
      .def("Delete", &Transaction::Delete)
      .def("BatchDelete", &Transaction::BatchDelete)
      .def("Scan",
           [](Transaction& transaction, const std::string& start_key, const std::string& end_key, uint64_t limit) {
             std::vector<KVPair> kvs;
             Status status = transaction.Scan(start_key, end_key, limit, kvs);
             return std::make_tuple(status, kvs);
           })
      .def("PreCommit", &Transaction::PreCommit)
      .def("Commit", &Transaction::Commit)
      .def("Rollback", &Transaction::Rollback);

  py::enum_<EngineType>(m, "EngineType")
      .value("kLSM", EngineType::kLSM)
      .value("kBTree", EngineType::kBTree)
      .value("kXDPROCKS", EngineType::kXDPROCKS);

  py::class_<RegionCreator>(m, "RegionCreator")
      .def("SetRegionName", &RegionCreator::SetRegionName)
      .def("SetRange", &RegionCreator::SetRange)
      .def("SetEngineType", &RegionCreator::SetEngineType)
      .def("SetReplicaNum", &RegionCreator::SetReplicaNum)
      .def("Wait", &RegionCreator::Wait)
      .def("Create",
           [](RegionCreator& regioncreator, int64_t region_id) -> std::tuple<Status, int64_t> {
             int64_t out_region_id = region_id;
             Status status = regioncreator.Create(out_region_id);
             return std::make_tuple(status, out_region_id);
           })
      .def("CreateRegionId",
           [](RegionCreator& regioncreator, int64_t count) -> std::tuple<Status, std::vector<int64_t>> {
             std::vector<int64_t> out_region_ids;
             Status status = regioncreator.CreateRegionId(count, out_region_ids);
             return std::make_tuple(status, out_region_ids);
           });
}
