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

#include "sdk/client.h"

void DefineClientBindings(pybind11::module& m) {
  using namespace dingodb;
  using namespace dingodb::sdk;
  namespace py = pybind11;

  py::class_<Client>(m, "Client")
      .def_static("BuildAndInitLog",
                  [](std::string addrs) {
                    Client* pptr;
                    Status status = Client::BuildAndInitLog(addrs, &pptr);
                    return std::make_tuple(status, pptr);
                  })
      .def_static("BuildFromAddrs",
                  [](std::string addrs) {
                    Client* pptr;
                    Status status = Client::BuildFromAddrs(addrs, &pptr);
                    return std::make_tuple(status, pptr);
                  })
      .def_static("Build",
                  [](std::string naming_service_url) {
                    Client* pptr;
                    Status status = Client::Build(naming_service_url, &pptr);
                    return std::make_tuple(status, pptr);
                  })
      .def("NewRawKV",
           [](Client& client) {
             RawKV* pptr;
             Status status = client.NewRawKV(&pptr);
             return std::make_tuple(status, pptr);
           })
      .def("NewTransaction",
           [](Client& client, const TransactionOptions& options) {
             Transaction* pptr;
             Status status = client.NewTransaction(options, &pptr);
             return std::make_tuple(status, pptr);
           })
      .def("NewRegionCreator",
           [](Client& client) {
             RegionCreator* pptr;
             Status status = client.NewRegionCreator(&pptr);
             return std::make_tuple(status, pptr);
           })
      .def("IsCreateRegionInProgress",
           [](Client& client, int64_t region_id) {
             bool out_create_in_progress;
             Status status = client.IsCreateRegionInProgress(region_id, out_create_in_progress);
             return std::make_tuple(status, out_create_in_progress);
           })
      .def("DropRegion", &Client::DropRegion)
      .def("NewVectorClient",
           [](Client& client) {
             VectorClient* pptr;
             Status status = client.NewVectorClient(&pptr);
             return std::make_tuple(status, pptr);
           })
      .def("NewVectorIndexCreator",
           [](Client& client) {
             VectorIndexCreator* pptr;
             Status status = client.NewVectorIndexCreator(&pptr);
             return std::make_tuple(status, pptr);
           })
      .def("GetIndexId",
           [](Client& client, int64_t schema_id, const std::string& index_name) {
             int64_t out_index_id;
             Status status = client.GetIndexId(schema_id, index_name, out_index_id);
             return std::make_tuple(status, out_index_id);
           })
      .def("DropIndex", &Client::DropIndex)
      .def("DropIndexByName", &Client::DropIndexByName);

  py::class_<KVPair>(m, "KVPair")
      .def(py::init<>())
      .def_readwrite("key", &KVPair::key)
      .def_readwrite("value", &KVPair::value);

  py::class_<KeyOpState>(m, "KeyOpState")
      .def(py::init<>())
      .def_readwrite("key", &KeyOpState::key)
      .def_readwrite("state", &KeyOpState::state);

  py::class_<RawKV>(m, "RawKV")
      // .def(py::init<>())
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
             return std::make_tuple(status, out_delete_count);
           })
      .def("DeleteRange",
           [](RawKV& rawkv, const std::string& start_key, const std::string& end_key) {
             int64_t out_delete_count;
             Status status = rawkv.DeleteRange(start_key, end_key, out_delete_count);
             return std::make_tuple(status, out_delete_count);
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
      // .def(py::init<>())
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
      // .def(py::init<>())
      .def("SetRegionName", &RegionCreator::SetRegionName)  // 需不需要添加py::return_value_policy::reference
      .def("SetRange", &RegionCreator::SetRange)
      .def("SetEngineType", &RegionCreator::SetEngineType)
      .def("SetReplicaNum", &RegionCreator::SetReplicaNum)
      .def("Wait", &RegionCreator::Wait)
      .def("Create", [](RegionCreator& regioncreator) -> std::tuple<Status, int64_t> {
        int64_t out_region_id;
        Status status = regioncreator.Create(out_region_id);
        return std::make_tuple(status, out_region_id);
      });
}
