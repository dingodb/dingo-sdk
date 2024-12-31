
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

#include "document_bindings.h"

#include <pybind11/functional.h>
#include <pybind11/stl.h>

#include <cstdint>
#include <tuple>

#include "dingosdk/document.h"

void DefineDocumentBindings(pybind11::module& m) {
  using namespace dingodb;
  using namespace dingodb::sdk;
  namespace py = pybind11;

  py::class_<DocumentColumn>(m, "DocumentColumn")
      .def(py::init<const std::string&, Type>())
      .def_readwrite("key", &DocumentColumn::key)
      .def_readwrite("type", &DocumentColumn::type);

  py::class_<DocumentSchema>(m, "DocumentSchema")
      .def(py::init<>())
      .def("AddColumn", &DocumentSchema::AddColumn)
      .def_readwrite("cols", &DocumentSchema::cols);

  py::class_<DocumentIndexCreator>(m, "DocumentIndexCreator")
      .def("SetSchemaId", &DocumentIndexCreator::SetSchemaId)
      .def("SetName", &DocumentIndexCreator::SetName)
      .def("SetRangePartitions", &DocumentIndexCreator::SetRangePartitions)
      .def("SetReplicaNum", &DocumentIndexCreator::SetReplicaNum)
      .def("SetAutoIncrementStart", &DocumentIndexCreator::SetAutoIncrementStart)
      .def("SetSchema", &DocumentIndexCreator::SetSchema)
      .def("SetJsonParams", &DocumentIndexCreator::SetJsonParams)
      .def("Create", [](DocumentIndexCreator& documentindexcreator) -> std::tuple<Status, int64_t> {
        int64_t out_index_id;
        Status status = documentindexcreator.Create(out_index_id);
        return std::make_tuple(status, out_index_id);
      });

  py::class_<DocValue>(m, "DocValue")
      .def_static("FromInt", &DocValue::FromInt)
      .def_static("FromDouble", &DocValue::FromDouble)
      .def_static("FromString", &DocValue::FromString)
      .def_static("FromBytes", &DocValue::FromBytes)
      .def_static("FromBool",&DocValue::FromBool)
      .def_static("FromDatetime", &DocValue::FromDatetime)
      .def("GetType", &DocValue::GetType)
      .def("IntValue", &DocValue::IntValue)
      .def("DoubleValue", &DocValue::DoubleValue)
      .def("StringValue", &DocValue::StringValue)
      .def("BytesValue", &DocValue::BytesValue)
      .def("BoolValue",&DocValue::BoolValue)
      .def("DatetimeValue", &DocValue::DatetimeValue)
      .def("ToString", &DocValue::ToString);

  py::class_<Document>(m, "Document")
      .def(py::init<>())
      .def("AddField", &Document::AddField)
      .def("GetFields", &Document::GetFields)
      .def("ToString", &Document::ToString);

  py::class_<DocWithId>(m, "DocWithId")
      .def(py::init<>())
      .def(py::init<int64_t, Document>())
      .def(py::init<Document>())
      .def_readwrite("id", &DocWithId::id)
      .def_readwrite("doc", &DocWithId::doc)
      .def("ToString", &DocWithId::ToString);

  py::class_<DocQueryParam>(m, "DocQueryParam")
      .def(py::init<>())
      .def_readwrite("doc_ids", &DocQueryParam::doc_ids)
      .def_readwrite("with_scalar_data", &DocQueryParam::with_scalar_data)
      .def_readwrite("selected_keys", &DocQueryParam::selected_keys);

  py::class_<DocQueryResult>(m, "DocQueryResult")
      .def(py::init<>())
      .def_readwrite("docs", &DocQueryResult::docs)
      .def("ToString", &DocQueryResult::ToString);

  py::class_<DocSearchParam>(m, "DocSearchParam")
      .def(py::init<>())
      .def_readwrite("top_n", &DocSearchParam::top_n)
      .def_readwrite("query_string", &DocSearchParam::query_string)
      .def_readwrite("use_id_filter", &DocSearchParam::use_id_filter)
      .def_readwrite("doc_ids", &DocSearchParam::doc_ids)
      .def_readwrite("column_names", &DocSearchParam::column_names)
      .def_readwrite("with_scalar_data", &DocSearchParam::with_scalar_data)
      .def_readwrite("selected_keys", &DocSearchParam::selected_keys);

  py::class_<DocWithStore>(m, "DocWithStore")
      .def(py::init<>())
      .def_readwrite("doc_with_id", &DocWithStore::doc_with_id)
      .def_readwrite("score", &DocWithStore::score)
      .def("ToString", &DocWithStore::ToString);

  py::class_<DocSearchResult>(m, "DocSearchResult")
      .def(py::init<>())
      .def_readwrite("doc_sores", &DocSearchResult::doc_sores)
      .def("ToString", &DocSearchResult::ToString);

  py::class_<DocDeleteResult>(m, "DocDeleteResult")
      .def(py::init<>())
      .def_readwrite("doc_id", &DocDeleteResult::doc_id)
      .def_readwrite("deleted", &DocDeleteResult::deleted)
      .def("ToString", &DocDeleteResult::ToString);

  py::class_<DocScanQueryParam>(m, "DocScanQueryParam")
      .def(py::init<>())
      .def_readwrite("doc_id_start", &DocScanQueryParam::doc_id_start)
      .def_readwrite("doc_id_end", &DocScanQueryParam::doc_id_end)
      .def_readwrite("is_reverse", &DocScanQueryParam::is_reverse)
      .def_readwrite("max_scan_count", &DocScanQueryParam::max_scan_count)
      .def_readwrite("with_scalar_data", &DocScanQueryParam::with_scalar_data)
      .def_readwrite("selected_keys", &DocScanQueryParam::selected_keys);

  py::class_<DocScanQueryResult>(m, "DocScanQueryResult")
      .def(py::init<>())
      .def_readwrite("docs", &DocScanQueryResult::docs)
      .def("ToString", &DocScanQueryResult::ToString);

  py::class_<DocIndexMetricsResult>(m, "DocIndexMetricsResult")
      .def(py::init<>())
      .def_readwrite("total_num_docs", &DocIndexMetricsResult::total_num_docs)
      .def_readwrite("total_num_tokens", &DocIndexMetricsResult::total_num_tokens)
      .def_readwrite("max_doc_id", &DocIndexMetricsResult::max_doc_id)
      .def_readwrite("min_doc_id", &DocIndexMetricsResult::min_doc_id)
      .def_readwrite("meta_json", &DocIndexMetricsResult::meta_json)
      .def_readwrite("json_parameter", &DocIndexMetricsResult::json_parameter)
      .def("ToString", &DocIndexMetricsResult::ToString);

  py::class_<DocumentClient>(m, "DocumentClient")
      .def("AddByIndexId",
           [](DocumentClient& documentclient, int64_t index_id, std::vector<DocWithId>& docs) {
             Status status = documentclient.AddByIndexId(index_id, docs);
             return std::make_tuple(status, docs);
           })
      .def("AddByIndexName",
           [](DocumentClient& documentclient, int64_t schema_id, const std::string& index_name,
              std::vector<DocWithId>& docs) {
             Status status = documentclient.AddByIndexName(schema_id, index_name, docs);
             return std::make_tuple(status, docs);
           })
      .def("UpdateByIndexId",
           [](DocumentClient& documentclient, int64_t index_id, std::vector<DocWithId>& docs) {
             Status status = documentclient.UpdateByIndexId(index_id, docs);
             return std::make_tuple(status, docs);
           })
      .def("UpdateByIndexName",
           [](DocumentClient& documentclient, int64_t schema_id, const std::string& index_name,
              std::vector<DocWithId>& docs) {
             Status status = documentclient.UpdateByIndexName(schema_id, index_name, docs);
             return std::make_tuple(status, docs);
           })
      .def("SearchByIndexId",
           [](DocumentClient& documentclient, int64_t index_id, const DocSearchParam& search_param) {
             DocSearchResult out_result;
             Status status = documentclient.SearchByIndexId(index_id, search_param, out_result);
             return std::make_tuple(status, out_result);
           })
      .def("SearchByIndexName",
           [](DocumentClient& documentclient, int64_t schema_id, const std::string& index_name,
              const DocSearchParam& search_param) {
             DocSearchResult out_result;
             Status status = documentclient.SearchByIndexName(schema_id, index_name, search_param, out_result);
             return std::make_tuple(status, out_result);
           })
      .def("DeleteByIndexId",
           [](DocumentClient& documentclient, int64_t index_id, const std::vector<int64_t>& doc_ids) {
             std::vector<DocDeleteResult> out_result;
             Status status = documentclient.DeleteByIndexId(index_id, doc_ids, out_result);
             return std::make_tuple(status, out_result);
           })
      .def("DeleteByIndexName",
           [](DocumentClient& documentclient, int64_t schema_id, const std::string& index_name,
              const std::vector<int64_t>& doc_ids) {
             std::vector<DocDeleteResult> out_result;
             Status status = documentclient.DeleteByIndexName(schema_id, index_name, doc_ids, out_result);
             return std::make_tuple(status, out_result);
           })
      .def("BatchQueryByIndexId",
           [](DocumentClient& documentclient, int64_t index_id, const DocQueryParam& query_param) {
             DocQueryResult out_result;
             Status status = documentclient.BatchQueryByIndexId(index_id, query_param, out_result);
             return std::make_tuple(status, out_result);
           })
      .def("BatchQueryByIndexName",
           [](DocumentClient& documentclient, int64_t schema_id, const std::string& index_name,
              const DocQueryParam& query_param) {
             DocQueryResult out_result;
             Status status = documentclient.BatchQueryByIndexName(schema_id, index_name, query_param, out_result);
             return std::make_tuple(status, out_result);
           })
      .def("GetBorderByIndexId",
           [](DocumentClient& documentclient, int64_t index_id, bool is_max) {
             int64_t out_doc_id;
             Status status = documentclient.GetBorderByIndexId(index_id, is_max, out_doc_id);
             return std::make_tuple(status, out_doc_id);
           })
      .def("GetBorderByIndexName",
           [](DocumentClient& documentclient, int64_t schema_id, const std::string& index_name, bool is_max) {
             int64_t out_doc_id;
             Status status = documentclient.GetBorderByIndexName(schema_id, index_name, is_max, out_doc_id);
             return std::make_tuple(status, out_doc_id);
           })
      .def("ScanQueryByIndexId",
           [](DocumentClient& documentclient, int64_t index_id, const DocScanQueryParam& query_param) {
             DocScanQueryResult out_result;
             Status status = documentclient.ScanQueryByIndexId(index_id, query_param, out_result);
             return std::make_tuple(status, out_result);
           })
      .def("ScanQueryByIndexName",
           [](DocumentClient& documentclient, int64_t schema_id, const std::string& index_name,
              const DocScanQueryParam& query_param) {
             DocScanQueryResult out_result;
             Status status = documentclient.ScanQueryByIndexName(schema_id, index_name, query_param, out_result);
             return std::make_tuple(status, out_result);
           })
      .def("GetIndexMetricsByIndexId",
           [](DocumentClient& documentclient, int64_t index_id) {
             DocIndexMetricsResult out_result;
             Status status = documentclient.GetIndexMetricsByIndexId(index_id, out_result);
             return std::make_tuple(status, out_result);
           })
      .def("GetIndexMetricsByIndexName",
           [](DocumentClient& documentclient, int64_t schema_id, const std::string& index_name) {
             DocIndexMetricsResult out_result;
             Status status = documentclient.GetIndexMetricsByIndexName(schema_id, index_name, out_result);
             return std::make_tuple(status, out_result);
           })
      .def("CountAllByIndexId",
           [](DocumentClient& documentclient, int64_t index_id) {
             int64_t out_count;
             Status status = documentclient.CountAllByIndexId(index_id, out_count);
             return std::make_tuple(status, out_count);
           })
      .def("CountallByIndexName",
           [](DocumentClient& documentclient, int64_t schema_id, const std::string& index_name) {
             int64_t out_count;
             Status status = documentclient.CountallByIndexName(schema_id, index_name, out_count);
             return std::make_tuple(status, out_count);
           })
      .def("CountByIndexId",
           [](DocumentClient& documentclient, int64_t index_id, int64_t start_doc_id, int64_t end_doc_id) {
             int64_t out_count{0};
             Status status = documentclient.CountByIndexId(index_id, start_doc_id, end_doc_id, out_count);
             return std::make_tuple(status, out_count);
           })
      .def("CountByIndexName",
           [](DocumentClient& documentclient, int64_t schema_id, const std::string& index_name, int64_t start_doc_id,
              int64_t end_doc_id) {
             int64_t out_count{0};
             Status status =
                 documentclient.CountByIndexName(schema_id, index_name, start_doc_id, end_doc_id, out_count);
             return std::make_tuple(status, out_count);
           })
      .def("GetAutoIncrementIdByIndexId",
           [](DocumentClient& documentclient, int64_t index_id) {
             int64_t start_id{0};
             Status status = documentclient.GetAutoIncrementIdByIndexId(index_id, start_id);
             return std::make_tuple(status, start_id);
           })
      .def("GetAutoIncrementIdByIndexName",
           [](DocumentClient& documentclient, int64_t schema_id, const std::string& index_name) {
             int64_t start_id{0};
             Status status = documentclient.GetAutoIncrementIdByIndexName(schema_id, index_name, start_id);
             return std::make_tuple(status, start_id);
           })
      .def("UpdateAutoIncrementIdByIndexId",
           [](DocumentClient& documentclient, int64_t index_id, int64_t start_id) {
             Status status = documentclient.UpdateAutoIncrementIdByIndexId(index_id, start_id);
             return status;
           })
      .def("UpdateAutoIncrementIdByIndexName",
           [](DocumentClient& documentclient, int64_t schema_id, const std::string& index_name, int64_t start_id) {
             Status status = documentclient.UpdateAutoIncrementIdByIndexName(schema_id, index_name, start_id);
             return status;
           });
}