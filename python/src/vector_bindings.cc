
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

#include "vector_bindings.h"

#include <nanobind/nanobind.h>
#include <nanobind/stl/function.h>
#include <nanobind/stl/map.h>
#include <nanobind/stl/string.h>
#include <nanobind/stl/tuple.h>
#include <nanobind/stl/vector.h>

#include <cstdint>
#include <tuple>

#include "dingosdk/vector.h"

void DefineVectorBindings(nanobind::module_& m) {
  using namespace dingodb;
  using namespace dingodb::sdk;
  namespace py = nanobind;

  py::class_<RegionStatus>(m, "RegionStatus")
      .def(py::init<>())
      .def_rw("region_id", &RegionStatus::region_id)
      .def_rw("status", &RegionStatus::status);

  py::class_<ErrStatusResult>(m, "ErrStatusResult")
      .def(py::init<>())
      .def("ToString", &ErrStatusResult::ToString)
      .def_rw("region_status", &ErrStatusResult::region_status);

  py::enum_<DiskANNRegionState>(m, "DiskANNRegionState")
      .value("kBuildFailed", DiskANNRegionState::kBuildFailed)
      .value("kLoadFailed", DiskANNRegionState::kLoadFailed)
      .value("kInittialized", DiskANNRegionState::kInittialized)
      .value("kBuilding", DiskANNRegionState::kBuilding)
      .value("kBuilded", DiskANNRegionState::kBuilded)
      .value("kLoading", DiskANNRegionState::kLoading)
      .value("kLoaded", DiskANNRegionState::kLoaded)
      .value("kNoData", DiskANNRegionState::kNoData);

  py::class_<RegionState>(m, "RegionState")
      .def(py::init<>())
      .def_rw("region_id", &RegionState::region_id)
      .def_rw("state", &RegionState::state);

  py::class_<StateResult>(m, "StateResult")
      .def(py::init<>())
      .def("ToString", &StateResult::ToString)
      .def_rw("region_states", &StateResult::region_states);

  m.def("RegionStateToString", &RegionStateToString, "description: RegionStateToString");

  py::enum_<VectorIndexType>(m, "VectorIndexType")
      .value("kNoneIndexType", VectorIndexType::kNoneIndexType)
      .value("kFlat", VectorIndexType::kFlat)
      .value("kIvfFlat", VectorIndexType::kIvfFlat)
      .value("kIvfPq", VectorIndexType::kIvfPq)
      .value("kHnsw", VectorIndexType::kHnsw)
      .value("kDiskAnn", VectorIndexType::kDiskAnn)
      .value("kBruteForce", VectorIndexType::kBruteForce)
      .value("kBinaryFlat", VectorIndexType::kBinaryFlat)
      .value("kBinaryIvfFlat", VectorIndexType::kBinaryIvfFlat);

  m.def("VectorIndexTypeToString", &VectorIndexTypeToString, "description: VectorIndexTypeToString");

  py::enum_<MetricType>(m, "MetricType")
      .value("kNoneMetricType", MetricType::kNoneMetricType)
      .value("kL2", MetricType::kL2)
      .value("kInnerProduct", MetricType::kInnerProduct)
      .value("kCosine", MetricType::kCosine)
      .value("kHamming", MetricType::kHamming);

  m.def("MetricTypeToString", &MetricTypeToString, "description: MetricTypeToString");

  py::enum_<ValueType>(m, "ValueType")
      .value("kNoneValueType", ValueType::kNoneValueType)
      .value("kFloat", ValueType::kFloat)
      .value("kUint8", ValueType::kUint8)
      .value("kInt8", ValueType::kInt8);

  m.def("ValueTypeToString", &ValueTypeToString, "description: ValueTypeToString");

  py::class_<FlatParam>(m, "FlatParam")
      .def(py::init<int32_t, MetricType>())
      .def_static("Type", &FlatParam::Type)
      .def_rw("dimension", &FlatParam::dimension)
      .def_rw("metric_type", &FlatParam::metric_type);

  py::class_<IvfFlatParam>(m, "IvfFlatParam")
      .def(py::init<int32_t, MetricType>())
      .def_static("Type", &IvfFlatParam::Type)
      .def_rw("dimension", &IvfFlatParam::dimension)
      .def_rw("metric_type", &IvfFlatParam::metric_type)
      .def_rw("ncentroids", &IvfFlatParam::ncentroids);

  py::class_<IvfPqParam>(m, "IvfPqParam")
      .def(py::init<int32_t, MetricType>())
      .def_static("Type", &IvfPqParam::Type)
      .def_rw("dimension", &IvfPqParam::dimension)
      .def_rw("metric_type", &IvfPqParam::metric_type)
      .def_rw("ncentroids", &IvfPqParam::ncentroids)
      .def_rw("nsubvector", &IvfPqParam::nsubvector)
      .def_rw("bucket_init_size", &IvfPqParam::bucket_init_size)
      .def_rw("bucket_max_size", &IvfPqParam::bucket_max_size)
      .def_rw("nbits_per_idx", &IvfPqParam::nbits_per_idx);

  py::class_<HnswParam>(m, "HnswParam")
      .def(py::init<int32_t, MetricType, int32_t>())
      .def_static("Type", &HnswParam::Type)
      .def_rw("dimension", &HnswParam::dimension)
      .def_rw("metric_type", &HnswParam::metric_type)
      .def_rw("ef_construction", &HnswParam::ef_construction)
      .def_rw("max_elements", &HnswParam::max_elements)
      .def_rw("nlinks", &HnswParam::nlinks);

  py::class_<DiskAnnParam>(m, "DiskAnnParam")
      .def(py::init<int32_t, MetricType, ValueType>())
      .def_static("Type", &DiskAnnParam::Type)
      .def_rw("dimension", &DiskAnnParam::dimension)
      .def_rw("metric_type", &DiskAnnParam::metric_type)
      .def_rw("value_type", &DiskAnnParam::value_type)
      .def_rw("max_degree", &DiskAnnParam::max_degree)
      .def_rw("search_list_size", &DiskAnnParam::search_list_size);

  py::class_<BruteForceParam>(m, "BruteForceParam")
      .def(py::init<int32_t, MetricType>())
      .def_static("Type", &BruteForceParam::Type)
      .def_rw("dimension", &BruteForceParam::dimension)
      .def_rw("metric_type", &BruteForceParam::metric_type);

  py::class_<BinaryFlatParam>(m, "BinaryFlatParam")
      .def(py::init<int32_t, MetricType>())
      .def_static("Type", &BinaryFlatParam::Type)
      .def_rw("dimension", &BinaryFlatParam::dimension)
      .def_rw("metric_type", &BinaryFlatParam::metric_type);

  py::class_<BinaryIvfFlatParam>(m, "BinaryIvfFlatParam")
      .def(py::init<int32_t, MetricType>())
      .def_static("Type", &BinaryIvfFlatParam::Type)
      .def_rw("dimension", &BinaryIvfFlatParam::dimension)
      .def_rw("metric_type", &BinaryIvfFlatParam::metric_type)
      .def_rw("ncentroids", &BinaryIvfFlatParam::ncentroids);

  py::class_<VectorScalarColumnSchema>(m, "VectorScalarColumnSchema")
      .def(py::init<const std::string&, Type, bool>(), py::arg("key"), py::arg("type"), py::arg("speed") = false)
      .def_rw("key", &VectorScalarColumnSchema::key)
      .def_rw("type", &VectorScalarColumnSchema::type)
      .def_rw("speed", &VectorScalarColumnSchema::speed);

  py::class_<VectorScalarSchema>(m, "VectorScalarSchema")
      .def(py::init<>())
      .def("AddScalarColumn", &VectorScalarSchema::AddScalarColumn)
      .def_rw("cols", &VectorScalarSchema::cols);

  py::class_<Vector>(m, "Vector")
      .def(py::init<>())
      .def(py::init<ValueType, int32_t>())
      .def("Size", &Vector::Size)
      .def("ToString", &Vector::ToString)
      .def_rw("dimension", &Vector::dimension)
      .def_rw("value_type", &Vector::value_type)
      .def_rw("float_values", &Vector::float_values)
      .def_rw("binary_values", &Vector::binary_values);

  py::class_<ScalarField>(m, "ScalarField")
      .def(py::init<>())
      .def_rw("bool_data", &ScalarField::bool_data)
      .def_rw("long_data", &ScalarField::long_data)
      .def_rw("double_data", &ScalarField::double_data)
      .def_rw("string_data", &ScalarField::string_data);

  py::class_<ScalarValue>(m, "ScalarValue")
      .def(py::init<>())
      .def("ToString", &ScalarValue::ToString)
      .def_rw("type", &ScalarValue::type)
      .def_rw("fields", &ScalarValue::fields);

  py::class_<VectorWithId>(m, "VectorWithId")
      .def(py::init<>())
      .def(py::init<int64_t, Vector>())
      .def(py::init<Vector>())
      .def("ToString", &VectorWithId::ToString)
      .def_rw("id", &VectorWithId::id)
      .def_rw("vector", &VectorWithId::vector)
      .def_rw("scalar_data", &VectorWithId::scalar_data);

  py::enum_<FilterSource>(m, "FilterSource")
      .value("kNoneFilterSource", FilterSource::kNoneFilterSource)
      .value("kScalarFilter", FilterSource::kScalarFilter)
      .value("kTableFilter", FilterSource::kTableFilter)
      .value("kVectorIdFilter", FilterSource::kVectorIdFilter);

  py::enum_<FilterType>(m, "FilterType")
      .value("kNoneFilterType", FilterType::kNoneFilterType)
      .value("kQueryPost", FilterType::kQueryPost)
      .value("kQueryPre", FilterType::kQueryPre);

  py::enum_<SearchExtraParamType>(m, "SearchExtraParamType")
      .value("kParallelOnQueries", SearchExtraParamType::kParallelOnQueries)
      .value("kNprobe", SearchExtraParamType::kNprobe)
      .value("kRecallNum", SearchExtraParamType::kRecallNum)
      .value("kEfSearch", SearchExtraParamType::kEfSearch);

  py::class_<SearchParam>(m, "SearchParam")
      .def(py::init<>())
      .def_rw("topk", &SearchParam::topk)
      .def_rw("with_vector_data", &SearchParam::with_vector_data)
      .def_rw("with_scalar_data", &SearchParam::with_scalar_data)
      .def_rw("selected_keys", &SearchParam::selected_keys)
      .def_rw("with_table_data", &SearchParam::with_table_data)
      .def_rw("enable_range_search", &SearchParam::enable_range_search)
      .def_rw("radius", &SearchParam::radius)
      .def_rw("filter_source", &SearchParam::filter_source)
      .def_rw("filter_type", &SearchParam::filter_type)
      .def_rw("is_negation", &SearchParam::is_negation)
      .def_rw("is_sorted", &SearchParam::is_sorted)
      .def_rw("vector_ids", &SearchParam::vector_ids)
      .def_rw("use_brute_force", &SearchParam::use_brute_force)
      .def_rw("extra_params", &SearchParam::extra_params)
      .def_rw("langchain_expr_json", &SearchParam::langchain_expr_json)
      .def_rw("beamwidth", &SearchParam::beamwidth)
      .def_rw("is_scalar_speed_up_with_document", &SearchParam::is_scalar_speed_up_with_document)
      .def_rw("query_string", &SearchParam::query_string);

  py::class_<VectorWithDistance>(m, "VectorWithDistance")
      .def(py::init<>())
      .def("ToString", &VectorWithDistance::ToString)
      .def_rw("vector_data", &VectorWithDistance::vector_data)
      .def_rw("distance", &VectorWithDistance::distance)
      .def_rw("metric_type", &VectorWithDistance::metric_type);

  py::class_<SearchResult>(m, "SearchResult")
      .def(py::init<>())
      .def(py::init<VectorWithId>())
      .def("ToString", &SearchResult::ToString)
      .def_rw("id", &SearchResult::id)
      .def_rw("vector_datas", &SearchResult::vector_datas);

  py::class_<DeleteResult>(m, "DeleteResult")
      .def(py::init<>())
      .def("ToString", &DeleteResult::ToString)
      .def_rw("vector_id", &DeleteResult::vector_id)
      .def_rw("deleted", &DeleteResult::deleted);

  py::class_<QueryParam>(m, "QueryParam")
      .def(py::init<>())
      .def_rw("vector_ids", &QueryParam::vector_ids)
      .def_rw("with_vector_data", &QueryParam::with_vector_data)
      .def_rw("with_scalar_data", &QueryParam::with_scalar_data)
      .def_rw("selected_keys", &QueryParam::selected_keys)
      .def_rw("with_table_data", &QueryParam::with_table_data);

  py::class_<QueryResult>(m, "QueryResult")
      .def(py::init<>())
      .def("ToString", &QueryResult::ToString)
      .def_rw("vectors", &QueryResult::vectors);

  py::class_<ScanQueryParam>(m, "ScanQueryParam")
      .def(py::init<>())
      .def_rw("vector_id_start", &ScanQueryParam::vector_id_start)
      .def_rw("vector_id_end", &ScanQueryParam::vector_id_end)
      .def_rw("max_scan_count", &ScanQueryParam::max_scan_count)
      .def_rw("is_reverse", &ScanQueryParam::is_reverse)
      .def_rw("with_vector_data", &ScanQueryParam::with_vector_data)
      .def_rw("with_scalar_data", &ScanQueryParam::with_scalar_data)
      .def_rw("selected_keys", &ScanQueryParam::selected_keys)
      .def_rw("with_table_data", &ScanQueryParam::with_table_data)
      .def_rw("use_scalar_filter", &ScanQueryParam::use_scalar_filter)
      .def_rw("scalar_data", &ScanQueryParam::scalar_data);

  py::class_<ScanQueryResult>(m, "ScanQueryResult")
      .def(py::init<>())
      .def("ToString", &ScanQueryResult::ToString)
      .def_rw("vectors", &ScanQueryResult::vectors);

  py::class_<IndexMetricsResult>(m, "IndexMetricsResult")
      .def(py::init<>())
      .def("ToString", &IndexMetricsResult::ToString)
      .def_rw("index_type", &IndexMetricsResult::index_type)
      .def_rw("count", &IndexMetricsResult::count)
      .def_rw("deleted_count", &IndexMetricsResult::deleted_count)
      .def_rw("max_vector_id", &IndexMetricsResult::max_vector_id)
      .def_rw("min_vector_id", &IndexMetricsResult::min_vector_id)
      .def_rw("memory_bytes", &IndexMetricsResult::memory_bytes);

  py::class_<VectorIndexCreator>(m, "VectorIndexCreator")
      .def("SetSchemaId", [](VectorIndexCreator& c, int64_t id) { c.SetSchemaId(id); })
      .def("SetName", [](VectorIndexCreator& c, const std::string& n) { c.SetName(n); })
      .def("SetRangePartitions", [](VectorIndexCreator& c, std::vector<int64_t> ids) { c.SetRangePartitions(ids); })
      .def("SetReplicaNum", [](VectorIndexCreator& c, int64_t n) { c.SetReplicaNum(n); })
      .def("SetFlatParam", [](VectorIndexCreator& c, const FlatParam& p) { c.SetFlatParam(p); })
      .def("SetIvfFlatParam", [](VectorIndexCreator& c, const IvfFlatParam& p) { c.SetIvfFlatParam(p); })
      .def("SetIvfPqParam", [](VectorIndexCreator& c, const IvfPqParam& p) { c.SetIvfPqParam(p); })
      .def("SetHnswParam", [](VectorIndexCreator& c, const HnswParam& p) { c.SetHnswParam(p); })
      .def("SetDiskAnnParam", [](VectorIndexCreator& c, const DiskAnnParam& p) { c.SetDiskAnnParam(p); })
      .def("SetBruteForceParam", [](VectorIndexCreator& c, const BruteForceParam& p) { c.SetBruteForceParam(p); })
      .def("SetBinaryFlatParam", [](VectorIndexCreator& c, const BinaryFlatParam& p) { c.SetBinaryFlatParam(p); })
      .def("SetBinaryIvfFlatParam", [](VectorIndexCreator& c, const BinaryIvfFlatParam& p) { c.SetBinaryIvfFlatParam(p); })
      .def("SetAutoIncrementStart", [](VectorIndexCreator& c, int64_t id) { c.SetAutoIncrementStart(id); })
      .def("SetScalarSchema", [](VectorIndexCreator& c, const VectorScalarSchema& s) { c.SetScalarSchema(s); })
      .def("SetEnableScalarSpeedUpWithDocument", [](VectorIndexCreator& c, bool e) { c.SetEnableScalarSpeedUpWithDocument(e); })
      .def("SetDocumentSchema", [](VectorIndexCreator& c, const DocumentSchema& s) { c.SetDocumentSchema(s); })
      .def("SetJsonParams", [](VectorIndexCreator& c, std::string& p) { c.SetJsonParams(p); })
      .def("Create", [](VectorIndexCreator& vectorindexcreator) -> std::tuple<Status, int64_t> {
        int64_t out_index_id;
        Status status = vectorindexcreator.Create(out_index_id);
        return std::make_tuple(status, out_index_id);
      });

  py::class_<VectorClient>(m, "VectorClient")
      .def("AddByIndexId",
           [](VectorClient& vectorclient, int64_t index_id, std::vector<VectorWithId>& vectors) {
             Status status = vectorclient.AddByIndexId(index_id, vectors);
             return std::make_tuple(status, vectors);
           })
      .def("AddByIndexName",
           [](VectorClient& vectorclient, int64_t schema_id, const std::string& index_name,
              std::vector<VectorWithId>& vectors) {
             Status status = vectorclient.AddByIndexName(schema_id, index_name, vectors);
             return std::make_tuple(status, vectors);
           })
      .def("UpsertByIndexId",
           [](VectorClient& vectorclient, int64_t index_id, std::vector<VectorWithId>& vectors) {
             Status status = vectorclient.UpsertByIndexId(index_id, vectors);
             return std::make_tuple(status, vectors);
           })
      .def("UpsertByIndexName",
           [](VectorClient& vectorclient, int64_t schema_id, const std::string& index_name,
              std::vector<VectorWithId>& vectors) {
             Status status = vectorclient.UpsertByIndexName(schema_id, index_name, vectors);
             return std::make_tuple(status, vectors);
           })
      .def("SearchByIndexId",
           [](VectorClient& vectorclient, int64_t index_id, const SearchParam& search_param,
              const std::vector<VectorWithId>& target_vectors) {
             std::vector<SearchResult> out_result;
             Status status = vectorclient.SearchByIndexId(index_id, search_param, target_vectors, out_result);
             return std::make_tuple(status, out_result);
           })
      .def("SearchByIndexName",
           [](VectorClient& vectorclient, int64_t schema_id, const std::string& index_name,
              const SearchParam& search_param, const std::vector<VectorWithId>& target_vectors) {
             std::vector<SearchResult> out_result;
             Status status =
                 vectorclient.SearchByIndexName(schema_id, index_name, search_param, target_vectors, out_result);
             return std::make_tuple(status, out_result);
           })
      .def("DeleteByIndexId",
           [](VectorClient& vectorclient, int64_t index_id, const std::vector<int64_t>& vector_ids) {
             std::vector<DeleteResult> out_result;
             Status status = vectorclient.DeleteByIndexId(index_id, vector_ids, out_result);
             return std::make_tuple(status, out_result);
           })
      .def("DeleteByIndexName",
           [](VectorClient& vectorclient, int64_t schema_id, const std::string& index_name,
              const std::vector<int64_t>& vector_ids) {
             std::vector<DeleteResult> out_result;
             Status status = vectorclient.DeleteByIndexName(schema_id, index_name, vector_ids, out_result);
             return std::make_tuple(status, out_result);
           })
      .def("BatchQueryByIndexId",
           [](VectorClient& vectorclient, int64_t index_id, const QueryParam& query_param) {
             QueryResult out_result;
             Status status = vectorclient.BatchQueryByIndexId(index_id, query_param, out_result);
             return std::make_tuple(status, out_result);
           })
      .def("BatchQueryByIndexName",
           [](VectorClient& vectorclient, int64_t schema_id, const std::string& index_name,
              const QueryParam& query_param) {
             QueryResult out_result;
             Status status = vectorclient.BatchQueryByIndexName(schema_id, index_name, query_param, out_result);
             return std::make_tuple(status, out_result);
           })
      .def("GetBorderByIndexId",
           [](VectorClient& vectorclient, int64_t index_id, bool is_max) {
             int64_t out_vector_id;
             Status status = vectorclient.GetBorderByIndexId(index_id, is_max, out_vector_id);
             return std::make_tuple(status, out_vector_id);
           })
      .def("GetBorderByIndexName",
           [](VectorClient& vectorclient, int64_t schema_id, const std::string& index_name, bool is_max) {
             int64_t out_vector_id;
             Status status = vectorclient.GetBorderByIndexName(schema_id, index_name, is_max, out_vector_id);
             return std::make_tuple(status, out_vector_id);
           })
      .def("ScanQueryByIndexId",
           [](VectorClient& vectorclient, int64_t index_id, const ScanQueryParam& query_param) {
             ScanQueryResult out_result;
             Status status = vectorclient.ScanQueryByIndexId(index_id, query_param, out_result);
             return std::make_tuple(status, out_result);
           })
      .def("ScanQueryByIndexName",
           [](VectorClient& vectorclient, int64_t schema_id, const std::string& index_name,
              const ScanQueryParam& query_param) {
             ScanQueryResult out_result;
             Status status = vectorclient.ScanQueryByIndexName(schema_id, index_name, query_param, out_result);
             return std::make_tuple(status, out_result);
           })
      .def("GetIndexMetricsByIndexId",
           [](VectorClient& vectorclient, int64_t index_id) {
             IndexMetricsResult out_result;
             Status status = vectorclient.GetIndexMetricsByIndexId(index_id, out_result);
             return std::make_tuple(status, out_result);
           })
      .def("GetIndexMetricsByIndexName",
           [](VectorClient& vectorclient, int64_t schema_id, const std::string& index_name) {
             IndexMetricsResult out_result;
             Status status = vectorclient.GetIndexMetricsByIndexName(schema_id, index_name, out_result);
             return std::make_tuple(status, out_result);
           })
      .def("CountAllByIndexId",
           [](VectorClient& vectorclient, int64_t index_id) {
             int64_t out_count;
             Status status = vectorclient.CountAllByIndexId(index_id, out_count);
             return std::make_tuple(status, out_count);
           })
      .def("CountallByIndexName",
           [](VectorClient& vectorclient, int64_t schema_id, const std::string& index_name) {
             int64_t out_count;
             Status status = vectorclient.CountallByIndexName(schema_id, index_name, out_count);
             return std::make_tuple(status, out_count);
           })
      .def("CountByIndexId",
           [](VectorClient& vectorclient, int64_t index_id, int64_t start_vector_id, int64_t end_vector_id) {
             int64_t out_count;
             Status status = vectorclient.CountByIndexId(index_id, start_vector_id, end_vector_id, out_count);
             return std::make_tuple(status, out_count);
           })
      .def("CountByIndexName",
           [](VectorClient& vectorclient, int64_t schema_id, const std::string& index_name, int64_t start_vector_id,
              int64_t end_vector_id) {
             int64_t out_count;
             Status status =
                 vectorclient.CountByIndexName(schema_id, index_name, start_vector_id, end_vector_id, out_count);
             return std::make_tuple(status, out_count);
           })
      .def("StatusByIndexId",
           [](VectorClient& vectorclient, int64_t index_id) {
             StateResult result;
             Status status = vectorclient.StatusByIndexId(index_id, result);
             return std::make_tuple(status, result);
           })
      .def("StatusByIndexName",
           [](VectorClient& vectorclient, int64_t schema_id, const std::string& index_name) {
             StateResult result;
             Status status = vectorclient.StatusByIndexName(schema_id, index_name, result);
             return std::make_tuple(status, result);
           })
      .def("StatusByRegionId",
           [](VectorClient& vectorclient, int64_t index_id, const std::vector<int64_t>& region_ids) {
             StateResult result;
             Status status = vectorclient.StatusByRegionId(index_id, region_ids, result);
             return std::make_tuple(status, result);
           })
      .def("StatusByRegionIdIndexName",
           [](VectorClient& vectorclient, int64_t schema_id, const std::string& index_name,
              const std::vector<int64_t>& region_ids) {
             StateResult result;
             Status status = vectorclient.StatusByRegionIdIndexName(schema_id, index_name, region_ids, result);
             return std::make_tuple(status, result);
           })
      .def("BuildByIndexId",
           [](VectorClient& vectorclient, int64_t index_id) {
             ErrStatusResult result;
             Status status = vectorclient.BuildByIndexId(index_id, result);
             return std::make_tuple(status, result);
           })
      .def("BuildByIndexName",
           [](VectorClient& vectorclient, int64_t schema_id, const std::string& index_name) {
             ErrStatusResult result;
             Status status = vectorclient.BuildByIndexName(schema_id, index_name, result);
             return std::make_tuple(status, result);
           })
      .def("BuildByRegionId",
           [](VectorClient& vectorclient, int64_t index_id, const std::vector<int64_t>& region_ids) {
             ErrStatusResult result;
             Status status = vectorclient.BuildByRegionId(index_id, region_ids, result);
             return std::make_tuple(status, result);
           })
      .def("BuildByRegionIdIndexName",
           [](VectorClient& vectorclient, int64_t schema_id, const std::string& index_name,
              const std::vector<int64_t>& region_ids) {
             ErrStatusResult result;
             Status status = vectorclient.BuildByRegionIdIndexName(schema_id, index_name, region_ids, result);
             return std::make_tuple(status, result);
           })
      .def("LoadByIndexId",
           [](VectorClient& vectorclient, int64_t index_id) {
             ErrStatusResult result;
             Status status = vectorclient.LoadByIndexId(index_id, result);
             return std::make_tuple(status, result);
           })
      .def("LoadByIndexName",
           [](VectorClient& vectorclient, int64_t schema_id, const std::string& index_name) {
             ErrStatusResult result;
             Status status = vectorclient.LoadByIndexName(schema_id, index_name, result);
             return std::make_tuple(status, result);
           })
      .def("LoadByRegionId",
           [](VectorClient& vectorclient, int64_t index_id, const std::vector<int64_t>& region_ids) {
             ErrStatusResult result;
             Status status = vectorclient.LoadByRegionId(index_id, region_ids, result);
             return std::make_tuple(status, result);
           })
      .def("LoadByRegionIdIndexName",
           [](VectorClient& vectorclient, int64_t schema_id, const std::string& index_name,
              const std::vector<int64_t>& region_ids) {
             ErrStatusResult result;
             Status status = vectorclient.LoadByRegionIdIndexName(schema_id, index_name, region_ids, result);
             return std::make_tuple(status, result);
           })
      .def("ResetByIndexId",
           [](VectorClient& vectorclient, int64_t index_id) {
             ErrStatusResult result;
             Status status = vectorclient.ResetByIndexId(index_id, result);
             return std::make_tuple(status, result);
           })
      .def("ResetByIndexName",
           [](VectorClient& vectorclient, int64_t schema_id, const std::string& index_name) {
             ErrStatusResult result;
             Status status = vectorclient.ResetByIndexName(schema_id, index_name, result);
             return std::make_tuple(status, result);
           })
      .def("ResetByRegionId",
           [](VectorClient& vectorclient, int64_t index_id, const std::vector<int64_t>& region_ids) {
             ErrStatusResult result;
             Status status = vectorclient.ResetByRegionId(index_id, region_ids, result);
             return std::make_tuple(status, result);
           })
      .def("ResetByRegionIdIndexName",
           [](VectorClient& vectorclient, int64_t schema_id, const std::string& index_name,
              const std::vector<int64_t>& region_ids) {
             ErrStatusResult result;
             Status status = vectorclient.ResetByRegionIdIndexName(schema_id, index_name, region_ids, result);
             return std::make_tuple(status, result);
           })
      .def("ImportAddByIndexId",
           [](VectorClient& vectorclient, int64_t index_id, std::vector<VectorWithId>& vectors) {
             Status status = vectorclient.ImportAddByIndexId(index_id, vectors);
             return std::make_tuple(status, vectors);
           })
      .def("ImportAddByIndexName",
           [](VectorClient& vectorclient, int64_t schema_id, const std::string& index_name,
              std::vector<VectorWithId>& vectors) {
             Status status = vectorclient.ImportAddByIndexName(schema_id, index_name, vectors);
             return std::make_tuple(status, vectors);
           })
      .def("ImportDeleteByIndexId",
           [](VectorClient& vectorclient, int64_t index_id, const std::vector<int64_t>& vector_ids) {
             Status status = vectorclient.ImportDeleteByIndexId(index_id, vector_ids);
             return status;
           })
      .def("ImportDeleteByIndexName",
           [](VectorClient& vectorclient, int64_t schema_id, const std::string& index_name,
              const std::vector<int64_t>& vector_ids) {
             Status status = vectorclient.ImportDeleteByIndexName(schema_id, index_name, vector_ids);
             return status;
           })
      .def("CountMemoryByIndexId",
           [](VectorClient& vectorclient, int64_t index_id) {
             int64_t count{0};
             Status status = vectorclient.CountMemoryByIndexId(index_id, count);
             return std::make_tuple(status, count);
           })
      .def("CountMemoryByIndexName",
           [](VectorClient& vectorclient, int64_t schema_id, const std::string& index_name) {
             int64_t count{0};
             Status status = vectorclient.CountMemoryByIndexName(schema_id, index_name, count);
             return std::make_tuple(status, count);
           })
      .def("GetAutoIncrementIdByIndexId",
           [](VectorClient& vectorclient, int64_t index_id) {
             int64_t start_id{0};
             Status status = vectorclient.GetAutoIncrementIdByIndexId(index_id, start_id);
             return std::make_tuple(status, start_id);
           })
      .def("GetAutoIncrementIdByIndexName",
           [](VectorClient& vectorclient, int64_t schema_id, const std::string& index_name) {
             int64_t start_id{0};
             Status status = vectorclient.GetAutoIncrementIdByIndexName(schema_id, index_name, start_id);
             return std::make_tuple(status, start_id);
           })
      .def("UpdateAutoIncrementIdByIndexId",
           [](VectorClient& vectorclient, int64_t index_id, int64_t start_id) {
             Status status = vectorclient.UpdateAutoIncrementIdByIndexId(index_id, start_id);
             return status;
           })
      .def("UpdateAutoIncrementIdByIndexName",
           [](VectorClient& vectorclient, int64_t schema_id, const std::string& index_name, int64_t start_id) {
             Status status = vectorclient.UpdateAutoIncrementIdByIndexName(schema_id, index_name, start_id);
             return status;
           })
      .def("DumpByIndexId",
           [](VectorClient& vectorclient, int64_t index_id) {
             std::vector<std::string> datas;
             Status status = vectorclient.DumpByIndexId(index_id, datas);
             return std::make_tuple(status, datas);
           })
      .def("DumpByIndexName", [](VectorClient& vectorclient, int64_t schema_id, const std::string& index_name) {
        std::vector<std::string> datas;
        Status status = vectorclient.DumpByIndexName(schema_id, index_name, datas);
        return std::make_tuple(status, datas);
      });
}
