
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

#include <pybind11/functional.h>
#include <pybind11/stl.h>

#include <cstdint>
#include <tuple>

#include "sdk/vector.h"

void DefineVectorBindings(pybind11::module& m) {
  using namespace dingodb;
  using namespace dingodb::sdk;
  namespace py = pybind11;

  py::enum_<VectorIndexType>(m, "VectorIndexType")
      .value("kNoneIndexType", VectorIndexType::kNoneIndexType)
      .value("kFlat", VectorIndexType::kFlat)
      .value("kIvfFlat", VectorIndexType::kIvfFlat)
      .value("kIvfPq", VectorIndexType::kIvfPq)
      .value("kHnsw", VectorIndexType::kHnsw)
      .value("kDiskAnn", VectorIndexType::kDiskAnn)
      .value("kBruteForce", VectorIndexType::kBruteForce);

  m.def("VectorIndexTypeToString", &VectorIndexTypeToString, "description: VectorIndexTypeToString");

  py::enum_<MetricType>(m, "MetricType")
      .value("kNoneMetricType", MetricType::kNoneMetricType)
      .value("kL2", MetricType::kL2)
      .value("kInnerProduct", MetricType::kInnerProduct)
      .value("kCosine", MetricType::kCosine);

  m.def("MetricTypeToString", &MetricTypeToString, "description: MetricTypeToString");

  py::class_<FlatParam>(m, "FlatParam")
      .def(py::init<int32_t, MetricType>())
      .def_static("Type", &FlatParam::Type)
      .def_readwrite("dimension", &FlatParam::dimension)
      .def_readwrite("metric_type", &FlatParam::metric_type);

  py::class_<IvfFlatParam>(m, "IvfFlatParam")
      .def(py::init<int32_t, MetricType>())
      .def_static("Type", &IvfFlatParam::Type)
      .def_readwrite("dimension", &IvfFlatParam::dimension)
      .def_readwrite("metric_type", &IvfFlatParam::metric_type)
      .def_readwrite("ncentroids", &IvfFlatParam::ncentroids);

  py::class_<IvfPqParam>(m, "IvfPqParam")
      .def(py::init<int32_t, MetricType>())
      .def_static("Type", &IvfPqParam::Type)
      .def_readwrite("dimension", &IvfPqParam::dimension)
      .def_readwrite("metric_type", &IvfPqParam::metric_type)
      .def_readwrite("ncentroids", &IvfPqParam::ncentroids)
      .def_readwrite("nsubvector", &IvfPqParam::nsubvector)
      .def_readwrite("bucket_init_size", &IvfPqParam::bucket_init_size)
      .def_readwrite("bucket_max_size", &IvfPqParam::bucket_max_size)
      .def_readwrite("nbits_per_idx", &IvfPqParam::nbits_per_idx);

  py::class_<HnswParam>(m, "HnswParam")
      .def(py::init<int32_t, MetricType, int32_t>())
      .def_static("Type", &HnswParam::Type)
      .def_readwrite("dimension", &HnswParam::dimension)
      .def_readwrite("metric_type", &HnswParam::metric_type)
      .def_readwrite("ef_construction", &HnswParam::ef_construction)
      .def_readwrite("max_elements", &HnswParam::max_elements)
      .def_readwrite("nlinks", &HnswParam::nlinks);

  py::class_<DiskAnnParam>(m, "DiskAnnParam").def(py::init<>());

  py::class_<BruteForceParam>(m, "BruteForceParam")
      .def(py::init<int32_t, MetricType>())
      .def_static("Type", &BruteForceParam::Type)
      .def_readwrite("dimension", &BruteForceParam::dimension)
      .def_readwrite("metric_type", &BruteForceParam::metric_type);

  py::class_<VectorScalarColumnSchema>(m, "VectorScalarColumnSchema")
      .def(py::init<const std::string&, Type, bool>(), py::arg(), py::arg(), py::arg()=false)
      .def_readwrite("key", &VectorScalarColumnSchema::key)
      .def_readwrite("type", &VectorScalarColumnSchema::type)
      .def_readwrite("speed", &VectorScalarColumnSchema::speed);

  py::class_<VectorScalarSchema>(m, "VectorScalarSchema")
      .def(py::init<>())
      .def("AddScalarColumn", &VectorScalarSchema::AddScalarColumn)
      .def_readwrite("cols", &VectorScalarSchema::cols);

  py::enum_<ValueType>(m, "ValueType")
      .value("kNoneValueType", ValueType::kNoneValueType)
      .value("kFloat", ValueType::kFloat)
      .value("kUint8", ValueType::kUint8);

  m.def("ValueTypeToString", &ValueTypeToString, "description: ValueTypeToString");

  py::class_<Vector>(m, "Vector")
      .def(py::init<>())
      .def(py::init<ValueType, int32_t>())
      .def("Size", &Vector::Size)
      .def("ToString", &Vector::ToString)
      .def_readwrite("dimension", &Vector::dimension)
      .def_readwrite("value_type", &Vector::value_type)
      .def_readwrite("float_values", &Vector::float_values)
      .def_readwrite("binary_values", &Vector::binary_values);

  py::class_<ScalarField>(m, "ScalarField")
      .def(py::init<>())
      .def_readwrite("bool_data", &ScalarField::bool_data)
      .def_readwrite("long_data", &ScalarField::long_data)
      .def_readwrite("double_data", &ScalarField::double_data)
      .def_readwrite("string_data", &ScalarField::string_data);

  py::class_<ScalarValue>(m, "ScalarValue")
      .def(py::init<>())
      .def("ToString", &ScalarValue::ToString)
      .def_readwrite("type", &ScalarValue::type)
      .def_readwrite("fields", &ScalarValue::fields);

  py::class_<VectorWithId>(m, "VectorWithId")
      .def(py::init<>())
      .def(py::init<int64_t, Vector>())
      .def(py::init<Vector>())
      .def("ToString", &VectorWithId::ToString)
      .def_readwrite("id", &VectorWithId::id)
      .def_readwrite("vector", &VectorWithId::vector)
      .def_readwrite("scalar_data", &VectorWithId::scalar_data);

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
      .def_readwrite("topk", &SearchParam::topk)
      .def_readwrite("with_vector_data", &SearchParam::with_vector_data)
      .def_readwrite("with_scalar_data", &SearchParam::with_scalar_data)
      .def_readwrite("selected_keys", &SearchParam::selected_keys)
      .def_readwrite("with_table_data", &SearchParam::with_table_data)
      .def_readwrite("enable_range_search", &SearchParam::enable_range_search)
      .def_readwrite("radius", &SearchParam::radius)
      .def_readwrite("filter_source", &SearchParam::filter_source)
      .def_readwrite("filter_type", &SearchParam::filter_type)
      .def_readwrite("is_negation", &SearchParam::is_negation)
      .def_readwrite("is_sorted", &SearchParam::is_sorted)
      .def_readwrite("vector_ids", &SearchParam::vector_ids)
      .def_readwrite("use_brute_force", &SearchParam::use_brute_force)
      .def_readwrite("extra_params", &SearchParam::extra_params)
      .def_readwrite("langchain_expr_json", &SearchParam::langchain_expr_json);

  py::class_<VectorWithDistance>(m, "VectorWithDistance")
      .def(py::init<>())
      .def("ToString", &VectorWithDistance::ToString)
      .def_readwrite("vector_data", &VectorWithDistance::vector_data)
      .def_readwrite("distance", &VectorWithDistance::distance)
      .def_readwrite("metric_type", &VectorWithDistance::metric_type);

  py::class_<SearchResult>(m, "SearchResult")
      .def(py::init<>())
      .def(py::init<VectorWithId>())
      .def("ToString", &SearchResult::ToString)
      .def_readwrite("id", &SearchResult::id)
      .def_readwrite("vector_datas", &SearchResult::vector_datas);

  py::class_<DeleteResult>(m, "DeleteResult")
      .def(py::init<>())
      .def("ToString", &DeleteResult::ToString)
      .def_readwrite("vector_id", &DeleteResult::vector_id)
      .def_readwrite("deleted", &DeleteResult::deleted);

  py::class_<QueryParam>(m, "QueryParam")
      .def(py::init<>())
      .def_readwrite("vector_ids", &QueryParam::vector_ids)
      .def_readwrite("with_vector_data", &QueryParam::with_vector_data)
      .def_readwrite("with_scalar_data", &QueryParam::with_scalar_data)
      .def_readwrite("selected_keys", &QueryParam::selected_keys)
      .def_readwrite("with_table_data", &QueryParam::with_table_data);

  py::class_<QueryResult>(m, "QueryResult")
      .def(py::init<>())
      .def("ToString", &QueryResult::ToString)
      .def_readwrite("vectors", &QueryResult::vectors);

  py::class_<ScanQueryParam>(m, "ScanQueryParam")
      .def(py::init<>())
      .def_readwrite("vector_id_start", &ScanQueryParam::vector_id_start)
      .def_readwrite("vector_id_end", &ScanQueryParam::vector_id_end)
      .def_readwrite("max_scan_count", &ScanQueryParam::max_scan_count)
      .def_readwrite("is_reverse", &ScanQueryParam::is_reverse)
      .def_readwrite("with_vector_data", &ScanQueryParam::with_vector_data)
      .def_readwrite("with_scalar_data", &ScanQueryParam::with_scalar_data)
      .def_readwrite("selected_keys", &ScanQueryParam::selected_keys)
      .def_readwrite("with_table_data", &ScanQueryParam::with_table_data)
      .def_readwrite("use_scalar_filter", &ScanQueryParam::use_scalar_filter)
      .def_readwrite("scalar_data", &ScanQueryParam::scalar_data);

  py::class_<ScanQueryResult>(m, "ScanQueryResult")
      .def(py::init<>())
      .def("ToString", &ScanQueryResult::ToString)
      .def_readwrite("vectors", &ScanQueryResult::vectors);

  py::class_<IndexMetricsResult>(m, "IndexMetricsResult")
      .def(py::init<>())
      .def("ToString", &IndexMetricsResult::ToString)
      .def_readwrite("index_type", &IndexMetricsResult::index_type)
      .def_readwrite("count", &IndexMetricsResult::count)
      .def_readwrite("deleted_count", &IndexMetricsResult::deleted_count)
      .def_readwrite("max_vector_id", &IndexMetricsResult::max_vector_id)
      .def_readwrite("min_vector_id", &IndexMetricsResult::min_vector_id)
      .def_readwrite("memory_bytes", &IndexMetricsResult::memory_bytes);

  py::class_<VectorIndexCreator>(m, "VectorIndexCreator")
      .def("SetSchemaId", &VectorIndexCreator::SetSchemaId)
      .def("SetName", &VectorIndexCreator::SetName)
      .def("SetRangePartitions", &VectorIndexCreator::SetRangePartitions)
      .def("SetReplicaNum", &VectorIndexCreator::SetReplicaNum)
      .def("SetFlatParam", &VectorIndexCreator::SetFlatParam)
      .def("SetIvfFlatParam", &VectorIndexCreator::SetIvfFlatParam)
      .def("SetIvfPqParam", &VectorIndexCreator::SetIvfPqParam)
      .def("SetHnswParam", &VectorIndexCreator::SetHnswParam)
      .def("SetBruteForceParam", &VectorIndexCreator::SetBruteForceParam)
      .def("SetAutoIncrementStart", &VectorIndexCreator::SetAutoIncrementStart)
      .def("SetScalarSchema", &VectorIndexCreator::SetScalarSchema)
      .def("Create", [](VectorIndexCreator& vectorindexcreator) -> std::tuple<Status, int64_t> {
        int64_t out_index_id;
        Status status = vectorindexcreator.Create(out_index_id);
        return std::make_tuple(status, out_index_id);
      });

  py::class_<VectorClient>(m, "VectorClient")
      .def("AddByIndexId", 
           [](VectorClient& vectorclient, int64_t index_id, std::vector<VectorWithId>& vectors, bool replace_deleted = false,
                      bool is_update = false){
             Status status = vectorclient.AddByIndexId(index_id, vectors, replace_deleted, is_update);
             return std::make_tuple(status, vectors);    
           }, py::arg(), py::arg(), py::arg()=false, py::arg()=false)
      .def("AddByIndexName",
           [](VectorClient& vectorclient, int64_t schema_id, const std::string& index_name, std::vector<VectorWithId>& vectors,
                        bool replace_deleted = false, bool is_update = false){
             Status status = vectorclient.AddByIndexName(schema_id, index_name, vectors, replace_deleted, is_update);
             return std::make_tuple(status, vectors); 
            }, py::arg(), py::arg(), py::arg(), py::arg()=false, py::arg()=false)
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
      .def("CountByIndexName", [](VectorClient& vectorclient, int64_t schema_id, const std::string& index_name,
                                  int64_t start_vector_id, int64_t end_vector_id) {
        int64_t out_count;
        Status status = vectorclient.CountByIndexName(schema_id, index_name, start_vector_id, end_vector_id, out_count);
        return std::make_tuple(status, out_count);
      });
}