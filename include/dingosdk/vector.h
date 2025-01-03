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

#ifndef DINGODB_SDK_VECTOR_H_
#define DINGODB_SDK_VECTOR_H_

#include <cstdint>
#include <map>
#include <string>
#include <vector>

#include "dingosdk/status.h"
#include "dingosdk/types.h"

namespace dingodb {
namespace sdk {

class ClientStub;

struct RegionStatus {
  int64_t region_id;
  Status status;
};

struct ErrStatusResult {
  std::vector<RegionStatus> region_status;
  std::string ToString() const;
};

enum DiskANNRegionState : uint8_t {
  kBuildFailed,
  kLoadFailed,
  kInittialized,
  kBuilding,
  kBuilded,
  kLoading,
  kLoaded,
  kNoData
};

struct RegionState {
  int64_t region_id;
  DiskANNRegionState state;
};

struct StateResult {
  std::vector<RegionState> region_states;
  std::string ToString() const;
};

std::string RegionStateToString(DiskANNRegionState state);

enum VectorIndexType : uint8_t {
  kNoneIndexType,
  kFlat,
  kIvfFlat,
  kIvfPq,
  kHnsw,
  kDiskAnn,
  kBruteForce,
  kBinaryFlat,
  kBinaryIvfFlat
};

std::string VectorIndexTypeToString(VectorIndexType type);

enum MetricType : uint8_t { kNoneMetricType, kL2, kInnerProduct, kCosine, kHamming };

std::string MetricTypeToString(MetricType type);

enum ValueType : uint8_t { kNoneValueType, kFloat, kUint8, kInt8 };

std::string ValueTypeToString(ValueType type);

struct FlatParam {
  // dimensions required
  int32_t dimension;
  // distance calculation method (L2 or InnerProduct) required
  MetricType metric_type;

  explicit FlatParam(int32_t p_dimension, MetricType p_metric_type)
      : dimension(p_dimension), metric_type(p_metric_type) {}

  static VectorIndexType Type() { return VectorIndexType::kFlat; }
};

struct IvfFlatParam {
  // dimensions required
  int32_t dimension;
  // distance calculation method (L2 or InnerProduct) required
  MetricType metric_type;
  // Number of cluster centers Default 2048 required
  int32_t ncentroids{2048};

  explicit IvfFlatParam(int32_t p_dimension, MetricType p_metric_type)
      : dimension(p_dimension), metric_type(p_metric_type) {}

  static VectorIndexType Type() { return VectorIndexType::kIvfFlat; }
};

struct IvfPqParam {
  // dimensions required
  int32_t dimension;
  // distance calculation method (L2 or InnerProduct) required
  MetricType metric_type;
  // Number of cluster centers Default 2048 required
  int32_t ncentroids{2048};
  // PQ split sub-vector size default 64 required
  int32_t nsubvector{64};
  // Inverted list (IVF) bucket initialization size default 1000 optional
  int32_t bucket_init_size{1000};
  // Inverted list (IVF) bucket maximum capacity default 1280000 optional
  int32_t bucket_max_size{1280000};
  // bit number of sub cluster center. default 8 required.  means 256.
  int32_t nbits_per_idx{8};

  explicit IvfPqParam(int32_t p_dimension, MetricType p_metric_type)
      : dimension(p_dimension), metric_type(p_metric_type) {}

  static VectorIndexType Type() { return VectorIndexType::kIvfPq; }
};

struct HnswParam {
  // dimensions required
  int32_t dimension;
  // distance calculation method (L2 or InnerProduct) required
  MetricType metric_type;
  // the range traversed in the graph during the process of finding node neighbors when
  // composing the graph. The larger the value, the better the composition effect and the
  // longer the composition time. Default 40 required
  int32_t ef_construction{40};
  // Set the maximum number of elements. required
  int32_t max_elements = 4;
  // The number of node neighbors, the larger the value, the better the composition effect, and the
  // more memory it takes. Default 32. required .
  int32_t nlinks{32};

  explicit HnswParam(int32_t p_dimension, MetricType p_metric_type, int32_t p_max_elements)
      : dimension(p_dimension), metric_type(p_metric_type), max_elements(p_max_elements) {}

  static VectorIndexType Type() { return VectorIndexType::kHnsw; }
};

struct DiskAnnParam {
  // The number of dimensions in the vector data. required
  int32_t dimension;

  // distance calculation method (L2 or InnerProduct) required
  // The distance calculation method to be used for the index.
  MetricType metric_type;

  // value_type , one of {int8, uint8, float} - float is single precision (32 bit)
  // Note that we currently only support float. default is float. required
  ValueType value_type{kFloat};

  // the degree of the graph index, typically between 60 and 150.
  // Larger max_degree will result in larger indices and longer indexing times, but better search quality.
  // (default is 64) . R . required
  int32_t max_degree{64};

  // the size of search list during index build. Typical values are between 75 to 200.
  // Larger values will take more time to build but result in indices that provide higher recall for the same search
  // complexity. Use a value for search_list_size value that is at least the value of max_degree unless you need to
  // build indices really quickly and can somewhat compromise on quality. (default is 100) . L . required
  int32_t search_list_size{100};

  explicit DiskAnnParam(int32_t p_dimension, MetricType p_metric_type, ValueType p_value_type)
      : dimension(p_dimension), metric_type(p_metric_type), value_type(p_value_type) {}

  static VectorIndexType Type() { return VectorIndexType::kDiskAnn; }
};

struct BruteForceParam {
  // dimensions required
  int32_t dimension;
  // distance calculation method (L2 or InnerProduct) required
  MetricType metric_type;

  explicit BruteForceParam(int32_t p_dimension, MetricType p_metric_type)
      : dimension(p_dimension), metric_type(p_metric_type) {}

  static VectorIndexType Type() { return VectorIndexType::kBruteForce; }
};

struct BinaryFlatParam {
  // dimensions required
  int32_t dimension;
  // distance calculation method (hamming) required
  MetricType metric_type;

  explicit BinaryFlatParam(int32_t p_dimension, MetricType p_metric_type)
      : dimension(p_dimension), metric_type(p_metric_type) {}

  static VectorIndexType Type() { return VectorIndexType::kBinaryFlat; }
};

struct BinaryIvfFlatParam {
  // dimensions required
  int32_t dimension;
  // distance calculation method (hamming) required
  MetricType metric_type;
  // Number of cluster centers Default 2048
  int32_t ncentroids{2048};

  explicit BinaryIvfFlatParam(int32_t p_dimension, MetricType p_metric_type)
      : dimension(p_dimension), metric_type(p_metric_type) {}

  static VectorIndexType Type() { return VectorIndexType::kBinaryIvfFlat; }
};

struct VectorScalarColumnSchema {
  std::string key;
  Type type;
  bool speed;

  VectorScalarColumnSchema(const std::string& key, Type type, bool speed = false)
      : key(key), type(type), speed(speed) {}
};

struct VectorScalarSchema {
  void AddScalarColumn(const VectorScalarColumnSchema& col) { cols.push_back(col); }

  std::vector<VectorScalarColumnSchema> cols;
};

struct VectorLoadDiskAnnParam {
  uint32_t num_nodes_to_cache{0};
  bool warmup{true};
  explicit VectorLoadDiskAnnParam(uint32_t p_num_nodes_to_cache = 0, bool p_warmup = true)
      : num_nodes_to_cache(p_num_nodes_to_cache), warmup(p_warmup) {}
};

struct Vector {
  int32_t dimension;
  ValueType value_type;
  std::vector<float> float_values;
  std::vector<uint8_t> binary_values;

  explicit Vector() : value_type(kNoneValueType), dimension(0) {}

  explicit Vector(ValueType p_value_type, int32_t p_dimension) : value_type(p_value_type), dimension(p_dimension) {}

  Vector(Vector&& other) = default;
  Vector& operator=(Vector&& other) = default;

  Vector(const Vector& other) = default;
  Vector& operator=(const Vector&) = default;

  uint32_t Size() const { return float_values.size() * 4 + binary_values.size() + 4; }

  std::string ToString() const;
};

// TODO: maybe use std::variant, when swig support
struct ScalarField {
  bool bool_data;
  int64_t long_data;
  double double_data;
  std::string string_data;
};

struct ScalarValue {
  Type type;
  std::vector<ScalarField> fields;

  std::string ToString() const;
};

struct VectorWithId {
  int64_t id;
  Vector vector;
  std::map<std::string, ScalarValue> scalar_data;
  //  TODO: maybe support table data

  explicit VectorWithId() : id(0) {}

  explicit VectorWithId(int64_t p_id, Vector p_vector) : id(p_id), vector(std::move(p_vector)) {}

  explicit VectorWithId(Vector p_vector) : id(0), vector(std::move(p_vector)) {}

  VectorWithId(VectorWithId&& other) = default;
  VectorWithId& operator=(VectorWithId&& other) = default;

  VectorWithId(const VectorWithId& other) = default;
  VectorWithId& operator=(const VectorWithId&) = default;

  std::string ToString() const;
};

enum FilterSource : uint8_t {
  kNoneFilterSource,
  // filter vector scalar include post filter and pre filter
  kScalarFilter,
  // use coprocessor only include pre filter
  kTableFilter,
  // vector id search direct by ids. only include pre filter
  kVectorIdFilter
};

enum FilterType : uint8_t {
  kNoneFilterType,
  // first vector search, then filter
  kQueryPost,
  // first search from rocksdb, then search vector
  kQueryPre
};

enum SearchExtraParamType : uint8_t { kParallelOnQueries, kNprobe, kRecallNum, kEfSearch };

struct SearchParam {
  int32_t topk{0};
  bool with_vector_data{true};
  bool with_scalar_data{false};
  std::vector<std::string> selected_keys;
  bool with_table_data{false};      // Default false, if true, response without table data
  bool enable_range_search{false};  // if enable_range_search = true. top_n disabled.
  float radius{0.0f};
  FilterSource filter_source{kNoneFilterSource};
  FilterType filter_type{kNoneFilterType};
  bool is_negation{false};
  bool is_sorted{false};
  std::vector<int64_t> vector_ids;  // vector id array; filter_source == kVectorIdFilter enable vector_ids
  bool use_brute_force{false};      // use brute-force search
  std::map<SearchExtraParamType, int32_t> extra_params;  // The search method to use
  std::string langchain_expr_json;                       // must json format, will convert to coprocessor
  uint32_t beamwidth{2};

  explicit SearchParam() = default;

  SearchParam(SearchParam&& other) noexcept
      : topk(other.topk),
        with_vector_data(other.with_vector_data),
        with_scalar_data(other.with_scalar_data),
        with_table_data(other.with_table_data),
        enable_range_search(other.enable_range_search),
        radius(other.radius),
        filter_source(other.filter_source),
        filter_type(other.filter_type),
        vector_ids(std::move(other.vector_ids)),
        use_brute_force(other.use_brute_force),
        extra_params(std::move(other.extra_params)),
        langchain_expr_json(std::move(other.langchain_expr_json)),
        beamwidth(other.beamwidth) {
    other.topk = 0;
    other.with_vector_data = true;
    other.with_scalar_data = false;
    other.with_table_data = false;
    other.enable_range_search = false;
    other.radius = 0.0f;
    other.filter_source = kNoneFilterSource;
    other.filter_type = kNoneFilterType;
    other.use_brute_force = false;
    other.beamwidth = 2;
  }

  SearchParam& operator=(SearchParam&& other) noexcept {
    topk = other.topk;
    with_vector_data = other.with_vector_data;
    with_scalar_data = other.with_scalar_data;
    with_table_data = other.with_table_data;
    enable_range_search = other.enable_range_search;
    radius = other.radius;
    filter_source = other.filter_source;
    filter_type = other.filter_type;
    vector_ids = std::move(other.vector_ids);
    use_brute_force = other.use_brute_force;
    extra_params = std::move(other.extra_params);
    langchain_expr_json = std::move(other.langchain_expr_json);
    beamwidth = other.beamwidth;

    other.topk = 0;
    other.with_vector_data = true;
    other.with_scalar_data = false;
    other.with_table_data = false;
    other.enable_range_search = false;
    other.radius = 0.0f;
    other.filter_source = kNoneFilterSource;
    other.filter_type = kNoneFilterType;
    other.use_brute_force = false;
    other.beamwidth = 2;

    return *this;
  }
};

struct VectorWithDistance {
  VectorWithId vector_data;
  float distance;
  MetricType metric_type{kNoneMetricType};

  explicit VectorWithDistance() = default;

  VectorWithDistance(VectorWithDistance&& other) = default;
  VectorWithDistance& operator=(VectorWithDistance&& other) = default;

  VectorWithDistance(const VectorWithDistance&) = default;
  VectorWithDistance& operator=(const VectorWithDistance&) = default;

  std::string ToString() const;
};

struct SearchResult {
  // TODO : maybe remove VectorWithId
  VectorWithId id;
  std::vector<VectorWithDistance> vector_datas;

  SearchResult() = default;

  explicit SearchResult(VectorWithId p_id) : id(std::move(p_id)) {}

  SearchResult(SearchResult&& other) noexcept : id(std::move(other.id)), vector_datas(std::move(other.vector_datas)) {}

  SearchResult& operator=(SearchResult&& other) noexcept {
    id = std::move(other.id);
    vector_datas = std::move(other.vector_datas);
    return *this;
  }

  SearchResult(const SearchResult&) = default;
  SearchResult& operator=(const SearchResult&) = default;

  std::string ToString() const;
};

struct DeleteResult {
  int64_t vector_id;
  bool deleted;

  std::string ToString() const;
};

struct QueryParam {
  std::vector<int64_t> vector_ids;
  // If true, response with vector data
  bool with_vector_data{true};
  // if true, response with scalar data
  bool with_scalar_data{false};
  // If with_scalar_data is true, selected_keys is used to select scalar data, and if this parameter is null, all scalar
  // data will be returned.
  std::vector<std::string> selected_keys;
  // if true, response witho table data
  bool with_table_data{false};
};

struct QueryResult {
  std::vector<VectorWithId> vectors;

  std::string ToString() const;
};

struct ScanQueryParam {
  int64_t vector_id_start;
  // the end id of scan
  // if is_reverse is true, vector_id_end must be less than vector_id_start
  // if is_reverse is false, vector_id_end must be greater than vector_id_start
  // the real range is [start, end], include start and end
  // if vector_id_end == 0, scan to the end of the region
  int64_t vector_id_end{0};
  int64_t max_scan_count{0};
  bool is_reverse{false};

  bool with_vector_data{true};
  bool with_scalar_data{false};
  // If with_scalar_data is true, selected_keys is used to select scalar data, and if this parameter is null, all scalar
  // data will be returned.
  std::vector<std::string> selected_keys;
  bool with_table_data{false};  // Default false, if true, response without table data

  bool use_scalar_filter{false};
  std::map<std::string, ScalarValue> scalar_data;

  explicit ScanQueryParam() = default;

  ScanQueryParam(ScanQueryParam&& other) noexcept
      : vector_id_start(other.vector_id_start),
        vector_id_end(other.vector_id_end),
        max_scan_count(other.max_scan_count),
        is_reverse(other.is_reverse),
        with_vector_data(other.with_vector_data),
        with_scalar_data(other.with_scalar_data),
        selected_keys(std::move(other.selected_keys)),
        with_table_data(other.with_table_data),
        use_scalar_filter(other.use_scalar_filter),
        scalar_data(std::move(other.scalar_data)) {}

  ScanQueryParam& operator=(ScanQueryParam&& other) noexcept {
    if (this != &other) {
      vector_id_start = other.vector_id_start;
      vector_id_end = other.vector_id_end;
      max_scan_count = other.max_scan_count;
      is_reverse = other.is_reverse;
      with_vector_data = other.with_vector_data;
      with_scalar_data = other.with_scalar_data;
      selected_keys = std::move(other.selected_keys);
      with_table_data = other.with_table_data;
      use_scalar_filter = other.use_scalar_filter;
      scalar_data = std::move(other.scalar_data);
    }
    return *this;
  }
};

struct ScanQueryResult {
  std::vector<VectorWithId> vectors;

  std::string ToString() const;
};

struct IndexMetricsResult {
  VectorIndexType index_type{kNoneIndexType};
  int64_t count{0};
  int64_t deleted_count{0};
  int64_t max_vector_id{0};
  int64_t min_vector_id{0};
  int64_t memory_bytes{0};

  std::string ToString() const;
};

class VectorIndexCreator {
 public:
  ~VectorIndexCreator();

  VectorIndexCreator& SetSchemaId(int64_t schema_id);

  VectorIndexCreator& SetName(const std::string& name);

  VectorIndexCreator& SetRangePartitions(std::vector<int64_t> separator_id);

  VectorIndexCreator& SetReplicaNum(int64_t num);

  // one of FlatParam/IvfFlatParam/HnswParam/DiskAnnParam/BruteForceParam/BinaryFlat/BinaryIvfFlat, if set multiple, the
  // last one will effective
  VectorIndexCreator& SetFlatParam(const FlatParam& params);
  VectorIndexCreator& SetIvfFlatParam(const IvfFlatParam& params);
  VectorIndexCreator& SetIvfPqParam(const IvfPqParam& params);
  VectorIndexCreator& SetHnswParam(const HnswParam& params);
  VectorIndexCreator& SetDiskAnnParam(const DiskAnnParam& params);
  VectorIndexCreator& SetBruteForceParam(const BruteForceParam& params);
  VectorIndexCreator& SetBinaryFlatParam(const BinaryFlatParam& params);
  VectorIndexCreator& SetBinaryIvfFlatParam(const BinaryIvfFlatParam& params);

  // when start_id greater than 0, index is enable auto_increment
  VectorIndexCreator& SetAutoIncrementStart(int64_t start_id);

  VectorIndexCreator& SetScalarSchema(const VectorScalarSchema& schema);

  Status Create(int64_t& out_index_id);

 private:
  friend class Client;

  // own
  class Data;
  Data* data_;
  explicit VectorIndexCreator(Data* data);
};

class VectorClient {
 public:
  VectorClient(const VectorClient&) = delete;
  const VectorClient& operator=(const VectorClient&) = delete;

  ~VectorClient() = default;

  Status AddByIndexId(int64_t index_id, std::vector<VectorWithId>& vectors);
  Status AddByIndexName(int64_t schema_id, const std::string& index_name, std::vector<VectorWithId>& vectors);

  Status UpsertByIndexId(int64_t index_id, std::vector<VectorWithId>& vectors);
  Status UpsertByIndexName(int64_t schema_id, const std::string& index_name, std::vector<VectorWithId>& vectors);

  Status SearchByIndexId(int64_t index_id, const SearchParam& search_param,
                         const std::vector<VectorWithId>& target_vectors, std::vector<SearchResult>& out_result);
  Status SearchByIndexName(int64_t schema_id, const std::string& index_name, const SearchParam& search_param,
                           const std::vector<VectorWithId>& target_vectors, std::vector<SearchResult>& out_result);

  Status DeleteByIndexId(int64_t index_id, const std::vector<int64_t>& vector_ids,
                         std::vector<DeleteResult>& out_result);
  Status DeleteByIndexName(int64_t schema_id, const std::string& index_name, const std::vector<int64_t>& vector_ids,
                           std::vector<DeleteResult>& out_result);

  Status BatchQueryByIndexId(int64_t index_id, const QueryParam& query_param, QueryResult& out_result);
  Status BatchQueryByIndexName(int64_t schema_id, const std::string& index_name, const QueryParam& query_param,
                               QueryResult& out_result);

  Status GetBorderByIndexId(int64_t index_id, bool is_max, int64_t& out_vector_id);
  Status GetBorderByIndexName(int64_t schema_id, const std::string& index_name, bool is_max, int64_t& out_vector_id);

  Status ScanQueryByIndexId(int64_t index_id, const ScanQueryParam& query_param, ScanQueryResult& out_result);
  Status ScanQueryByIndexName(int64_t schema_id, const std::string& index_name, const ScanQueryParam& query_param,
                              ScanQueryResult& out_result);

  Status GetIndexMetricsByIndexId(int64_t index_id, IndexMetricsResult& out_result);
  Status GetIndexMetricsByIndexName(int64_t schema_id, const std::string& index_name, IndexMetricsResult& out_result);

  Status CountAllByIndexId(int64_t index_id, int64_t& out_count);
  Status CountallByIndexName(int64_t schema_id, const std::string& index_name, int64_t& out_count);

  Status CountByIndexId(int64_t index_id, int64_t start_vector_id, int64_t end_vector_id, int64_t& out_count);
  Status CountByIndexName(int64_t schema_id, const std::string& index_name, int64_t start_vector_id,
                          int64_t end_vector_id, int64_t& out_count);

  // DiskANN
  Status StatusByIndexId(int64_t index_id, StateResult& result);
  Status StatusByIndexName(int64_t schema_id, const std::string& index_name, StateResult& result);
  Status StatusByRegionId(int64_t index_id, const std::vector<int64_t>& region_ids, StateResult& result);
  Status StatusByRegionIdIndexName(int64_t schema_id, const std::string& index_name,
                                   const std::vector<int64_t>& region_ids, StateResult& result);

  Status BuildByIndexId(int64_t index_id, ErrStatusResult& result);
  Status BuildByIndexName(int64_t schema_id, const std::string& index_name, ErrStatusResult& result);
  Status BuildByRegionId(int64_t index_id, const std::vector<int64_t>& region_ids, ErrStatusResult& result);
  Status BuildByRegionIdIndexName(int64_t schema_id, const std::string& index_name,
                                  const std::vector<int64_t>& region_ids, ErrStatusResult& result);

  Status LoadByIndexId(int64_t index_id, ErrStatusResult& result);
  Status LoadByIndexName(int64_t schema_id, const std::string& index_name, ErrStatusResult& result);
  Status LoadByRegionId(int64_t index_id, const std::vector<int64_t>& region_ids, ErrStatusResult& result);
  Status LoadByRegionIdIndexName(int64_t schema_id, const std::string& index_name,
                                 const std::vector<int64_t>& region_ids, ErrStatusResult& result);

  Status ResetByIndexId(int64_t index_id, ErrStatusResult& result);
  Status ResetByIndexName(int64_t schema_id, const std::string& index_name, ErrStatusResult& result);
  Status ResetByRegionId(int64_t index_id, const std::vector<int64_t>& region_ids, ErrStatusResult& result);
  Status ResetByRegionIdIndexName(int64_t schema_id, const std::string& index_name,
                                  const std::vector<int64_t>& region_ids, ErrStatusResult& result);

  Status ImportAddByIndexId(int64_t index_id, std::vector<VectorWithId>& vectors);
  Status ImportAddByIndexName(int64_t schema_id, const std::string& index_name, std::vector<VectorWithId>& vectors);

  Status ImportDeleteByIndexId(int64_t index_id, const std::vector<int64_t>& vector_ids);
  Status ImportDeleteByIndexName(int64_t schema_id, const std::string& index_name,
                                 const std::vector<int64_t>& vector_ids);

  Status CountMemoryByIndexId(int64_t index_id, int64_t& count);
  Status CountMemoryByIndexName(int64_t schema_id, const std::string& index_name, int64_t& count);

  // dump
  Status DumpByIndexId(int64_t index_id, std::vector<std::string>& datas);
  Status DumpByIndexName(int64_t schema_id, const std::string& index_name, std::vector<std::string>& datas);

  Status GetAutoIncrementIdByIndexId(int64_t index_id, int64_t& start_id);
  Status GetAutoIncrementIdByIndexName(int64_t schema_id, const std::string& index_name, int64_t& start_id);

  Status UpdateAutoIncrementIdByIndexId(int64_t index_id, int64_t start_id);
  Status UpdateAutoIncrementIdByIndexName(int64_t schema_id, const std::string& index_name, int64_t start_id);

 private:
  friend class Client;

  const ClientStub& stub_;

  explicit VectorClient(const ClientStub& stub);
};
}  // namespace sdk

}  // namespace dingodb
#endif  // DINGODB_SDK_VECTOR_H_