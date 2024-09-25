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

#include <cstdint>

#include "sdk/client_stub.h"
#include "sdk/status.h"
#include "sdk/vector.h"
#include "sdk/vector/diskann/vector_diskann_build_by_index_task.h"
#include "sdk/vector/diskann/vector_diskann_build_by_region_task.h"
#include "sdk/vector/diskann/vector_diskann_count_memory_task.h"
#include "sdk/vector/diskann/vector_diskann_dump.h"
#include "sdk/vector/diskann/vector_diskann_import_task.h"
#include "sdk/vector/diskann/vector_diskann_load_by_index_task.h"
#include "sdk/vector/diskann/vector_diskann_load_by_region_task.h"
#include "sdk/vector/diskann/vector_diskann_reset_by_index_task.h"
#include "sdk/vector/diskann/vector_diskann_reset_by_region_task.h"
#include "sdk/vector/diskann/vector_diskann_status_by_index_task.h"
#include "sdk/vector/diskann/vector_diskann_status_by_region_task.h"
#include "sdk/vector/vector_add_task.h"
#include "sdk/vector/vector_batch_query_task.h"
#include "sdk/vector/vector_count_task.h"
#include "sdk/vector/vector_delete_task.h"
#include "sdk/vector/vector_get_border_task.h"
#include "sdk/vector/vector_get_index_metrics_task.h"
#include "sdk/vector/vector_index_cache.h"
#include "sdk/vector/vector_scan_query_task.h"
#include "sdk/vector/vector_search_task.h"
#include "sdk/vector/vector_upsert_task.h"

namespace dingodb {
namespace sdk {

VectorClient::VectorClient(const ClientStub& stub) : stub_(stub) {}

Status VectorClient::AddByIndexId(int64_t index_id, std::vector<VectorWithId>& vectors) {
  VectorAddTask task(stub_, index_id, vectors);
  return task.Run();
}

Status VectorClient::AddByIndexName(int64_t schema_id, const std::string& index_name,
                                    std::vector<VectorWithId>& vectors) {
  int64_t index_id{0};
  DINGO_RETURN_NOT_OK(
      stub_.GetVectorIndexCache()->GetIndexIdByKey(EncodeVectorIndexCacheKey(schema_id, index_name), index_id));
  CHECK_GT(index_id, 0);
  VectorAddTask task(stub_, index_id, vectors);
  return task.Run();
}

Status VectorClient::UpsertByIndexId(int64_t index_id, std::vector<VectorWithId>& vectors) {
  VectorUpsertTask task(stub_, index_id, vectors);
  return task.Run();
}

Status VectorClient::UpsertByIndexName(int64_t schema_id, const std::string& index_name,
                                       std::vector<VectorWithId>& vectors) {
  int64_t index_id{0};
  DINGO_RETURN_NOT_OK(
      stub_.GetVectorIndexCache()->GetIndexIdByKey(EncodeVectorIndexCacheKey(schema_id, index_name), index_id));
  CHECK_GT(index_id, 0);
  VectorUpsertTask task(stub_, index_id, vectors);
  return task.Run();
}

Status VectorClient::SearchByIndexId(int64_t index_id, const SearchParam& search_param,
                                     const std::vector<VectorWithId>& target_vectors,
                                     std::vector<SearchResult>& out_result) {
  VectorSearchTask task(stub_, index_id, search_param, target_vectors, out_result);
  return task.Run();
}

Status VectorClient::SearchByIndexName(int64_t schema_id, const std::string& index_name,
                                       const SearchParam& search_param, const std::vector<VectorWithId>& target_vectors,
                                       std::vector<SearchResult>& out_result) {
  int64_t index_id{0};
  DINGO_RETURN_NOT_OK(
      stub_.GetVectorIndexCache()->GetIndexIdByKey(EncodeVectorIndexCacheKey(schema_id, index_name), index_id));
  CHECK_GT(index_id, 0);
  VectorSearchTask task(stub_, index_id, search_param, target_vectors, out_result);
  return task.Run();
}

Status VectorClient::DeleteByIndexId(int64_t index_id, const std::vector<int64_t>& vector_ids,
                                     std::vector<DeleteResult>& out_result) {
  VectorDeleteTask task(stub_, index_id, vector_ids, out_result);
  return task.Run();
}

Status VectorClient::DeleteByIndexName(int64_t schema_id, const std::string& index_name,
                                       const std::vector<int64_t>& vector_ids, std::vector<DeleteResult>& out_result) {
  int64_t index_id{0};
  DINGO_RETURN_NOT_OK(
      stub_.GetVectorIndexCache()->GetIndexIdByKey(EncodeVectorIndexCacheKey(schema_id, index_name), index_id));
  CHECK_GT(index_id, 0);
  VectorDeleteTask task(stub_, index_id, vector_ids, out_result);
  return task.Run();
}

Status VectorClient::BatchQueryByIndexId(int64_t index_id, const QueryParam& query_param, QueryResult& out_result) {
  VectorBatchQueryTask task(stub_, index_id, query_param, out_result);
  return task.Run();
}

Status VectorClient::BatchQueryByIndexName(int64_t schema_id, const std::string& index_name,
                                           const QueryParam& query_param, QueryResult& out_result) {
  int64_t index_id{0};
  DINGO_RETURN_NOT_OK(
      stub_.GetVectorIndexCache()->GetIndexIdByKey(EncodeVectorIndexCacheKey(schema_id, index_name), index_id));
  CHECK_GT(index_id, 0);
  VectorBatchQueryTask task(stub_, index_id, query_param, out_result);
  return task.Run();
}

Status VectorClient::GetBorderByIndexId(int64_t index_id, bool is_max, int64_t& out_vector_id) {
  VectorGetBorderTask task(stub_, index_id, is_max, out_vector_id);
  return task.Run();
}

Status VectorClient::GetBorderByIndexName(int64_t schema_id, const std::string& index_name, bool is_max,
                                          int64_t& out_vector_id) {
  int64_t index_id{0};
  DINGO_RETURN_NOT_OK(
      stub_.GetVectorIndexCache()->GetIndexIdByKey(EncodeVectorIndexCacheKey(schema_id, index_name), index_id));
  CHECK_GT(index_id, 0);
  VectorGetBorderTask task(stub_, index_id, is_max, out_vector_id);
  return task.Run();
}

Status VectorClient::ScanQueryByIndexId(int64_t index_id, const ScanQueryParam& query_param,
                                        ScanQueryResult& out_result) {
  VectorScanQueryTask task(stub_, index_id, query_param, out_result);
  return task.Run();
}

Status VectorClient::ScanQueryByIndexName(int64_t schema_id, const std::string& index_name,
                                          const ScanQueryParam& query_param, ScanQueryResult& out_result) {
  int64_t index_id{0};
  DINGO_RETURN_NOT_OK(
      stub_.GetVectorIndexCache()->GetIndexIdByKey(EncodeVectorIndexCacheKey(schema_id, index_name), index_id));
  CHECK_GT(index_id, 0);
  VectorScanQueryTask task(stub_, index_id, query_param, out_result);
  return task.Run();
}

Status VectorClient::GetIndexMetricsByIndexId(int64_t index_id, IndexMetricsResult& out_result) {
  VectorGetIndexMetricsTask task(stub_, index_id, out_result);
  return task.Run();
}

Status VectorClient::GetIndexMetricsByIndexName(int64_t schema_id, const std::string& index_name,
                                                IndexMetricsResult& out_result) {
  int64_t index_id{0};
  DINGO_RETURN_NOT_OK(
      stub_.GetVectorIndexCache()->GetIndexIdByKey(EncodeVectorIndexCacheKey(schema_id, index_name), index_id));
  CHECK_GT(index_id, 0);
  VectorGetIndexMetricsTask task(stub_, index_id, out_result);
  return task.Run();
}

Status VectorClient::CountAllByIndexId(int64_t index_id, int64_t& out_count) {
  VectorCountTask task(stub_, index_id, 0, INT64_MAX, out_count);
  return task.Run();
}

Status VectorClient::CountallByIndexName(int64_t schema_id, const std::string& index_name, int64_t& out_count) {
  int64_t index_id{0};
  DINGO_RETURN_NOT_OK(
      stub_.GetVectorIndexCache()->GetIndexIdByKey(EncodeVectorIndexCacheKey(schema_id, index_name), index_id));
  CHECK_GT(index_id, 0);

  VectorCountTask task(stub_, index_id, 0, INT64_MAX, out_count);
  return task.Run();
}

Status VectorClient::CountByIndexId(int64_t index_id, int64_t start_vector_id, int64_t end_vector_id,
                                    int64_t& out_count) {
  VectorCountTask task(stub_, index_id, start_vector_id, end_vector_id, out_count);
  return task.Run();
}

Status VectorClient::CountByIndexName(int64_t schema_id, const std::string& index_name, int64_t start_vector_id,
                                      int64_t end_vector_id, int64_t& out_count) {
  int64_t index_id{0};
  DINGO_RETURN_NOT_OK(
      stub_.GetVectorIndexCache()->GetIndexIdByKey(EncodeVectorIndexCacheKey(schema_id, index_name), index_id));
  CHECK_GT(index_id, 0);

  VectorCountTask task(stub_, index_id, start_vector_id, end_vector_id, out_count);
  return task.Run();
}

Status VectorClient::StatusByIndexId(int64_t index_id, StateResult& result) {
  VectorStatusByIndexTask task(stub_, index_id, result);
  return task.Run();
}

Status VectorClient::StatusByRegionId(int64_t index_id, const std::vector<int64_t>& region_ids, StateResult& result) {
  VectorStatusByRegionTask task(stub_, index_id, region_ids, result);
  return task.Run();
}

Status VectorClient::StatusByIndexName(int64_t schema_id, const std::string& index_name, StateResult& result) {
  int64_t index_id{0};
  DINGO_RETURN_NOT_OK(
      stub_.GetVectorIndexCache()->GetIndexIdByKey(EncodeVectorIndexCacheKey(schema_id, index_name), index_id));
  CHECK_GT(index_id, 0);
  VectorStatusByIndexTask task(stub_, index_id, result);
  return task.Run();
}

Status VectorClient::StatusByRegionIdIndexName(int64_t schema_id, const std::string& index_name,
                                               const std::vector<int64_t>& region_ids, StateResult& result) {
  int64_t index_id{0};
  DINGO_RETURN_NOT_OK(
      stub_.GetVectorIndexCache()->GetIndexIdByKey(EncodeVectorIndexCacheKey(schema_id, index_name), index_id));
  CHECK_GT(index_id, 0);
  VectorStatusByRegionTask task(stub_, index_id, region_ids, result);
  return task.Run();
}

Status VectorClient::BuildByIndexId(int64_t index_id, ErrStatusResult& result) {
  VectorBuildByIndexTask task(stub_, index_id, result);
  return task.Run();
}
Status VectorClient::BuildByRegionId(int64_t index_id, const std::vector<int64_t>& region_ids,
                                     ErrStatusResult& result) {
  VectorBuildByRegionTask task(stub_, index_id, region_ids, result);
  return task.Run();
}

Status VectorClient::BuildByIndexName(int64_t schema_id, const std::string& index_name, ErrStatusResult& result) {
  int64_t index_id{0};
  DINGO_RETURN_NOT_OK(
      stub_.GetVectorIndexCache()->GetIndexIdByKey(EncodeVectorIndexCacheKey(schema_id, index_name), index_id));
  CHECK_GT(index_id, 0);
  VectorBuildByIndexTask task(stub_, index_id, result);
  return task.Run();
}

Status VectorClient::BuildByRegionIdIndexName(int64_t schema_id, const std::string& index_name,
                                              const std::vector<int64_t>& region_ids, ErrStatusResult& result) {
  int64_t index_id{0};
  DINGO_RETURN_NOT_OK(
      stub_.GetVectorIndexCache()->GetIndexIdByKey(EncodeVectorIndexCacheKey(schema_id, index_name), index_id));
  CHECK_GT(index_id, 0);
  VectorBuildByRegionTask task(stub_, index_id, region_ids, result);
  return task.Run();
}

Status VectorClient::LoadByIndexId(int64_t index_id, ErrStatusResult& result) {
  VectorLoadByIndexTask task(stub_, index_id, result);
  return task.Run();
}

Status VectorClient::LoadByRegionId(int64_t index_id, const std::vector<int64_t>& region_ids, ErrStatusResult& result) {
  VectorLoadByRegionTask task(stub_, index_id, region_ids, result);
  return task.Run();
}

Status VectorClient::LoadByIndexName(int64_t schema_id, const std::string& index_name, ErrStatusResult& result) {
  int64_t index_id{0};
  DINGO_RETURN_NOT_OK(
      stub_.GetVectorIndexCache()->GetIndexIdByKey(EncodeVectorIndexCacheKey(schema_id, index_name), index_id));
  CHECK_GT(index_id, 0);
  VectorLoadByIndexTask task(stub_, index_id, result);
  return task.Run();
}

Status VectorClient::LoadByRegionIdIndexName(int64_t schema_id, const std::string& index_name,
                                             const std::vector<int64_t>& region_ids, ErrStatusResult& result) {
  int64_t index_id{0};
  DINGO_RETURN_NOT_OK(
      stub_.GetVectorIndexCache()->GetIndexIdByKey(EncodeVectorIndexCacheKey(schema_id, index_name), index_id));
  CHECK_GT(index_id, 0);
  VectorLoadByRegionTask task(stub_, index_id, region_ids, result);
  return task.Run();
}

Status VectorClient::ResetByIndexId(int64_t index_id, ErrStatusResult& result) {
  VectorResetByIndexTask task(stub_, index_id, result);
  return task.Run();
}

Status VectorClient::ResetByRegionId(int64_t index_id, const std::vector<int64_t>& region_ids,
                                     ErrStatusResult& result) {
  VectorResetByRegionTask task(stub_, index_id, region_ids, result);
  return task.Run();
}

Status VectorClient::ResetByIndexName(int64_t schema_id, const std::string& index_name, ErrStatusResult& result) {
  int64_t index_id{0};
  DINGO_RETURN_NOT_OK(
      stub_.GetVectorIndexCache()->GetIndexIdByKey(EncodeVectorIndexCacheKey(schema_id, index_name), index_id));
  CHECK_GT(index_id, 0);
  VectorResetByIndexTask task(stub_, index_id, result);
  return task.Run();
}
Status VectorClient::ResetByRegionIdIndexName(int64_t schema_id, const std::string& index_name,
                                              const std::vector<int64_t>& region_ids, ErrStatusResult& result) {
  int64_t index_id{0};
  DINGO_RETURN_NOT_OK(
      stub_.GetVectorIndexCache()->GetIndexIdByKey(EncodeVectorIndexCacheKey(schema_id, index_name), index_id));
  CHECK_GT(index_id, 0);
  VectorResetByRegionTask task(stub_, index_id, region_ids, result);
  return task.Run();
}

Status VectorClient::CountMemoryByIndexId(int64_t index_id, int64_t& count) {
  VectorCountMemoryByIndexTask task(stub_, index_id, count);
  return task.Run();
}

Status VectorClient::CountMemoryByIndexName(int64_t schema_id, const std::string& index_name, int64_t& count) {
  int64_t index_id{0};
  DINGO_RETURN_NOT_OK(
      stub_.GetVectorIndexCache()->GetIndexIdByKey(EncodeVectorIndexCacheKey(schema_id, index_name), index_id));
  CHECK_GT(index_id, 0);
  VectorCountMemoryByIndexTask task(stub_, index_id, count);
  return task.Run();
}

Status VectorClient::ImportAddByIndexId(int64_t index_id, std::vector<VectorWithId>& vectors) {
  VectorImportAddTask task(stub_, index_id, vectors);
  return task.Run();
}

Status VectorClient::ImportAddByIndexName(int64_t schema_id, const std::string& index_name,
                                          std::vector<VectorWithId>& vectors) {
  int64_t index_id{0};
  DINGO_RETURN_NOT_OK(
      stub_.GetVectorIndexCache()->GetIndexIdByKey(EncodeVectorIndexCacheKey(schema_id, index_name), index_id));
  CHECK_GT(index_id, 0);
  VectorImportAddTask task(stub_, index_id, vectors);
  return task.Run();
}

Status VectorClient::ImportDeleteByIndexId(int64_t index_id, const std::vector<int64_t>& vector_ids) {
  VectorImportDeleteTask task(stub_, index_id, vector_ids);
  return task.Run();
}

Status VectorClient::ImportDeleteByIndexName(int64_t schema_id, const std::string& index_name,
                                             const std::vector<int64_t>& vector_ids) {
  int64_t index_id{0};
  DINGO_RETURN_NOT_OK(
      stub_.GetVectorIndexCache()->GetIndexIdByKey(EncodeVectorIndexCacheKey(schema_id, index_name), index_id));
  CHECK_GT(index_id, 0);
  VectorImportDeleteTask task(stub_, index_id, vector_ids);
  return task.Run();
}

// dump
Status VectorClient::DumpByIndexId(int64_t index_id) {
  VectorDumpTask task(stub_, index_id);
  return task.Run();
}

Status VectorClient::DumpByIndexName(int64_t schema_id, const std::string& index_name) {
  int64_t index_id{0};
  DINGO_RETURN_NOT_OK(
      stub_.GetVectorIndexCache()->GetIndexIdByKey(EncodeVectorIndexCacheKey(schema_id, index_name), index_id));
  CHECK_GT(index_id, 0);
  VectorDumpTask task(stub_, index_id);
  return task.Run();
}

}  // namespace sdk

}  // namespace dingodb