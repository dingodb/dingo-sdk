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
#include "dingosdk/document.h"
#include "sdk/document/document_add_task.h"
#include "sdk/document/document_batch_query_task.h"
#include "sdk/document/document_count_task.h"
#include "sdk/document/document_delete_task.h"
#include "sdk/document/document_get_auto_increment_id_task.h"
#include "sdk/document/document_get_border_task.h"
#include "sdk/document/document_get_index_metrics_task.h"
#include "sdk/document/document_index_cache.h"
#include "sdk/document/document_scan_query_task.h"
#include "sdk/document/document_search_task.h"
#include "sdk/document/document_update_auto_increment_task.h"
#include "sdk/document/document_update_task.h"
#include "dingosdk/status.h"

namespace dingodb {
namespace sdk {

DocumentClient::DocumentClient(const ClientStub& stub) : stub_(stub) {}

Status DocumentClient::AddByIndexId(int64_t index_id, std::vector<DocWithId>& docs) {
  DocumentAddTask task(stub_, index_id, docs);
  return task.Run();
}

Status AddByIndexName(int64_t schema_id, const std::string& index_name, std::vector<DocWithId>& docs);
Status DocumentClient::AddByIndexName(int64_t schema_id, const std::string& index_name, std::vector<DocWithId>& docs) {
  int64_t index_id{0};
  DINGO_RETURN_NOT_OK(
      stub_.GetDocumentIndexCache()->GetIndexIdByKey(EncodeDocumentIndexCacheKey(schema_id, index_name), index_id));
  CHECK_GT(index_id, 0);
  DocumentAddTask task(stub_, index_id, docs);
  return task.Run();
}

Status DocumentClient::UpdateByIndexId(int64_t index_id, std::vector<DocWithId>& docs) {
  DocumentUpdateTask task(stub_, index_id, docs);
  return task.Run();
}

Status DocumentClient::UpdateByIndexName(int64_t schema_id, const std::string& index_name,
                                         std::vector<DocWithId>& docs) {
  int64_t index_id{0};
  DINGO_RETURN_NOT_OK(
      stub_.GetDocumentIndexCache()->GetIndexIdByKey(EncodeDocumentIndexCacheKey(schema_id, index_name), index_id));
  CHECK_GT(index_id, 0);

  DocumentUpdateTask task(stub_, index_id, docs);
  return task.Run();
}

Status DocumentClient::SearchByIndexId(int64_t index_id, const DocSearchParam& search_param,
                                       DocSearchResult& out_result) {
  DocumentSearchTask task(stub_, index_id, search_param, out_result);
  return task.Run();
}

Status DocumentClient::SearchByIndexName(int64_t schema_id, const std::string& index_name,
                                         const DocSearchParam& search_param, DocSearchResult& out_result) {
  int64_t index_id{0};
  DINGO_RETURN_NOT_OK(
      stub_.GetDocumentIndexCache()->GetIndexIdByKey(EncodeDocumentIndexCacheKey(schema_id, index_name), index_id));
  CHECK_GT(index_id, 0);
  DocumentSearchTask task(stub_, index_id, search_param, out_result);
  return task.Run();
}

Status DocumentClient::DeleteByIndexId(int64_t index_id, const std::vector<int64_t>& doc_ids,
                                       std::vector<DocDeleteResult>& out_result) {
  DocumentDeleteTask task(stub_, index_id, doc_ids, out_result);
  return task.Run();
}

Status DocumentClient::DeleteByIndexName(int64_t schema_id, const std::string& index_name,
                                         const std::vector<int64_t>& doc_ids,
                                         std::vector<DocDeleteResult>& out_result) {
  int64_t index_id{0};
  DINGO_RETURN_NOT_OK(
      stub_.GetDocumentIndexCache()->GetIndexIdByKey(EncodeDocumentIndexCacheKey(schema_id, index_name), index_id));
  CHECK_GT(index_id, 0);
  DocumentDeleteTask task(stub_, index_id, doc_ids, out_result);
  return task.Run();
}

Status DocumentClient::BatchQueryByIndexId(int64_t index_id, const DocQueryParam& query_param,
                                           DocQueryResult& out_result) {
  DocumentBatchQueryTask task(stub_, index_id, query_param, out_result);
  return task.Run();
}

Status DocumentClient::BatchQueryByIndexName(int64_t schema_id, const std::string& index_name,
                                             const DocQueryParam& query_param, DocQueryResult& out_result) {
  int64_t index_id{0};
  DINGO_RETURN_NOT_OK(
      stub_.GetDocumentIndexCache()->GetIndexIdByKey(EncodeDocumentIndexCacheKey(schema_id, index_name), index_id));
  DocumentBatchQueryTask task(stub_, index_id, query_param, out_result);
  return task.Run();
}

Status DocumentClient::GetBorderByIndexId(int64_t index_id, bool is_max, int64_t& out_doc_id) {
  DocumentGetBorderTask task(stub_, index_id, is_max, out_doc_id);
  return task.Run();
}

Status DocumentClient::GetBorderByIndexName(int64_t schema_id, const std::string& index_name, bool is_max,
                                            int64_t& out_doc_id) {
  int64_t index_id{0};
  DINGO_RETURN_NOT_OK(
      stub_.GetDocumentIndexCache()->GetIndexIdByKey(EncodeDocumentIndexCacheKey(schema_id, index_name), index_id));
  CHECK_GT(index_id, 0);

  DocumentGetBorderTask task(stub_, index_id, is_max, out_doc_id);
  return task.Run();
}

Status DocumentClient::ScanQueryByIndexId(int64_t index_id, const DocScanQueryParam& query_param,
                                          DocScanQueryResult& out_result) {
  DocumentScanQueryTask task(stub_, index_id, query_param, out_result);
  return task.Run();
}

Status DocumentClient::ScanQueryByIndexName(int64_t schema_id, const std::string& index_name,
                                            const DocScanQueryParam& query_param, DocScanQueryResult& out_result) {
  int64_t index_id{0};
  DINGO_RETURN_NOT_OK(
      stub_.GetDocumentIndexCache()->GetIndexIdByKey(EncodeDocumentIndexCacheKey(schema_id, index_name), index_id));
  CHECK_GT(index_id, 0);
  DocumentScanQueryTask task(stub_, index_id, query_param, out_result);
  return task.Run();
}

Status DocumentClient::GetIndexMetricsByIndexId(int64_t index_id, DocIndexMetricsResult& out_result) {
  DocumentGetIndexMetricsTask task(stub_, index_id, out_result);
  return task.Run();
}

Status DocumentClient::GetIndexMetricsByIndexName(int64_t schema_id, const std::string& index_name,
                                                  DocIndexMetricsResult& out_result) {
  int64_t index_id{0};
  DINGO_RETURN_NOT_OK(
      stub_.GetDocumentIndexCache()->GetIndexIdByKey(EncodeDocumentIndexCacheKey(schema_id, index_name), index_id));
  CHECK_GT(index_id, 0);
  DocumentGetIndexMetricsTask task(stub_, index_id, out_result);
  return task.Run();
}

Status DocumentClient::CountAllByIndexId(int64_t index_id, int64_t& out_count) {
  DocumentCountTask task(stub_, index_id, 0, INT64_MAX, out_count);
  return task.Run();
}

Status DocumentClient::CountallByIndexName(int64_t schema_id, const std::string& index_name, int64_t& out_count) {
  int64_t index_id{0};
  DINGO_RETURN_NOT_OK(
      stub_.GetDocumentIndexCache()->GetIndexIdByKey(EncodeDocumentIndexCacheKey(schema_id, index_name), index_id));
  CHECK_GT(index_id, 0);
  DocumentCountTask task(stub_, index_id, 0, INT64_MAX, out_count);
  return task.Run();
}

Status DocumentClient::CountByIndexId(int64_t index_id, int64_t start_doc_id, int64_t end_doc_id, int64_t& out_count) {
  DocumentCountTask task(stub_, index_id, start_doc_id, end_doc_id, out_count);
  return task.Run();
}

Status DocumentClient::CountByIndexName(int64_t schema_id, const std::string& index_name, int64_t start_doc_id,
                                        int64_t end_doc_id, int64_t& out_count) {
  int64_t index_id{0};
  DINGO_RETURN_NOT_OK(
      stub_.GetDocumentIndexCache()->GetIndexIdByKey(EncodeDocumentIndexCacheKey(schema_id, index_name), index_id));
  CHECK_GT(index_id, 0);
  DocumentCountTask task(stub_, index_id, start_doc_id, end_doc_id, out_count);
  return task.Run();
}

Status DocumentClient::GetAutoIncrementIdByIndexId(int64_t index_id, int64_t& start_id) {
  DocumentGetAutoIncrementIdTask task(stub_, index_id, start_id);
  return task.Run();
}
Status DocumentClient::GetAutoIncrementIdByIndexName(int64_t schema_id, const std::string& index_name,
                                                     int64_t& start_id) {
  int64_t index_id{0};
  DINGO_RETURN_NOT_OK(
      stub_.GetDocumentIndexCache()->GetIndexIdByKey(EncodeDocumentIndexCacheKey(schema_id, index_name), index_id));
  CHECK_GT(index_id, 0);
  DocumentGetAutoIncrementIdTask task(stub_, index_id, start_id);
  return task.Run();
}

Status DocumentClient::UpdateAutoIncrementIdByIndexId(int64_t index_id, int64_t start_id) {
  DocumentUpdateAutoIncrementTask task(stub_, index_id, start_id);
  return task.Run();
}

Status DocumentClient::UpdateAutoIncrementIdByIndexName(int64_t schema_id, const std::string& index_name,
                                                        int64_t& start_id) {
  int64_t index_id{0};
  DINGO_RETURN_NOT_OK(
      stub_.GetDocumentIndexCache()->GetIndexIdByKey(EncodeDocumentIndexCacheKey(schema_id, index_name), index_id));
  CHECK_GT(index_id, 0);
  DocumentUpdateAutoIncrementTask task(stub_, index_id, start_id);
  return task.Run();
}

}  // namespace sdk

}  // namespace dingodb