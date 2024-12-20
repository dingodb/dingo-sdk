
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

#ifndef DINGODB_SDK_DOCUMENT_GET_INDEX_METRICS_TASK_H_
#define DINGODB_SDK_DOCUMENT_GET_INDEX_METRICS_TASK_H_

#include <cstdint>
#include <unordered_map>

#include "fmt/core.h"
#include "sdk/client_stub.h"
#include "dingosdk/document.h"
#include "sdk/document/document_task.h"
#include "sdk/rpc/document_service_rpc.h"
#include "sdk/rpc/store_rpc_controller.h"

namespace dingodb {
namespace sdk {

static void MergeDocIndexMetricsResult(const DocIndexMetricsResult& from, DocIndexMetricsResult& to) {
  to.total_num_docs += from.total_num_docs;
  to.total_num_tokens += from.total_num_tokens;
  to.max_doc_id = std::max(to.max_doc_id, from.max_doc_id);
  if (from.min_doc_id != 0) {
    to.min_doc_id = std::min(to.min_doc_id, from.min_doc_id);
  }
}

static DocIndexMetricsResult CreateDocIndexMetricsResult() {
  DocIndexMetricsResult to_return;
  to_return.min_doc_id = INT64_MAX;
  to_return.max_doc_id = INT64_MIN;
  return to_return;
}

class DocumentGetIndexMetricsPartTask;
class DocumentGetIndexMetricsTask : public DocumentTask {
 public:
  DocumentGetIndexMetricsTask(const ClientStub& stub, int64_t index_id, DocIndexMetricsResult& out_result)
      : DocumentTask(stub), index_id_(index_id), out_result_(out_result), tmp_result_(CreateDocIndexMetricsResult()) {}

  ~DocumentGetIndexMetricsTask() override = default;

 private:
  Status Init() override;
  void DoAsync() override;

  std::string Name() const override { return fmt::format("DocumentGetIndexMetricsTask-{}", index_id_); }

  void SubTaskCallback(Status status, DocumentGetIndexMetricsPartTask* sub_task);

  const int64_t index_id_;
  DocIndexMetricsResult& out_result_;

  std::shared_ptr<DocumentIndex> doc_index_;

  std::shared_mutex rw_lock_;
  std::set<int64_t> next_part_ids_;
  Status status_;
  DocIndexMetricsResult tmp_result_;

  std::atomic<int> sub_tasks_count_{0};
};

class DocumentGetIndexMetricsPartTask : public DocumentTask {
 public:
  DocumentGetIndexMetricsPartTask(const ClientStub& stub, std::shared_ptr<DocumentIndex> vector_index, int64_t part_id)
      : DocumentTask(stub),
        doc_index_(std::move(vector_index)),
        part_id_(part_id),
        total_metrics_(CreateDocIndexMetricsResult()) {}

  ~DocumentGetIndexMetricsPartTask() override = default;

  DocIndexMetricsResult GetResult() {
    std::shared_lock<std::shared_mutex> r(rw_lock_);

    return total_metrics_;
  }

 private:
  friend class DocumentGetIndexMetricsTask;

  void DoAsync() override;

  std::string Name() const override {
    return fmt::format("DocumentGetIndexMetricsPartTask-{}-{}", doc_index_->GetId(), part_id_);
  }

  void DocumentGetRegionMetricsRpcCallback(const Status& status, DocumentGetRegionMetricsRpc* rpc);

  const std::shared_ptr<DocumentIndex> doc_index_;
  const int64_t part_id_;

  std::vector<StoreRpcController> controllers_;
  std::vector<std::unique_ptr<DocumentGetRegionMetricsRpc>> rpcs_;

  std::shared_mutex rw_lock_;
  Status status_;
  std::unordered_map<int64_t, pb::common::DocumentIndexMetrics> region_id_to_metrics_;

  DocIndexMetricsResult total_metrics_;

  std::atomic<int> sub_tasks_count_{0};
};

}  // namespace sdk
}  // namespace dingodb

#endif  // DINGODB_SDK_DOCUMENT_GET_INDEX_METRICS_TASK_H_
