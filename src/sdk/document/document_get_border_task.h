
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

#ifndef DINGODB_SDK_DOCUMENT_GET_BORDER_TASK_H_
#define DINGODB_SDK_DOCUMENT_GET_BORDER_TASK_H_

#include <cstdint>

#include "dingosdk/document.h"
#include "sdk/document/document_task.h"
#include "sdk/rpc/document_service_rpc.h"
#include "sdk/rpc/store_rpc_controller.h"
#include "sdk/utils/rw_lock.h"

namespace dingodb {
namespace sdk {

class DocumentGetBorderPartTask;
class DocumentGetBorderTask : public DocumentTask {
 public:
  DocumentGetBorderTask(const ClientStub& stub, int64_t index_id, bool is_max, int64_t& out_doc_id)
      : DocumentTask(stub), index_id_(index_id), is_max_(is_max), out_doc_id_(out_doc_id) {
    target_doc_id_ = is_max_ ? -1 : INT64_MAX;
  }

  ~DocumentGetBorderTask() override = default;

 private:
  Status Init() override;
  void DoAsync() override;

  std::string Name() const override { return fmt::format("DocumentGetBorderTask-{}", index_id_); }

  void SubTaskCallback(Status status, DocumentGetBorderPartTask* sub_task);

  const int64_t index_id_;
  const bool is_max_;
  int64_t& out_doc_id_;
  std::shared_ptr<DocumentIndex> vector_index_;
  int64_t target_doc_id_;

  RWLock rw_lock_;
  std::set<int64_t> next_part_ids_;
  Status status_;

  std::atomic<int> sub_tasks_count_{0};
};

class DocumentGetBorderPartTask : public DocumentTask {
 public:
  DocumentGetBorderPartTask(const ClientStub& stub, std::shared_ptr<DocumentIndex> vector_index, int64_t part_id,
                            bool is_max)
      : DocumentTask(stub), vector_index_(std::move(vector_index)), part_id_(part_id), is_max_(is_max) {
    result_doc_id_ = is_max_ ? -1 : INT64_MAX;
  }

  ~DocumentGetBorderPartTask() override = default;

  int64_t GetResult() {
    ReadLockGuard guard(rw_lock_);
    return result_doc_id_;
  }

 private:
  friend class DocumentGetBorderTask;

  void DoAsync() override;

  std::string Name() const override {
    return fmt::format("DocumentGetBorderPartTask-{}-{}", vector_index_->GetId(), part_id_);
  }

  void DocumentGetBorderIdRpcCallback(const Status& status, DocumentGetBorderIdRpc* rpc);

  const std::shared_ptr<DocumentIndex> vector_index_;
  const int64_t part_id_;
  const bool is_max_;

  std::vector<StoreRpcController> controllers_;
  std::vector<std::unique_ptr<DocumentGetBorderIdRpc>> rpcs_;

  RWLock rw_lock_;
  Status status_;
  int64_t result_doc_id_;

  std::atomic<int> sub_tasks_count_{0};
};

}  // namespace sdk
}  // namespace dingodb

#endif  // DINGODB_SDK_DOCUMENT_GET_BORDER_TASK_H_