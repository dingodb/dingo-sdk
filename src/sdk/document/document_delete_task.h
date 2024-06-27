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

#ifndef DINGODB_SDK_DOCUMENT_DELETE_TASK_H_
#define DINGODB_SDK_DOCUMENT_DELETE_TASK_H_

#include <cstdint>

#include "sdk/document/document_task.h"
#include "sdk/rpc/document_service_rpc.h"
#include "sdk/rpc/store_rpc_controller.h"

namespace dingodb {
namespace sdk {

class DocumentDeleteTask : public DocumentTask {
 public:
  DocumentDeleteTask(const ClientStub& stub, int64_t index_id, const std::vector<int64_t>& doc_ids,
                     std::vector<DocDeleteResult>& out_result)
      : DocumentTask(stub), index_id_(index_id), doc_ids_(doc_ids), out_result_(out_result) {}

  ~DocumentDeleteTask() override = default;

 private:
  Status Init() override;
  void DoAsync() override;

  std::string Name() const override { return fmt::format("DocumentDeleteTask-{}", index_id_); }

  void DocumentDeleteRpcCallback(const Status& status, DocumentDeleteRpc* rpc);

  const int64_t index_id_;
  const std::vector<int64_t>& doc_ids_;

  std::vector<DocDeleteResult>& out_result_;

  std::shared_ptr<DocumentIndex> doc_index_;

  std::vector<StoreRpcController> controllers_;
  std::vector<std::unique_ptr<DocumentDeleteRpc>> rpcs_;

  std::shared_mutex rw_lock_;
  std::set<int64_t> next_doc_ids_;
  Status status_;

  std::atomic<int> sub_tasks_count_{0};
};

}  // namespace sdk
}  // namespace dingodb
#endif  // DINGODB_SDK_DOCUMENT_DELETE_TASK_H_