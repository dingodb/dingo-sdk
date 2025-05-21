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

#ifndef DINGODB_SDK_DOCUMENT_ADD_TASK_H_
#define DINGODB_SDK_DOCUMENT_ADD_TASK_H_

#include <cstdint>
#include <memory>
#include <unordered_map>

#include "sdk/client_stub.h"
#include "sdk/document/document_index.h"
#include "sdk/document/document_task.h"
#include "sdk/rpc/document_service_rpc.h"
#include "sdk/rpc/store_rpc_controller.h"
#include "sdk/utils/rw_lock.h"

namespace dingodb {
namespace sdk {

class DocumentAddTask : public DocumentTask {
 public:
  DocumentAddTask(const ClientStub& stub, int64_t index_id, std::vector<DocWithId>& docs)
      : DocumentTask(stub), index_id_(index_id), docs_(docs) {}

  ~DocumentAddTask() override = default;

 private:
  Status Init() override;
  void DoAsync() override;

  std::string Name() const override { return fmt::format("DocumentAddTask-{}", index_id_); }

  void DocumentAddRpcCallback(const Status& status, DocumentAddRpc* rpc);

  const int64_t index_id_;
  std::vector<DocWithId>& docs_;

  std::shared_ptr<DocumentIndex> doc_index_;

  std::vector<StoreRpcController> controllers_;
  std::vector<std::unique_ptr<DocumentAddRpc>> rpcs_;

  RWLock rw_lock_;
  std::unordered_map<int64_t, int64_t> doc_id_to_idx_;
  Status status_;

  std::atomic<int> sub_tasks_count_{0};
};

}  // namespace sdk
}  // namespace dingodb

#endif  // DINGODB_SDK_DOCUMENT_ADD_TASK_H_