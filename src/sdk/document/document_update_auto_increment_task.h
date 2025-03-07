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

#ifndef DINGODB_SDK_DOCUMENT_UPDATE_AUTO_INCREMENT_TASK_H_
#define DINGODB_SDK_DOCUMENT_UPDATE_AUTO_INCREMENT_TASK_H_

#include <cstdint>
#include <memory>

#include "sdk/client_stub.h"
#include "sdk/document/document_index.h"
#include "sdk/document/document_task.h"

namespace dingodb {
namespace sdk {

class DocumentUpdateAutoIncrementTask : public DocumentTask {
 public:
  DocumentUpdateAutoIncrementTask(const ClientStub& stub, int64_t index_id, int64_t start_id)
      : DocumentTask(stub), index_id_(index_id), start_id_(start_id) {}

  ~DocumentUpdateAutoIncrementTask() override = default;

 private:
  Status Init() override;
  void DoAsync() override;

  std::string Name() const override { return fmt::format("DocumentUpdateAutoIncrementTask-{}", index_id_); }

  const int64_t index_id_;
  const int64_t start_id_;

  std::shared_ptr<DocumentIndex> document_index_;

  Status status_;
};

}  // namespace sdk
}  // namespace dingodb

#endif  // DINGODB_SDK_VECTOR_GET_AUTO_INCREMENT_ID_TASK_H_