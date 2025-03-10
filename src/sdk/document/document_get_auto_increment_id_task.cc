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

#include "sdk/document/document_get_auto_increment_id_task.h"

#include "dingosdk/status.h"
#include "glog/logging.h"
#include "sdk/auto_increment_manager.h"
#include "sdk/document/document_index.h"

namespace dingodb {
namespace sdk {

Status DocumentGetAutoIncrementIdTask::Init() {
  std::shared_ptr<DocumentIndex> tmp;
  DINGO_RETURN_NOT_OK(stub.GetDocumentIndexCache()->GetDocumentIndexById(index_id_, tmp));
  DCHECK_NOTNULL(tmp);
  document_index_ = std::move(tmp);

  if (document_index_->HasAutoIncrement()) {
    auto incrementer = stub.GetAutoIncrementerManager()->GetOrCreateDocumentIndexIncrementer(document_index_);
    DINGO_RETURN_NOT_OK(incrementer->GetAutoIncrementId(start_id_));
  } else {
    return Status::InvalidArgument("document index not support auto increment");
  }

  return Status::OK();
}

void DocumentGetAutoIncrementIdTask::DoAsync() { DoAsyncDone(Status::OK()); }

}  // namespace sdk
}  // namespace dingodb