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

#include "sdk/vector/vector_task.h"

#include "common/logging.h"
#include "sdk/common/param_config.h"
#include "sdk/utils/async_util.h"
#include "sdk/common/common.h"

namespace dingodb {
namespace sdk {

Status VectorTask::Run() {
  Status ret;
  Synchronizer sync;
  AsyncRun(sync.AsStatusCallBack(ret));
  sync.Wait();
  return ret;
}

void VectorTask::AsyncRun(StatusCallback cb) {
  CHECK(cb) << "cb is invalid";
  {
    WriteLockGuard guard(rw_lock_);
    call_back_.swap(cb);
  }

  Status status = Init();
  if (status.ok()) {
    DoAsync();
  } else {
    status_ = status;
    FireCallback();
  }
}

Status VectorTask::Init() { return Status::OK(); }

std::string VectorTask::ErrorMsg() const { return ""; }

void VectorTask::DoAsyncDone(const Status& status) {
  status_ = status;
  if (status.ok()) {
    FireCallback();
  } else {
    FailOrRetry();
  }
}

void VectorTask::FailOrRetry() {
  if (NeedRetry()) {
    BackoffAndRetry();
  } else {
    FireCallback();
  }
}

bool VectorTask::NeedRetry() {
  if (status_.IsIncomplete()) {
    auto error_code = status_.Errno();
    if (IsRetryErrorCode(error_code)) {
      retry_count_++;
      if (retry_count_ < FLAGS_vector_op_max_retry) {
        std::string msg = fmt::format("Task:{} will retry, reason:{}, retry_count_:{}, max_retry:{}", Name(),
                                      pb::error::Errno_Name(error_code), retry_count_, FLAGS_vector_op_max_retry);
        DINGO_LOG(INFO) << msg;
        return true;
      } else {
        std::string msg =
            fmt::format("Fail task:{} retry too times:{}, last err:{}", Name(), retry_count_, status_.ToString());
        status_ = Status::Aborted(status_.Errno(), msg);
        DINGO_LOG(INFO) << msg;
      }
    }
  }

  return false;
}

void VectorTask::BackoffAndRetry() {
  auto delay = retry_count_ * FLAGS_vector_op_delay_ms;
  DINGO_LOG(INFO) << "Task:" << Name() << " will retry after " << delay << "ms";
  stub.GetActuator()->Schedule([this] { DoAsync(); }, delay);
}

void VectorTask::FireCallback() {
  PostProcess();

  if (!status_.ok()) {
    DINGO_LOG(WARNING) << "Fail task:" << Name() << ", status:" << status_.ToString() << ", error_msg:" << ErrorMsg();
  }

  StatusCallback cb;
  {
    ReadLockGuard guard(rw_lock_);
    CHECK(call_back_) << "call_back_ is invalid";
    call_back_.swap(cb);
  }

  cb(status_);
}

void VectorTask::PostProcess() {}

}  // namespace sdk
}  // namespace dingodb