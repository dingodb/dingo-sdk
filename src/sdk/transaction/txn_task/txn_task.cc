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

#include "sdk/transaction/txn_task/txn_task.h"

#include "dingosdk/status.h"
#include "proto/error.pb.h"
#include "sdk/common/common.h"
#include "sdk/common/param_config.h"
#include "sdk/utils/async_util.h"

namespace dingodb {
namespace sdk {
Status TxnTask::Run() {
  Status ret;
  Synchronizer sync;
  AsyncRun(sync.AsStatusCallBack(ret));
  sync.Wait();
  return ret;
}

void TxnTask::AsyncRun(StatusCallback cb) {
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

Status TxnTask::Init() { return Status::OK(); }

std::string TxnTask::ErrorMsg() const { return ""; }

void TxnTask::DoAsyncDone(const Status& status) {
  status_ = status;
  if (status.ok()) {
    FireCallback();
  } else {
    FailOrRetry();
  }
}

void TxnTask::FailOrRetry() {
  if (NeedRetry()) {
    BackoffAndRetry();
  } else {
    FireCallback();
  }
}

bool TxnTask::IsRetryError() { return (status_.IsIncomplete() && IsRetryErrorCode(status_.Errno())); }

bool TxnTask::NeedRetry() {
  if (IsRetryError()) {
    retry_count_++;
    if (retry_count_ < FLAGS_txn_op_max_retry) {
      return true;
    } else {
      std::string msg =
          fmt::format("Fail task:{} retry too times:{}, last err:{}", Name(), retry_count_, status_.ToString());
      status_ = Status::Aborted(status_.Errno(), msg);
    }
  }

  return false;
}

void TxnTask::DoAsyncRetry() {
  retry_count_++;
  if (retry_count_ < FLAGS_txn_op_max_retry) {
    BackoffAndRetry();
  } else {
    std::string msg = fmt::format("Fail task:{} retry too times:{}, last op : txn resolve lock", Name(), retry_count_);
    status_ = Status::Aborted(status_.Errno(), msg);
    FireCallback();
  }
}

void TxnTask::BackoffAndRetry() {
  stub.GetActuator()->Schedule([this] { DoAsync(); }, FLAGS_txn_op_delay_ms);
}

void TxnTask::FireCallback() {
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

void TxnTask::PostProcess() {}

}  // namespace sdk

}  // namespace dingodb