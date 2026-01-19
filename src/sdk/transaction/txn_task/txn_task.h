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

#ifndef DINGODB_SDK_TXN_TASK_H_
#define DINGODB_SDK_TXN_TASK_H_

#include "dingosdk/status.h"
#include "sdk/client_stub.h"
#include "sdk/utils/callback.h"
#include "sdk/utils/rw_lock.h"
#include "sdk/utils/scoped_cleanup.h"

namespace dingodb {
namespace sdk {

class TxnTask {
 public:
  TxnTask(const ClientStub& stub) : stub(stub) {}
  virtual ~TxnTask() = default;

  Status Run();
  void AsyncRun(StatusCallback cb);

 protected:
  virtual Status Init();
  virtual void PostProcess();
  virtual void DoAsync() = 0;
  virtual std::string ErrorMsg() const;
  virtual std::string Name() const = 0;

  // task must call this when complete DoAsync
  void DoAsyncDone(const Status& status);

  void DoAsyncRetry();

  const ClientStub& stub;

  // prewrite requires special processing
  virtual void BackoffAndRetry();
  virtual bool IsRetryError();
  virtual bool NeedRetry();

 private:
  void FailOrRetry();
  void FireCallback();

  Status status_;
  RWLock rw_lock_;
  StatusCallback call_back_;
  int retry_count_{0};
};

}  // namespace sdk
}  // namespace dingodb

#endif  // DINGODB_SDK_TXN_TASK_H_