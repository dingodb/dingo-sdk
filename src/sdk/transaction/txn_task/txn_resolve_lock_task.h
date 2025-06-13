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

#ifndef TXN_RESOLVE_LOCK_TASK_H
#define TXN_RESOLVE_LOCK_TASK_H

#include <cstdint>
#include <string>

#include "sdk/client_stub.h"
#include "sdk/rpc/brpc/store_rpc.h"
#include "sdk/rpc/store_rpc_controller.h"
#include "sdk/transaction/txn_lock_resolver.h"
#include "sdk/transaction/txn_task/txn_task.h"
#include "sdk/utils/rw_lock.h"

namespace dingodb {
namespace sdk {

class TxnResolveLockTask : public TxnTask {
 public:
  TxnResolveLockTask(const ClientStub& stub, int64_t lock_ts, const std::string& key, int64_t commit_ts)
      : TxnTask(stub), lock_ts_(lock_ts), key_(key), commit_ts_(commit_ts), store_rpc_controller_(stub, rpc_) {}

  ~TxnResolveLockTask() override = default;

 private:
  void DoAsync() override;

  std::string Name() const override { return "TxnResolveLockTask"; }

  void TxnResolveLockRpcCallback(const Status& status);

  int64_t lock_ts_{0};
  const std::string& key_;
  int64_t commit_ts_{0};

  StoreRpcController store_rpc_controller_;
  TxnResolveLockRpc rpc_;
  uint64_t resolved_lock_{0};

  RWLock rw_lock_;
  Status status_;
};

}  // namespace sdk

}  // namespace dingodb

#endif  // TXN_RESOLVE_LOCK_TASK_H