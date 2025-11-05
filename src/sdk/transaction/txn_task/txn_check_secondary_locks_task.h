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

#ifndef TXN_CHECK_SECONDARY_LOCKS_TASK_H
#define TXN_CHECK_SECONDARY_LOCKS_TASK_H

#include <atomic>
#include <cstdint>
#include <string>
#include <vector>

#include "sdk/client_stub.h"
#include "sdk/rpc/store_rpc.h"
#include "sdk/rpc/store_rpc_controller.h"
#include "sdk/transaction/txn_lock_resolver.h"
#include "sdk/transaction/txn_task/txn_task.h"
#include "sdk/utils/rw_lock.h"

namespace dingodb {
namespace sdk {

class TxnCheckSecondaryLocksTask : public TxnTask {
 public:
  TxnCheckSecondaryLocksTask(const ClientStub& stub, const std::vector<std::string> secondary_keys,
                             int64_t primary_lock_start_ts, TxnSecondaryLockStatus& txn_check_secondary_status)
      : TxnTask(stub),
        secondary_keys_(secondary_keys),
        primary_lock_start_ts_(primary_lock_start_ts),
        txn_check_secondary_status_(txn_check_secondary_status) {}

  ~TxnCheckSecondaryLocksTask() override = default;

 private:
  Status Init() override;

  void DoAsync() override;

  std::string Name() const override { return "TxnCheckSecondaryLocksTask"; }

  void TxnCheckSecondaryLocksRpcCallback(const Status& status, TxnCheckSecondaryLocksRpc* rpc);

  std::vector<std::string> secondary_keys_;
  TxnSecondaryLockStatus& txn_check_secondary_status_;
  std::set<std::string_view> next_keys_;
  int64_t primary_lock_start_ts_{0};

  std::vector<StoreRpcController> controllers_;
  std::vector<std::unique_ptr<TxnCheckSecondaryLocksRpc>> rpcs_;
  RWLock rw_lock_;
  Status status_;
  std::atomic<int> sub_tasks_count_{0};
};

}  // namespace sdk

}  // namespace dingodb

#endif  // TXN_CHECK_SECONDARY_LOCKS_TASK_H