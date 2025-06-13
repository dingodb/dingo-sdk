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

#ifndef DINGODB_SDK_TXN_BATCH_ROLLBACK_TASK_H_
#define DINGODB_SDK_TXN_BATCH_ROLLBACK_TASK_H_

#include <memory>
#include <set>
#include <string>
#include <string_view>
#include <vector>

#include "dingosdk/status.h"
#include "sdk/client_stub.h"
#include "sdk/rpc/store_rpc_controller.h"
#include "sdk/transaction/txn_impl.h"
#include "sdk/transaction/txn_task/txn_task.h"
#include "sdk/utils/rw_lock.h"

namespace dingodb {
namespace sdk {

class TxnBatchRollbackTask : public TxnTask {
 public:
  TxnBatchRollbackTask(const ClientStub& stub, const std::vector<std::string>& keys, std::shared_ptr<TxnImpl> txn_impl,
                       bool is_one_pc = false)
      : TxnTask(stub), keys_(keys), txn_impl_(txn_impl), is_one_pc_(is_one_pc) {}

  ~TxnBatchRollbackTask() override = default;

 private:
  Status Init() override;

  void DoAsync() override;

  std::string Name() const override { return "TxnBatchRollbackTask"; }

  void TxnBatchRollbackRpcCallback(const Status& status, TxnBatchRollbackRpc* rpc);

  const std::vector<std::string>& keys_;
  std::shared_ptr<TxnImpl> txn_impl_;
  bool is_one_pc_;

  std::vector<StoreRpcController> controllers_;
  std::vector<std::unique_ptr<TxnBatchRollbackRpc>> rpcs_;
  std::set<std::string_view> next_keys_;
  std::atomic<int> sub_tasks_count_{0};
  
  RWLock rw_lock_;
  Status status_;
};

}  // namespace sdk
}  // namespace dingodb

#endif  // DINGODB_SDK_TXN_BATCH_ROLLBACK_TASK_H_