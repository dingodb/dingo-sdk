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

#ifndef DINGODB_SDK_TXN_PREWRITE_TASK_H_
#define DINGODB_SDK_TXN_PREWRITE_TASK_H_

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "dingosdk/status.h"
#include "sdk/client_stub.h"
#include "sdk/rpc/store_rpc_controller.h"
#include "sdk/transaction/txn_buffer.h"
#include "sdk/transaction/txn_impl.h"
#include "sdk/transaction/txn_task/txn_task.h"
#include "sdk/utils/rw_lock.h"

namespace dingodb {
namespace sdk {

class TxnPrewriteTask : public TxnTask {
 public:
  TxnPrewriteTask(const ClientStub& stub, const std::string primary_key,
                  const std::map<std::string, const TxnMutation*>& mutations, std::shared_ptr<TxnImpl> txn_impl,
                  const std::map<std::string, const TxnMutation*>& mutations_ordinarykeys_for_async_commit,
                  bool& is_one_pc, bool& use_async_commit, uint64_t& min_commit_ts)
      : TxnTask(stub),
        primary_key_(primary_key),
        mutations_(mutations),
        txn_impl_(txn_impl),
        is_one_pc_(is_one_pc),
        use_async_commit_(use_async_commit),
        mutations_ordinarykeys_for_async_commit_(mutations_ordinarykeys_for_async_commit),
        min_commit_ts_(min_commit_ts) {}

  ~TxnPrewriteTask() override = default;

 private:
  Status Init() override;

  void DoAsync() override;

  std::string Name() const override { return "TxnPrewriteTask"; }

  void TxnPrewriteRpcCallback(const Status& status, TxnPrewriteRpc* rpc);

  void BackoffAndRetry() override;
  bool IsRetryError() override;
  bool NeedRetry() override;

  const std::string primary_key_;
  const std::map<std::string, const TxnMutation*>& mutations_;
  bool& is_one_pc_;

  // for async commit
  bool& use_async_commit_;
  const std::map<std::string, const TxnMutation*>& mutations_ordinarykeys_for_async_commit_;
  uint64_t& min_commit_ts_;

  std::vector<StoreRpcController> controllers_;
  std::vector<std::unique_ptr<TxnPrewriteRpc>> rpcs_;
  std::shared_ptr<TxnImpl> txn_impl_;

  bool need_retry_{false};

  std::map<std::string, const TxnMutation*> next_mutations_;

  std::atomic<int> sub_tasks_count_{0};
  RWLock rw_lock_;
  Status status_;
  int retry_count_{0};
};

}  // namespace sdk
}  // namespace dingodb

#endif  // DINGODB_SDK_TXN_PREWRITE_TASK_H_