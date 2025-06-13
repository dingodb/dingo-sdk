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

#ifndef TXN_BATCH_GET_TASK_H
#define TXN_BATCH_GET_TASK_H

#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "dingosdk/client.h"
#include "sdk/client_stub.h"
#include "sdk/rpc/brpc/store_rpc.h"
#include "sdk/rpc/store_rpc_controller.h"
#include "sdk/transaction/txn_impl.h"
#include "sdk/transaction/txn_task/txn_task.h"
#include "sdk/utils/rw_lock.h"

namespace dingodb {
namespace sdk {

class TxnBatchGetTask : public TxnTask {
 public:
  TxnBatchGetTask(const ClientStub& stub, const std::vector<std::string>& key, std::vector<KVPair>& out_kvs,
                  std::shared_ptr<TxnImpl> txn_impl)
      : TxnTask(stub), keys_(key), out_kvs_(out_kvs), txn_impl_(txn_impl) {}

  ~TxnBatchGetTask() override = default;

 private:
  Status Init() override;

  void DoAsync() override;

  std::string Name() const override { return "TxnBatchGetTask"; }

  void TxnBatchGetRpcCallback(const Status& status, TxnBatchGetRpc* rpc);

  const std::vector<std::string>& keys_;
  std::vector<KVPair>& out_kvs_;

  std::set<std::string_view> next_keys_;
  std::shared_ptr<TxnImpl> txn_impl_;

  std::unordered_map<int64_t, uint64_t> region_id_to_resolved_lock_;

  std::vector<StoreRpcController> controllers_;
  std::vector<std::unique_ptr<TxnBatchGetRpc>> rpcs_;
  bool need_retry_{false};

  RWLock rw_lock_;
  Status status_;

  std::atomic<int> sub_tasks_count_{0};
};

}  // namespace sdk

}  // namespace dingodb

#endif  // DINGODB_SDK_TRANSACTION_BUFFER_H_