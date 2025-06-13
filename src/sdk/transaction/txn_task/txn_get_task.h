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

#ifndef DINGODB_SDK_TXN_GET_TASK_H_
#define DINGODB_SDK_TXN_GET_TASK_H_

#include <cstdint>
#include <memory>
#include <string>

#include "sdk/client_stub.h"
#include "sdk/rpc/store_rpc_controller.h"
#include "sdk/transaction/txn_impl.h"
#include "sdk/transaction/txn_task/txn_task.h"
#include "sdk/utils/rw_lock.h"

namespace dingodb {
namespace sdk {

class TxnGetTask : public TxnTask {
 public:
  TxnGetTask(const ClientStub& stub, const std::string& key, std::string& value, std::shared_ptr<TxnImpl> txn_impl)
      : TxnTask(stub), key_(key), value_(value), txn_impl_(txn_impl), store_rpc_controller_(stub, rpc_) {}

  ~TxnGetTask() override = default;

 private:
  void DoAsync() override;

  std::string Name() const override { return "TxnGetTask"; }

  void TxnGetRpcCallback(const Status& status);

  const std::string& key_;
  std::string& value_;

  std::shared_ptr<TxnImpl> txn_impl_;
  StoreRpcController store_rpc_controller_;
  TxnGetRpc rpc_;
  uint64_t resolved_lock_{0};

  RWLock rw_lock_;
  Status status_;
};

}  // namespace sdk

}  // namespace dingodb

#endif  // DINGODB_SDK_TXN_GET_TASK_H_