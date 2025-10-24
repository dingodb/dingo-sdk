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

#ifndef DINGODB_SDK_TRANSACTION_TXN_MANAGER_H_
#define DINGODB_SDK_TRANSACTION_TXN_MANAGER_H_

#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <unordered_map>

#include "dingosdk/client.h"
#include "dingosdk/status.h"
namespace dingodb {
namespace sdk {

// Forward declaration to avoid circular include
class ClientStub;

class TxnImpl;

class TxnManager : public std::enable_shared_from_this<TxnManager> {
 public:
  TxnManager() = default;
  ~TxnManager();

  Status RegisterTxn(std::shared_ptr<TxnImpl> txn_impl);

  void UnregisterTxn(int64_t txn_id);

  size_t GetActiveTxnCount() const;
  void Stop();

 private:
  void CheckTxnState();
  void WaitAllTxnsComplete();

  bool IsStopped() const { return stopped_.load(); }

  mutable std::mutex mutex_;
  std::condition_variable cv_;

  std::unordered_map<int64_t, std::shared_ptr<TxnImpl>> active_txns_;
  std::atomic<bool> stopped_{false};
};

}  // namespace sdk
}  // namespace dingodb

#endif  // DINGODB_SDK_TRANSACTION_TXN_MANAGER_H_