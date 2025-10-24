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

#include "sdk/transaction/txn_manager.h"

#include <glog/logging.h>

#include <memory>
#include <mutex>
#include <utility>

#include "common/logging.h"
#include "dingosdk/client.h"
#include "dingosdk/status.h"
#include "sdk/client_stub.h"
#include "sdk/transaction/txn_impl.h"
#include "sdk/transaction/txn_internal_data.h"

namespace dingodb {
namespace sdk {

TxnManager::~TxnManager() {
  DINGO_LOG(INFO) << "TxnManager destructor start";
  // stop, forbid add new txns
  Stop();

  DINGO_LOG(INFO) << "TxnManager destructor end";
}

Status TxnManager::RegisterTxn(std::shared_ptr<TxnImpl> txn_impl) {
  if (IsStopped()) {
    DINGO_LOG(WARNING) << fmt::format("TxnManager is stopped, refuse new txn");
    return Status::Aborted("TxnManager is stopped");
  }

  int64_t txn_id = txn_impl->ID();
  {
    std::lock_guard<std::mutex> lock(mutex_);
    CHECK(active_txns_.find(txn_id) == active_txns_.end()) << "txn already exists, txn id: " << txn_id;
    CHECK(active_txns_.emplace(txn_id, std::move(txn_impl)).second) << "failed to emplace txn, txn id: " << txn_id;
  }

  DINGO_LOG(DEBUG) << fmt::format("Register txn: {}, active txns: {}", txn_id, active_txns_.size());
  return Status::OK();
}

void TxnManager::UnregisterTxn(int64_t txn_id) {
  std::lock_guard<std::mutex> lock(mutex_);

  auto it = active_txns_.find(txn_id);
  if (it != active_txns_.end()) {
    active_txns_.erase(it);
    DINGO_LOG(DEBUG) << fmt::format("Unregister txn: {}, active txns: {}", txn_id, active_txns_.size());
  } else {
    DINGO_LOG(WARNING) << fmt::format("Txn not found for unregister: {}", txn_id);
  }

  if (active_txns_.empty()) {
    cv_.notify_all();
  }
}

void TxnManager::WaitAllTxnsComplete() {
  std::unique_lock<std::mutex> lock(mutex_);

  if (active_txns_.empty()) {
    DINGO_LOG(INFO) << "No active txns, return immediately";
    return;
  }

  DINGO_LOG(INFO) << "Waiting for all txns to complete, active txns: " << active_txns_.size();

  cv_.wait(lock, [this] { return active_txns_.empty(); });

  DINGO_LOG(INFO) << "All txns completed";
}

void TxnManager::CheckTxnState() {
  std::lock_guard<std::mutex> lock(mutex_);

  for (auto it = active_txns_.begin(); it != active_txns_.end();) {
    auto txn = it->second;
    CHECK(txn != nullptr) << "txn is nullptr";
    CHECK(txn->CheckFinished()) << "txn state is not finished, " << txn->DebugString();
    ++it;
  }
}

void TxnManager::Stop() {
  bool expected = false;
  if (stopped_.compare_exchange_strong(expected, true)) {
    DINGO_LOG(INFO) << "TxnManager stopped, no more txns will be accepted";
  }
  CheckTxnState();
  WaitAllTxnsComplete();
}

size_t TxnManager::GetActiveTxnCount() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return active_txns_.size();
}

}  // namespace sdk
}  // namespace dingodb