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

#include "sdk/transaction/txn_lock_resolver.h"

#include <fmt/format.h>

#include <cstdint>

#include "common/logging.h"
#include "dingosdk/status.h"
#include "glog/logging.h"
#include "sdk/client_stub.h"
#include "sdk/transaction/txn_task/txn_check_status_task.h"
#include "sdk/transaction/txn_task/txn_resolve_lock_task.h"

namespace dingodb {
namespace sdk {

TxnLockResolver::TxnLockResolver(const ClientStub& stub) : stub_(stub) {}

// TODO: maybe support retry
Status TxnLockResolver::ResolveLock(const pb::store::LockInfo& lock_info, int64_t start_ts) {
  DINGO_LOG(INFO) << fmt::format("[sdk.txn.{}] resolve lock, lock_info({}).", start_ts, lock_info.ShortDebugString());

  // check primary key lock status
  TxnStatus txn_status;
  TxnCheckStatusTask task_check_status(stub_, lock_info.lock_ts(), lock_info.primary_lock(), start_ts, txn_status);
  Status status = task_check_status.Run();
  if (!status.ok()) {
    if (status.IsNotFound()) {
      DINGO_LOG(DEBUG) << fmt::format("[sdk.txn.{}] not exist txn when check status, status({}) lock({}).", start_ts,
                                      status.ToString(), lock_info.ShortDebugString());

      return Status::OK();
    } else {
      return status;
    }
  }

  if (txn_status.IsMinCommitTSPushed()) {
    return Status::PushMinCommitTs("push min_commit_ts");
  }

  // primary key exist lock then outer txn rollback
  if (txn_status.IsLocked()) {
    return Status::TxnLockConflict(status.ToString());
  }

  CHECK(txn_status.IsCommitted() || txn_status.IsRollbacked()) << "unexpected txn_status:" << txn_status.ToString();

  // resolve conflict ordinary key
  if (lock_info.primary_lock() == lock_info.key()) {
    // if key is primary key, already committed or rollbacked, do nothing
    if (txn_status.IsCommitted()) {
      DINGO_LOG(INFO) << fmt::format("[sdk.txn.{}] primary key already committed, lock_info({}).", start_ts,
                                     lock_info.ShortDebugString());
    } else if (txn_status.IsRollbacked()) {
      DINGO_LOG(INFO) << fmt::format("[sdk.txn.{}] primary key already rollbacked, lock_info({}).", start_ts,
                                     lock_info.ShortDebugString());
    }
    return Status::OK();
  }
  TxnResolveLockTask task_resolve_lock(stub_, lock_info.lock_ts(), lock_info.key(), txn_status.commit_ts);
  status = task_resolve_lock.Run();
  if (!status.IsOK()) {
    DINGO_LOG(WARNING) << fmt::format("[sdk.txn.{}] resolve lock fail, lock_ts({}) key({}) txn_status({}) status({}).",
                                      start_ts, lock_info.lock_ts(), lock_info.key(), txn_status.ToString(),
                                      status.ToString());
    return status;
  }

  return Status::OK();
}

}  // namespace sdk
}  // namespace dingodb