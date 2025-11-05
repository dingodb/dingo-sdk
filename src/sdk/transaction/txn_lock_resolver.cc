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
#include <fmt/ranges.h>

#include <algorithm>
#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "common/logging.h"
#include "dingosdk/status.h"
#include "glog/logging.h"
#include "sdk/client_stub.h"
#include "sdk/transaction/txn_task/txn_check_secondary_locks_task.h"
#include "sdk/transaction/txn_task/txn_check_status_task.h"
#include "sdk/transaction/txn_task/txn_resolve_lock_task.h"

namespace dingodb {
namespace sdk {

TxnLockResolver::TxnLockResolver(const ClientStub& stub) : stub_(stub) {}

Status TxnLockResolver::ResolveLock(const pb::store::LockInfo& conflict_lock_info, int64_t start_ts,
                                    bool force_sync_commit) {
  DINGO_LOG(INFO) << fmt::format("[sdk.txn.{}] resolve lock, lock_info({}).", start_ts,
                                 conflict_lock_info.ShortDebugString());

  // check primary key lock status
  TxnStatus txn_status;
  TxnCheckStatusTask task_check_status(stub_, conflict_lock_info.lock_ts(), conflict_lock_info.primary_lock(), start_ts,
                                       txn_status, force_sync_commit);
  Status status = task_check_status.Run();
  if (!status.ok()) {
    if (status.IsNotFound()) {
      DINGO_LOG(DEBUG) << fmt::format("[sdk.txn.{}] not exist txn when check status, status({}) lock({}).", start_ts,
                                      status.ToString(), conflict_lock_info.ShortDebugString());

      return Status::OK();
    } else {
      return status;
    }
  }

  if (txn_status.primary_lock_info.lock_ts() > 0 && txn_status.primary_lock_info.use_async_commit() &&
      !force_sync_commit) {
    // resolve async commit lock
    return ResolveLockSecondaryLocks(txn_status.primary_lock_info, start_ts, txn_status, conflict_lock_info);
  }

  // resolve normal lock
  return ResolveNormalLock(conflict_lock_info, start_ts, txn_status);
}

Status TxnLockResolver::ResolveNormalLock(const pb::store::LockInfo& lock_info, int64_t start_ts,
                                          const TxnStatus& txn_status) {
  if (txn_status.IsMinCommitTSPushed()) {
    return Status::PushMinCommitTs("push min_commit_ts");
  }

  // primary key exist lock then outer txn rollback
  if (txn_status.IsLocked()) {
    return Status::TxnLockConflict(fmt::format("lock still exist, lock_info({})", lock_info.ShortDebugString()));
  }

  Status status;

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
  std::vector<std::string> keys = {lock_info.key()};
  TxnResolveLockTask task_resolve_lock(stub_, lock_info.lock_ts(), keys, txn_status.commit_ts);
  status = task_resolve_lock.Run();
  if (!status.IsOK()) {
    DINGO_LOG(WARNING) << fmt::format("[sdk.txn.{}] resolve lock fail, lock_ts({}) key({}) txn_status({}) status({}).",
                                      start_ts, lock_info.lock_ts(), lock_info.key(), txn_status.ToString(),
                                      status.ToString());
    return status;
  }

  return Status::OK();
}

Status TxnLockResolver::ResolveLockSecondaryLocks(const pb::store::LockInfo& primary_lock_info, int64_t start_ts,
                                                  const TxnStatus& txn_status,
                                                  const pb::store::LockInfo& conflict_lock_info) {
  DINGO_LOG(DEBUG) << fmt::format("[sdk.txn.{}] lock use async commit, lock_info({}), txn_status({}).", start_ts,
                                  primary_lock_info.ShortDebugString(), txn_status.ToString());

  // check lock ttl expired
  int64_t current_ts;
  Status status1 = stub_.GetTsoProvider()->GenPhysicalTs(2, current_ts);
  if (!status1.ok()) {
    return status1;
  }
  if (txn_status.primary_lock_info.lock_ttl() > current_ts) {
    // lock ttl not expired, can not resolve lock now
    DINGO_LOG(DEBUG) << fmt::format(
        "[sdk.txn.{}] async commit lock ttl not expired, lock_info({}), txn_status({}), current_ts({}).", start_ts,
        primary_lock_info.ShortDebugString(), txn_status.ToString(), current_ts);
    return Status::TxnLockConflict(
        fmt::format("async commit lock ttl not expired, lock_info({}), txn_status({}), current_ts({}).",
                    primary_lock_info.ShortDebugString(), txn_status.ToString(), current_ts));
  }

  // check secondary locks status
  TxnSecondaryLockStatus txn_secondary_lock_status;
  std::vector<std::string> secondary_keys(primary_lock_info.secondaries().begin(),
                                          primary_lock_info.secondaries().end());
  TxnCheckSecondaryLocksTask txn_check_secondary_locks_task(stub_, std::move(secondary_keys),
                                                            primary_lock_info.lock_ts(), txn_secondary_lock_status);
  Status status = txn_check_secondary_locks_task.Run();
  if (!status.ok()) {
    DINGO_LOG(WARNING) << fmt::format(
        "[sdk.txn.{}] check secondary locks fail, primary_lock_info({}), txn_status({}), status({}).",
        primary_lock_info.lock_ts(), primary_lock_info.ShortDebugString(), txn_status.ToString(), status.ToString());
    return status;
  }

  // check validate secondary lock status
  CHECK(!txn_secondary_lock_status.IsError())
      << fmt::format("[sdk.txn.{}]unexpected txn_secondary_lock_status: {}", primary_lock_info.lock_ts(),
                     txn_secondary_lock_status.ToString());

  if (txn_secondary_lock_status.is_rollbacked || txn_secondary_lock_status.is_txn_not_found) {
    // partial secondary keys is rollbacked or not prewrite, rollback all keys
    std::vector<std::string> keys_to_rollback{primary_lock_info.primary_lock()};
    for (auto& lock : txn_secondary_lock_status.locked_keys) {
      // check lock_ts consistency
      CHECK(lock.lock_ts() == primary_lock_info.lock_ts()) << fmt::format(
          "[sdk.txn.{}] inconsistent lock_ts, primary_lock_info({}) lock({}) txn_secondary_lock_status({}).", start_ts,
          primary_lock_info.ShortDebugString(), lock.ShortDebugString(), txn_secondary_lock_status.ToString());
      keys_to_rollback.push_back(lock.key());
    }

    TxnResolveLockTask task_resolve_lock(stub_, primary_lock_info.lock_ts(), keys_to_rollback, 0 /* commit_ts */);
    status = task_resolve_lock.Run();
    if (!status.ok()) {
      DINGO_LOG(WARNING) << fmt::format(
          "[sdk.txn.{}] resolve lock for rollback fail, lock_ts({}) keys({}) txn_status({}) status({}).", start_ts,
          primary_lock_info.lock_ts(), conflict_lock_info.key(), txn_status.ToString(), status.ToString());
      return status;
    }

  } else if (txn_secondary_lock_status.commit_ts > 0) {
    // partial secondary locks committed, need commit
    std::vector<std::string> keys_to_commit{primary_lock_info.primary_lock()};
    for (auto& lock : txn_secondary_lock_status.locked_keys) {
      // check lock_ts consistency
      CHECK(lock.lock_ts() == primary_lock_info.lock_ts()) << fmt::format(
          "[sdk.txn.{}] inconsistent lock_ts, primary_lock_info({}) lock({}) txn_secondary_lock_status({}).", start_ts,
          primary_lock_info.ShortDebugString(), lock.ShortDebugString(), txn_secondary_lock_status.ToString());
      CHECK(lock.min_commit_ts() <= txn_secondary_lock_status.commit_ts) << fmt::format(
          "[sdk.txn.{}] inconsistent min_commit_ts, primary_lock_info({}) lock({}) txn_secondary_lock_status({}).",
          start_ts, primary_lock_info.ShortDebugString(), lock.ShortDebugString(),
          txn_secondary_lock_status.ToString());
      keys_to_commit.push_back(lock.key());
    }

    // commit all locked keys
    TxnResolveLockTask task_resolve_lock(stub_, primary_lock_info.lock_ts(), keys_to_commit,
                                         txn_secondary_lock_status.commit_ts);
    status = task_resolve_lock.Run();
    if (!status.ok()) {
      DINGO_LOG(WARNING) << fmt::format(
          "[sdk.txn.{}] resolve lock for commit fail, lock_ts({}) keys({}) commit_ts({}) status({}).", start_ts,
          primary_lock_info.lock_ts(), conflict_lock_info.key(), txn_secondary_lock_status.commit_ts,
          status.ToString());
      return status;
    }

  } else if (txn_secondary_lock_status.locked_keys.size() > 0) {
    // all secondary locks still exist, need commit
    CHECK(txn_secondary_lock_status.locked_keys.size() == primary_lock_info.secondaries_size());

    // calculate min_commit_ts
    int64_t min_commit_ts = primary_lock_info.min_commit_ts();
    std::vector<std::string> keys_to_commit{primary_lock_info.primary_lock()};
    for (auto& lock : txn_secondary_lock_status.locked_keys) {
      // check lock_ts consistency
      CHECK(lock.lock_ts() == primary_lock_info.lock_ts())
          << fmt::format("[sdk.txn.{}] inconsistent lock_ts, primary_lock_info({}) lock({}).", start_ts,
                         primary_lock_info.ShortDebugString(), lock.ShortDebugString());
      if (!lock.use_async_commit() || lock.min_commit_ts() == 0) {
        // check all secondary lock use async commit, if not, downgrade to 2PC to normal resolve lock
        DINGO_LOG(WARNING) << fmt::format("[sdk.txn.{}] unexpected secondary locks still exist, lock_ts({}) lock({}).",
                                          start_ts, primary_lock_info.lock_ts(), lock.ShortDebugString());
        return ResolveLock(conflict_lock_info, start_ts, true);
      }

      keys_to_commit.push_back(lock.key());
      min_commit_ts = std::max(min_commit_ts, lock.min_commit_ts());
    }

    CHECK(min_commit_ts > 0) << fmt::format(
        "[sdk.txn.{}] invalid min_commit_ts({}) from secondary locks, primary_lock_info({}).", start_ts, min_commit_ts,
        primary_lock_info.ShortDebugString());

    // commit all keys
    TxnResolveLockTask task_resolve_lock(stub_, primary_lock_info.lock_ts(), keys_to_commit, min_commit_ts);
    status = task_resolve_lock.Run();
    if (!status.ok()) {
      DINGO_LOG(WARNING) << fmt::format(
          "[sdk.txn.{}] resolve lock for commit fail, lock_ts({}) keys({}) commit_ts({}) status({}).", start_ts,
          primary_lock_info.lock_ts(), conflict_lock_info.key(), min_commit_ts, status.ToString());
      return status;
    }
  }

  return Status::OK();
}

}  // namespace sdk
}  // namespace dingodb