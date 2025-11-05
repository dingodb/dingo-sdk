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

#ifndef DINGODB_SDK_TRANSACTION_LOCK_RESOLVER_H_
#define DINGODB_SDK_TRANSACTION_LOCK_RESOLVER_H_

#include <glog/logging.h>

#include <algorithm>
#include <cstdint>
#include <string>
#include <vector>

#include "dingosdk/status.h"
#include "fmt/core.h"
#include "proto/store.pb.h"

namespace dingodb {
namespace sdk {

class ClientStub;

struct TxnSecondaryLockStatus {
  std::vector<pb::store::LockInfo> locked_keys;
  uint64_t commit_ts;
  bool is_txn_not_found;
  bool is_rollbacked;
  explicit TxnSecondaryLockStatus() : commit_ts(0), is_rollbacked(false), locked_keys({}), is_txn_not_found(false) {}
  explicit TxnSecondaryLockStatus(uint64_t commit_ts, bool is_rollbacked = false,
                                  std::vector<pb::store::LockInfo> locked_keys = {}, bool is_txn_not_found = {})
      : commit_ts(commit_ts),
        is_rollbacked(is_rollbacked),
        locked_keys(std::move(locked_keys)),
        is_txn_not_found(is_txn_not_found) {}

  bool IsError() const {
    return (is_txn_not_found && commit_ts > 0) || (is_rollbacked && commit_ts > 0) ||
           (commit_ts == 0 && !is_rollbacked && locked_keys.size() == 0 && !is_txn_not_found);
  }

  std::string ToString() const {
    std::string locks_detail;
    for (size_t i = 0; i < locked_keys.size(); ++i) {
      if (i > 0) locks_detail += ", ";
      locks_detail += fmt::format("{}", locked_keys[i].ShortDebugString());
    }
    return fmt::format("commit_ts({}) is_rollbacked({}) is_txn_not_found({}) locked_keys_size({}) [{}] ", commit_ts,
                       is_rollbacked, locked_keys.size(), is_txn_not_found, locks_detail);
  }

  std::string ToShortString() const {
    return fmt::format("commit_ts({}) is_rollbacked({}) locked_keys_size({}) txn_not_found({})", commit_ts,
                       is_rollbacked, locked_keys.size(), is_txn_not_found);
  }
};

struct TxnStatus {
  int64_t lock_ttl;
  int64_t commit_ts;
  pb::store::Action action;
  // only for async commit lock
  pb::store::LockInfo primary_lock_info;

  explicit TxnStatus() : lock_ttl(-1), commit_ts(-1), action(pb::store::NoAction) {}
  explicit TxnStatus(int64_t lock_ttl, int64_t commit_ts, const pb::store::Action& action)
      : lock_ttl(lock_ttl), commit_ts(commit_ts), action(action) {}
  explicit TxnStatus(int64_t lock_ttl, int64_t commit_ts, const pb::store::Action& action,
                     const pb::store::LockInfo& primary_lock_info)
      : lock_ttl(lock_ttl), commit_ts(commit_ts), action(action), primary_lock_info(primary_lock_info) {}
  bool IsCommitted() const { return commit_ts > 0; }

  bool IsRollbacked() const { return lock_ttl == 0 && commit_ts == 0; }

  bool IsLocked() const { return lock_ttl > 0; }

  bool IsMinCommitTSPushed() const { return action == pb::store::MinCommitTSPushed; }

  std::string ToString() const {
    return fmt::format("lock_ttl({}) commit_ts({}) primary_lock_info({})", lock_ttl, commit_ts,
                       primary_lock_info.ShortDebugString());
  }
};

class TxnLockResolver {
 public:
  explicit TxnLockResolver(const ClientStub& stub);

  virtual ~TxnLockResolver() = default;

  virtual Status ResolveLock(const pb::store::LockInfo& conflict_lock_info, int64_t start_ts,
                             bool force_sync_commit = false);

  virtual Status ResolveLockSecondaryLocks(const pb::store::LockInfo& primary_lock_info, int64_t start_ts,
                                           const TxnStatus& txn_status, const pb::store::LockInfo& conflict_lock_info);

  virtual Status ResolveNormalLock(const pb::store::LockInfo& lock_info, int64_t start_ts, const TxnStatus& txn_status);

 private:
  const ClientStub& stub_;
};

}  // namespace sdk
}  // namespace dingodb

#endif  // DINGODB_SDK_TRANSACTION_LOCK_RESOLVER_H_