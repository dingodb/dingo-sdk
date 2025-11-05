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

#include "sdk/transaction/txn_task/txn_check_secondary_locks_task.h"

#include <fmt/format.h>
#include <glog/logging.h>

#include <algorithm>
#include <cstdint>
#include <utility>

#include "common/logging.h"
#include "dingosdk/status.h"
#include "sdk/common/common.h"
#include "sdk/rpc/store_rpc_controller.h"
#include "sdk/transaction/txn_common.h"
#include "sdk/utils/rw_lock.h"

namespace dingodb {
namespace sdk {
Status TxnCheckSecondaryLocksTask::Init() {
  next_keys_.clear();
  for (const auto& str : secondary_keys_) {
    if (!next_keys_.insert(str).second) {
      // duplicate key
      std::string msg = fmt::format("[sdk.txn.{}] duplicate key: {}", primary_lock_start_ts_, str);
      DINGO_LOG(ERROR) << msg;
      return Status::InvalidArgument(msg);
    }
  }
  return Status::OK();
}

void TxnCheckSecondaryLocksTask::DoAsync() {
  std::set<std::string_view> next_batch;
  {
    WriteLockGuard guard(rw_lock_);
    next_batch = next_keys_;
    status_ = Status::OK();
  }

  if (next_batch.empty()) {
    DoAsyncDone(Status::OK());
    return;
  }

  rpcs_.clear();
  controllers_.clear();

  std::unordered_map<int64_t, std::shared_ptr<Region>> region_id_to_region;
  std::unordered_map<int64_t, std::vector<std::string_view>> region_id_to_keys;

  auto meta_cache = stub.GetMetaCache();
  for (const auto& key : next_batch) {
    std::shared_ptr<Region> tmp;
    Status s = meta_cache->LookupRegionByKey(key, tmp);
    if (!s.ok()) {
      DoAsyncDone(s);
      return;
    }
    auto iter = region_id_to_region.find(tmp->RegionId());
    if (iter == region_id_to_region.end()) {
      region_id_to_region.emplace(std::make_pair(tmp->RegionId(), tmp));
    }
    region_id_to_keys[tmp->RegionId()].push_back(key);
  }

  for (const auto& entry : region_id_to_keys) {
    auto region_id = entry.first;
    auto iter = region_id_to_region.find(region_id);
    CHECK(iter != region_id_to_region.end());
    auto region = iter->second;

    uint64_t resolved_lock = 0;

    auto rpc = std::make_unique<TxnCheckSecondaryLocksRpc>();
    rpc->MutableRequest()->Clear();
    rpc->MutableRequest()->set_start_ts(primary_lock_start_ts_);
    FillRpcContext(*rpc->MutableRequest()->mutable_context(), region->RegionId(), region->GetEpoch(),
                   pb::store::IsolationLevel::SnapshotIsolation);
    for (const auto& key : entry.second) {
      *rpc->MutableRequest()->add_keys() = key;
    }
    StoreRpcController controller(stub, *rpc, region);
    controllers_.push_back(std::move(controller));
    rpcs_.push_back(std::move(rpc));
  }
  CHECK(rpcs_.size() == controllers_.size());
  sub_tasks_count_.store(rpcs_.size());

  for (int i = 0; i < rpcs_.size(); ++i) {
    auto& controller = controllers_[i];
    controller.AsyncCall([this, rpc = rpcs_[i].get()](const Status& s) { TxnCheckSecondaryLocksRpcCallback(s, rpc); });
  }
}

void TxnCheckSecondaryLocksTask::TxnCheckSecondaryLocksRpcCallback(const Status& status,
                                                                   TxnCheckSecondaryLocksRpc* rpc) {
  DINGO_LOG(DEBUG) << fmt::format("[sdk.txn.{}] rpc: {} request: {} response: {}", primary_lock_start_ts_,
                                  rpc->Method(), rpc->Request()->ShortDebugString(),
                                  rpc->Response()->ShortDebugString());
  Status s;
  const auto* response = rpc->Response();
  if (!status.ok()) {
    DINGO_LOG(WARNING) << fmt::format("[sdk.txn.{}] rpc: {} send to region: {} fail: {}", primary_lock_start_ts_,
                                      rpc->Method(), rpc->Request()->context().region_id(), status.ToString());
    s = status;
  } else {
    if (response->has_txn_result()) {
      Status s1 = CheckTxnResultInfo(response->txn_result());
      if (s1.IsTxnNotFound()) {
        WriteLockGuard guard(rw_lock_);
        txn_check_secondary_status_.is_txn_not_found = true;
      } else if (!s1.ok()) {
        s = s1;
        DINGO_LOG(WARNING) << fmt::format("[sdk.txn.{}] txn check secondary keys fail, status({}) , txn_result({}).",
                                          primary_lock_start_ts_, s.ToString(),
                                          response->txn_result().ShortDebugString());
      }
    }
    if (s.ok()) {
      if (response->locks_size() > 0) {
        WriteLockGuard guard(rw_lock_);
        txn_check_secondary_status_.locked_keys.insert(txn_check_secondary_status_.locked_keys.end(),
                                                       response->locks().begin(), response->locks().end());
      }
      if (response->commit_ts() > 0) {
        WriteLockGuard guard(rw_lock_);
        if (txn_check_secondary_status_.commit_ts > 0) {
          CHECK(response->commit_ts() == txn_check_secondary_status_.commit_ts)
              << fmt::format("[sdk.txn.{}] inconsistent commit_ts, existing({}) new({})", primary_lock_start_ts_,
                             txn_check_secondary_status_.commit_ts, response->commit_ts());
        }
        txn_check_secondary_status_.commit_ts = response->commit_ts();
      }
      if (!response->txn_result().has_txn_not_found() && response->locks_size() == 0 && response->commit_ts() == 0) {
        WriteLockGuard guard(rw_lock_);
        txn_check_secondary_status_.is_rollbacked = true;
      }
    }
  }

  {
    WriteLockGuard guard(rw_lock_);
    if (s.ok()) {
      for (const auto& key : rpc->Request()->keys()) {
        // remove the keys that have been processed
        next_keys_.erase(key);
      }
    } else {
      if (status_.ok()) {
        // only return first fail status
        status_ = s;
      }
      DINGO_LOG(WARNING) << fmt::format(
          "[sdk.txn.{}] txn check secondary keys fail, region({}) status({}) txn_result({}).", primary_lock_start_ts_,
          rpc->Request()->context().region_id(), s.ToString(), response->ShortDebugString());
    }
  }

  if (sub_tasks_count_.fetch_sub(1) == 1) {
    Status tmp;
    {
      ReadLockGuard guard(rw_lock_);
      tmp = status_;
    }
    DoAsyncDone(tmp);
  }
}

}  // namespace sdk
}  // namespace dingodb