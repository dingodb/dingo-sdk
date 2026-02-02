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

#include "sdk/transaction/txn_task/txn_resolve_lock_task.h"

#include <fmt/format.h>

#include "common/logging.h"
#include "dingosdk/status.h"
#include "sdk/common/common.h"
#include "sdk/common/helper.h"
#include "sdk/region.h"
#include "sdk/rpc/store_rpc_controller.h"
#include "sdk/transaction/txn_common.h"
#include "sdk/transaction/txn_lock_resolver.h"
#include "sdk/utils/callback.h"

namespace dingodb {
namespace sdk {

Status TxnResolveLockTask::Init() {
  next_keys_.clear();
  for (const auto& str : keys_) {
    if (!next_keys_.insert(str).second) {
      // duplicate key
      std::string msg = fmt::format("[sdk.txn] lock_ts: {}, duplicate key: {}", lock_ts_, StringToHex(str));
      DINGO_LOG(ERROR) << msg;
      return Status::InvalidArgument(msg);
    }
  }
  return Status::OK();
}

void TxnResolveLockTask::DoAsync() {
  std::unordered_set<std::string_view> next_batch;
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

    auto rpc = std::make_unique<TxnResolveLockRpc>();
    FillRpcContext(*rpc->MutableRequest()->mutable_context(), region->RegionId(), region->GetEpoch(),
                   pb::store::IsolationLevel::SnapshotIsolation);
    rpc->MutableRequest()->set_start_ts(lock_ts_);
    rpc->MutableRequest()->set_commit_ts(commit_ts_);
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
    controller.AsyncCall([this, rpc = rpcs_[i].get()](const Status& s) { TxnResolveLockRpcCallback(s, rpc); });
  }
}

void TxnResolveLockTask::TxnResolveLockRpcCallback(const Status& status, TxnResolveLockRpc* rpc) {
  DINGO_LOG(DEBUG) << fmt::format("[sdk.txn.{}] rpc: {} request: {} response: {}", lock_ts_, rpc->Method(),
                                  rpc->Request()->ShortDebugString(), rpc->Response()->ShortDebugString());
  const auto* response = rpc->Response();
  Status s;
  if (!status.ok()) {
    DINGO_LOG(WARNING) << fmt::format("[sdk.txn.{}] rpc: {} send to region: {} fail: {}", lock_ts_, rpc->Method(),
                                      rpc->Request()->context().region_id(), status.ToString());
    s = status;
  } else {
    if (response->has_txn_result()) {
      s = CheckTxnResultInfo(response->txn_result());
      if (!s.ok()) {
        DINGO_LOG(WARNING) << fmt::format("[sdk.txn] txn_resolve_lock fail, lock_ts: {},  response: {}, status: {}.",
                                          lock_ts_, response->ShortDebugString(), status_.ToString());
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