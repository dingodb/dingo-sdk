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

#include "sdk/transaction/txn_task/txn_batch_rollback_task.h"

#include <glog/logging.h>

#include <memory>

#include "dingosdk/status.h"
#include "sdk/common/common.h"
#include "sdk/rpc/store_rpc_controller.h"
#include "sdk/transaction/txn_common.h"
#include "sdk/utils/rw_lock.h"

namespace dingodb {
namespace sdk {

Status TxnBatchRollbackTask::Init() {
  next_keys_.clear();
  for (const auto& str : keys_) {
    if (!next_keys_.insert(str).second) {
      // duplicate key
      std::string msg = fmt::format("duplicate key: {}", str);
      DINGO_LOG(ERROR) << msg;
      return Status::InvalidArgument(msg);
    }
  }
  return Status::OK();
}

void TxnBatchRollbackTask::DoAsync() {
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

  if (is_one_pc_) {
    if (region_id_to_region.size() > 1) {
      std::string msg =
          fmt::format("one pc rollback only support one region, but got {} regions", region_id_to_region.size());
      DINGO_LOG(ERROR) << msg;
      DoAsyncDone(Status::InvalidArgument(msg));
      return;
    }
  }

  for (const auto& entry : region_id_to_keys) {
    auto region_id = entry.first;
    auto iter = region_id_to_region.find(region_id);
    CHECK(iter != region_id_to_region.end());
    auto region = iter->second;

    auto rpc = std::make_unique<TxnBatchRollbackRpc>();
    rpc->MutableRequest()->Clear();
    rpc->MutableRequest()->set_start_ts(txn_impl_->GetStartTs());
    FillRpcContext(*rpc->MutableRequest()->mutable_context(), region->RegionId(), region->GetEpoch(),
                   ToIsolationLevel(txn_impl_->GetOptions().isolation));

    for (const auto& key : entry.second) {
      auto* fill = rpc->MutableRequest()->add_keys();
      *fill = key;
    }
    StoreRpcController controller(stub, *rpc, region);
    controllers_.push_back(std::move(controller));
    rpcs_.push_back(std::move(rpc));
  }

  CHECK(rpcs_.size() == controllers_.size());

  sub_tasks_count_.store(region_id_to_keys.size());

  for (int i = 0; i < rpcs_.size(); ++i) {
    auto& controller = controllers_[i];
    controller.AsyncCall([this, rpc = rpcs_[i].get()](const Status& s) { TxnBatchRollbackRpcCallback(s, rpc); });
  }
}

void TxnBatchRollbackTask::TxnBatchRollbackRpcCallback(const Status& status, TxnBatchRollbackRpc* rpc) {
  DINGO_LOG(DEBUG) << "rpc : " << rpc->Method() << " request : " << rpc->Request()->ShortDebugString()
                   << " response : " << rpc->Response()->ShortDebugString();
  Status s;
  const auto* response = rpc->Response();
  if (!status.ok()) {
    DINGO_LOG(WARNING) << "rpc: " << rpc->Method() << " send to region: " << rpc->Request()->context().region_id()
                       << " fail: " << status.ToString();

    s = status;
  } else {
    if (response->has_txn_result()) {
      s = CheckTxnResultInfo(response->txn_result());
      if (!s.ok()) {
        DINGO_LOG(WARNING) << fmt::format("[sdk.txn.{}] rollback fail, status ({}), txn_result({}).", txn_impl_->ID(),
                                          s.ToString(), response->txn_result().ShortDebugString());
      }
    }
  }

  {
    WriteLockGuard guard(rw_lock_);
    if (s.ok()) {
      for (const auto& key : rpc->Request()->keys()) {
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
