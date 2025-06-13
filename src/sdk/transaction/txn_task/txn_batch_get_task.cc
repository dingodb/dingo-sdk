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

#include "sdk/transaction/txn_task/txn_batch_get_task.h"

#include <glog/logging.h>

#include <cstdint>
#include <set>
#include <string>
#include <string_view>
#include <vector>

#include "common/logging.h"
#include "dingosdk/client.h"
#include "dingosdk/status.h"
#include "sdk/common/common.h"
#include "sdk/transaction/txn_common.h"
#include "sdk/utils/rw_lock.h"

namespace dingodb {
namespace sdk {

Status TxnBatchGetTask::Init() {
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

void TxnBatchGetTask::DoAsync() {
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
    if (region_id_to_resolved_lock_.find(region_id) != region_id_to_resolved_lock_.end()) {
      resolved_lock = region_id_to_resolved_lock_[region_id];
    }

    auto rpc = std::make_unique<TxnBatchGetRpc>();
    rpc->MutableRequest()->Clear();
    rpc->MutableRequest()->set_start_ts(txn_impl_->GetStartTs());
    FillRpcContext(*rpc->MutableRequest()->mutable_context(), region->RegionId(), region->Epoch(), {resolved_lock},
                   ToIsolationLevel(txn_impl_->GetOptions().isolation));
    for (const auto& key : entry.second) {
      *rpc->MutableRequest()->add_keys() = key;
    }
    StoreRpcController controller(stub, *rpc, region);
    controllers_.push_back(std::move(controller));
    rpcs_.push_back(std::move(rpc));
  }
  CHECK(rpcs_.size() == controllers_.size());
  sub_tasks_count_.store(rpcs_.size());

  region_id_to_resolved_lock_.clear();

  for (int i = 0; i < rpcs_.size(); ++i) {
    auto& controller = controllers_[i];
    controller.AsyncCall([this, rpc = rpcs_[i].get()](const Status& s) { TxnBatchGetRpcCallback(s, rpc); });
  }
}

void TxnBatchGetTask::TxnBatchGetRpcCallback(const Status& status, TxnBatchGetRpc* rpc) {
  Status s;
  bool need_retry = false;
  const auto* response = rpc->Response();
  if (!status.ok()) {
    DINGO_LOG(WARNING) << "rpc: " << rpc->Method() << " send to region: " << rpc->Request()->context().region_id()
                       << " fail: " << status.ToString();
    s = status;
  } else {
    if (response->has_txn_result()) {
      s = CheckTxnResultInfo(response->txn_result());
      if (s.IsTxnLockConflict()) {
        s = stub.GetTxnLockResolver()->ResolveLock(response->txn_result().locked(), txn_impl_->GetStartTs());
        if (s.ok()) {
          need_retry = true;
        } else if (s.IsPushMinCommitTs()) {
          region_id_to_resolved_lock_[rpc->Request()->context().region_id()] =
              response->txn_result().locked().lock_ts();
          need_retry = true;
          s = Status::OK();
        }
      }
    }
  }

  {
    ReadLockGuard guard(rw_lock_);
    if (s.ok()) {
      if (!need_retry) {
        for (const auto& kv : response->kvs()) {
          if (!kv.value().empty()) {
            // save the kvs result
            out_kvs_.push_back({kv.key(), kv.value()});
            // remove the keys that have been processed
            next_keys_.erase(kv.key());
          }
        }
      } else {
        need_retry_ = true;
      }
    } else {
      if (status_.ok()) {
        // only return first fail status
        status_ = s;
      }
      DINGO_LOG(WARNING) << fmt::format("[sdk.txn.{}] batch get fail, region({}) status({}) txn_result({}).",
                                        txn_impl_->ID(), rpc->Request()->context().region_id(), s.ToString(),
                                        response->txn_result().ShortDebugString());
    }
  }

  if (sub_tasks_count_.fetch_sub(1) == 1) {
    Status tmp;
    bool tmp_need_retry = false;
    {
      ReadLockGuard guard(rw_lock_);
      tmp = status_;
      tmp_need_retry = need_retry_;
    }
    if (tmp.ok() && tmp_need_retry) {
      DoAsyncRetry();
      return;
    }
    DoAsyncDone(tmp);
  }
}

}  // namespace sdk
}  // namespace dingodb
