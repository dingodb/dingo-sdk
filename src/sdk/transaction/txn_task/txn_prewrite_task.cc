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

#include "sdk/transaction/txn_task/txn_prewrite_task.h"

#include <cstdint>

#include "common/logging.h"
#include "dingosdk/status.h"
#include "sdk/common/common.h"
#include "sdk/common/helper.h"
#include "sdk/common/param_config.h"
#include "sdk/region.h"
#include "sdk/rpc/store_rpc_controller.h"
#include "sdk/transaction/txn_buffer.h"
#include "sdk/transaction/txn_common.h"
#include "sdk/transaction/txn_lock_resolver.h"
#include "sdk/utils/callback.h"

namespace dingodb {
namespace sdk {

Status TxnPrewriteTask::Init() {
  next_mutations_.clear();
  for (const auto& [key, mutation] : mutations_) {
    if (next_mutations_.find(mutation->key) != next_mutations_.end()) {
      // duplicate mutation
      std::string msg = fmt::format("duplicate mutation: {}", mutation->ToString());
      DINGO_LOG(ERROR) << msg;
      return Status::InvalidArgument(msg);
    } else {
      next_mutations_.emplace(mutation->key, mutation);
    }
  }
  return Status::OK();
}

void TxnPrewriteTask::DoAsync() {
  std::map<std::string, const TxnMutation*> next_batch;
  {
    WriteLockGuard guard(rw_lock_);
    next_batch = next_mutations_;
    need_retry_ = false;
    status_ = Status::OK();
  }

  if (next_batch.empty()) {
    DoAsyncDone(Status::OK());
    return;
  }

  rpcs_.clear();
  controllers_.clear();

  std::unordered_map<int64_t, std::shared_ptr<Region>> region_id_to_region;
  std::unordered_map<int64_t, std::vector<const TxnMutation*>> region_id_to_mutations;

  auto meta_cache = stub.GetMetaCache();
  for (const auto& [_, mutation] : next_batch) {
    std::shared_ptr<Region> tmp;
    Status s = meta_cache->LookupRegionByKey(mutation->key, tmp);
    if (!s.ok()) {
      DoAsyncDone(s);
      return;
    }
    auto iter = region_id_to_region.find(tmp->RegionId());
    if (iter == region_id_to_region.end()) {
      region_id_to_region.emplace(std::make_pair(tmp->RegionId(), tmp));
    }

    if (region_id_to_mutations.find(tmp->RegionId()) == region_id_to_mutations.end()) {
      region_id_to_mutations[tmp->RegionId()] = {mutation};
    } else {
      if (primary_key_ != mutation->key) {
        region_id_to_mutations[tmp->RegionId()].push_back(mutation);
      } else {
        // If primary key is in the mutations, we need to put it at the front of the mutations list
        const auto* front = region_id_to_mutations[tmp->RegionId()].front();
        region_id_to_mutations[tmp->RegionId()][0] = mutation;
        region_id_to_mutations[tmp->RegionId()].push_back(front);
      }
    }
  }


  int64_t physical_ts{0};
  Status status = stub.GetTsoProvider()->GenPhysicalTs(2, physical_ts);

  for (const auto& entry : region_id_to_mutations) {
    auto region_id = entry.first;
    auto iter = region_id_to_region.find(region_id);
    CHECK(iter != region_id_to_region.end());
    auto region = iter->second;

    auto rpc = std::make_unique<TxnPrewriteRpc>();
    rpc->MutableRequest()->Clear();
    rpc->MutableRequest()->set_start_ts(txn_impl_->GetStartTs());
    FillRpcContext(*rpc->MutableRequest()->mutable_context(), region->RegionId(), region->Epoch(),
                   ToIsolationLevel(txn_impl_->GetOptions().isolation));
    rpc->MutableRequest()->set_primary_lock(primary_key_);
    rpc->MutableRequest()->set_lock_ttl(physical_ts + FLAGS_txn_heartbeat_lock_delay_ms);
    rpc->MutableRequest()->set_txn_size(mutations_.size());
    rpc->MutableRequest()->set_try_one_pc(is_one_pc_);

    for (const auto& mutation : entry.second) {
      if (rpc->MutableRequest()->mutations_size() == FLAGS_txn_max_batch_count) {
        DINGO_LOG(INFO) << fmt::format("[sdk.txn.{}] precommit key, region({}) mutations({}) equal max batch count.",
                                       txn_impl_->ID(), region->RegionId(), rpc->MutableRequest()->mutations_size());
        StoreRpcController controller(stub, *rpc, region);
        controllers_.push_back(std::move(controller));
        rpcs_.push_back(std::move(rpc));

        // reset rpc
        rpc = std::make_unique<TxnPrewriteRpc>();
        rpc->MutableRequest()->Clear();
        rpc->MutableRequest()->set_start_ts(txn_impl_->GetStartTs());
        FillRpcContext(*rpc->MutableRequest()->mutable_context(), region->RegionId(), region->Epoch(),
                       ToIsolationLevel(txn_impl_->GetOptions().isolation));
        rpc->MutableRequest()->set_primary_lock(primary_key_);
        rpc->MutableRequest()->set_lock_ttl(physical_ts + FLAGS_txn_heartbeat_lock_delay_ms);
        rpc->MutableRequest()->set_txn_size(mutations_.size());
        rpc->MutableRequest()->set_try_one_pc(is_one_pc_);
      }

      TxnMutation2MutationPB(*mutation, rpc->MutableRequest()->add_mutations());
    }

    StoreRpcController controller(stub, *rpc, region);
    controllers_.push_back(std::move(controller));
    rpcs_.push_back(std::move(rpc));
  }

  CHECK(rpcs_.size() == controllers_.size());

  sub_tasks_count_.store(rpcs_.size());

  for (int i = 0; i < rpcs_.size(); ++i) {
    auto& controller = controllers_[i];
    controller.AsyncCall([this, rpc = rpcs_[i].get()](const Status& s) { TxnPrewriteRpcCallback(s, rpc); });
  }
}

void TxnPrewriteTask::TxnPrewriteRpcCallback(const Status& status, TxnPrewriteRpc* rpc) {
  DINGO_LOG(DEBUG) << "rpc : " << rpc->Method() << " request : " << rpc->Request()->ShortDebugString()
                   << " response : " << rpc->Response()->ShortDebugString();
  bool need_retry = false;
  Status s;
  const auto* response = rpc->Response();
  if (!status.ok()) {
    DINGO_LOG(WARNING) << "rpc: " << rpc->Method() << " send to region: " << rpc->Request()->context().region_id()
                       << " fail: " << status.ToString();

    s = status;
  } else {
    Status s1;
    for (const auto& txn_result : response->txn_result()) {
      s1 = CheckTxnResultInfo(txn_result);
      if (s1.ok()) {
        continue;
      } else if (s1.IsTxnLockConflict()) {
        s1 = stub.GetTxnLockResolver()->ResolveLock(txn_result.locked(), txn_impl_->GetStartTs());
        if (!s1.ok()) {
          DINGO_LOG(WARNING) << fmt::format("[sdk.txn.{}] precommit resolve lock fail, pk() status({}) txn_result({}).",
                                            txn_impl_->ID(), StringToHex(primary_key_), s1.ToString(),
                                            txn_result.ShortDebugString());

          s = s1;
        } else {
          // need to retry
          need_retry = true;
        }

      } else if (s1.IsTxnWriteConflict()) {
        DINGO_LOG(WARNING) << fmt::format("[sdk.txn.{}] precommit write conflict, pk({}) status({}) txn_result({}).",
                                          txn_impl_->ID(), StringToHex(primary_key_), s1.ToString(),
                                          txn_result.ShortDebugString());
        s = s1;

        break;

      } else {
        DINGO_LOG(WARNING) << fmt::format("[sdk.txn.{}] precommit unexpect response, pk({}) , status({}) response({}).",
                                          txn_impl_->ID(), StringToHex(primary_key_), s1.ToString(),
                                          response->ShortDebugString());

        s = s1;

        break;
      }
    }
  }

  {
    WriteLockGuard guard(rw_lock_);
    if (s.ok()) {
      if (!need_retry) {
        for (const auto& mutation : rpc->Request()->mutations()) {
          next_mutations_.erase(mutation.key());
        }
      } else {
        need_retry_ = true;
      }
    } else {
      // only return first fail status
      if (status_.ok()) {
        status_ = s;
      }
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
