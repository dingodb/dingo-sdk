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

#include <fmt/format.h>
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
      std::string msg = fmt::format("[sdk.txn.{}] duplicate key: {}", txn_impl_->ID(), StringToHex(str));
      DINGO_LOG(ERROR) << msg;
      return Status::InvalidArgument(msg);
    }
  }
  return Status::OK();
}

void TxnBatchGetTask::DoAsync() {
  // Owns this frame's +1 pending count. Declared before the tracer cleanup so
  // it runs after it (reverse destruction order): once released, the last
  // finished sub task may complete the task and destroy it, so nothing may
  // touch task state afterwards.
  std::shared_ptr<Round> round;
  SCOPED_CLEANUP({
    if (round) {
      FinishSubTask(round);
    }
  });

  auto start_time = TimestampUs();
  SCOPED_CLEANUP(txn_impl_->GetTracer()->IncrementReadSdkTime(TimestampUs() - start_time););

  std::set<std::string_view> next_batch;
  std::unordered_map<int64_t, uint64_t> resolved_locks;
  {
    WriteLockGuard guard(rw_lock_);
    next_batch = next_keys_;
    need_retry_ = false;
    status_ = Status::OK();
    resolved_locks.swap(region_id_to_resolved_lock_);
  }

  if (next_batch.empty()) {
    // completes via the round release above
    round = std::make_shared<Round>();
    round->pending.store(1);
    return;
  }

  std::unordered_map<int64_t, std::shared_ptr<Region>> region_id_to_region;
  std::unordered_map<int64_t, std::vector<std::string_view>> region_id_to_keys;

  auto meta_cache = stub.GetMetaCache();
  for (const auto& key : next_batch) {
    std::shared_ptr<Region> tmp;
    Status s = meta_cache->LookupRegionByKey(key, tmp);
    if (!s.ok()) {
      {
        WriteLockGuard guard(rw_lock_);
        status_ = s;
        need_retry_ = false;
      }
      round = std::make_shared<Round>();
      round->pending.store(1);
      return;
    }
    auto iter = region_id_to_region.find(tmp->RegionId());
    if (iter == region_id_to_region.end()) {
      region_id_to_region.emplace(std::make_pair(tmp->RegionId(), tmp));
    }
    region_id_to_keys[tmp->RegionId()].push_back(key);
  }

  round = std::make_shared<Round>();
  round->controllers.reserve(region_id_to_keys.size());
  round->rpcs.reserve(region_id_to_keys.size());
  for (const auto& entry : region_id_to_keys) {
    auto region_id = entry.first;
    auto iter = region_id_to_region.find(region_id);
    CHECK(iter != region_id_to_region.end());
    auto region = iter->second;

    uint64_t resolved_lock = 0;
    auto resolved_iter = resolved_locks.find(region_id);
    if (resolved_iter != resolved_locks.end()) {
      resolved_lock = resolved_iter->second;
    }

    auto rpc = std::make_shared<TxnBatchGetRpc>();
    rpc->MutableRequest()->Clear();
    rpc->MutableRequest()->set_start_ts(txn_impl_->GetStartTs());
    rpc->SetTxnId(txn_impl_->GetStartTs());
    FillRpcContext(*rpc->MutableRequest()->mutable_context(), region->RegionId(), region->GetEpoch(), {resolved_lock},
                   ToIsolationLevel(txn_impl_->GetOptions().isolation));
    for (const auto& key : entry.second) {
      *rpc->MutableRequest()->add_keys() = key;
    }
    round->controllers.emplace_back(stub, rpc, region);
    round->rpcs.push_back(rpc);
  }
  CHECK(round->rpcs.size() == round->controllers.size());
  // +1 for this issuing frame, released by the scoped cleanup above
  round->pending.store(static_cast<int>(round->rpcs.size()) + 1);

  for (size_t i = 0; i < round->rpcs.size(); ++i) {
    auto& controller = round->controllers[i];
    // round is captured by value: the callbacks collectively keep this
    // round's rpcs/controllers alive until every chain fully unwinds
    controller.AsyncCall(
        [this, round, rpc = round->rpcs[i].get()](const Status& s) { TxnBatchGetRpcCallback(round, s, rpc); });
  }
}

void TxnBatchGetTask::TxnBatchGetRpcCallback(const std::shared_ptr<Round>& round, const Status& status,
                                             TxnBatchGetRpc* rpc) {
  DINGO_LOG(DEBUG) << fmt::format("[sdk.txn.{}] rpc: {} request: {} response: {}", txn_impl_->ID(), rpc->Method(),
                                  rpc->Request()->ShortDebugString(), rpc->Response()->ShortDebugString());

  txn_impl_->GetTracer()->IncrementReadRpcTime(rpc->ElapsedTimeUs());
  txn_impl_->GetTracer()->IncrementReadRpcRetryCount(rpc->GetRetryTimes());
  txn_impl_->GetTracer()->IncrementSleepTime(rpc->GetSleepTimesUs());
  txn_impl_->GetTracer()->IncrementSleepCount(rpc->GetSleepCount());

  Status s;
  bool need_retry = false;
  uint64_t resolved_lock_ts = 0;
  const auto* response = rpc->Response();
  if (!status.ok()) {
    DINGO_LOG(WARNING) << fmt::format("[sdk.txn.{}] rpc: {} send to region: {} fail: {}", txn_impl_->ID(),
                                      rpc->Method(), rpc->Request()->context().region_id(), status.ToString());
    s = status;
  } else {
    if (response->has_txn_result()) {
      s = CheckTxnResultInfo(response->txn_result());
      if (s.IsTxnLockConflict()) {
        auto start_time = TimestampUs();
        s = stub.GetTxnLockResolver()->ResolveLock(response->txn_result().locked(), txn_impl_->GetStartTs());
        if (s.ok()) {
          need_retry = true;
        } else if (s.IsPushMinCommitTs()) {
          resolved_lock_ts = response->txn_result().locked().lock_ts();
          need_retry = true;
          s = Status::OK();
        }
        txn_impl_->GetTracer()->IncrementResolveLockTime(TimestampUs() - start_time);
      } else if (!s.ok()) {
        DINGO_LOG(WARNING) << fmt::format("[sdk.txn.{}] batch get fail, status({}) , txn_result({}).", txn_impl_->ID(),
                                          s.ToString(), response->txn_result().ShortDebugString());
      }
    }
  }

  {
    WriteLockGuard guard(rw_lock_);
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
        if (resolved_lock_ts != 0) {
          region_id_to_resolved_lock_[rpc->Request()->context().region_id()] = resolved_lock_ts;
        }
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

  FinishSubTask(round);
}

void TxnBatchGetTask::FinishSubTask(const std::shared_ptr<Round>& round) {
  if (round->pending.fetch_sub(1) != 1) {
    return;
  }

  Status tmp;
  bool tmp_need_retry = false;
  {
    ReadLockGuard guard(rw_lock_);
    tmp = status_;
    tmp_need_retry = need_retry_;
  }
  if (tmp.ok() && tmp_need_retry) {
    txn_impl_->GetTracer()->IncrementReadRetryCount(1);
    DoAsyncRetry();
    return;
  }
  DoAsyncDone(tmp);
}

}  // namespace sdk
}  // namespace dingodb
