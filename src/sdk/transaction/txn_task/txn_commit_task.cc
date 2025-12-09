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

#include "sdk/transaction/txn_task/txn_commit_task.h"

#include <fmt/format.h>
#include <glog/logging.h>

#include <cstdint>
#include <utility>

#include "common/logging.h"
#include "dingosdk/status.h"
#include "sdk/common/common.h"
#include "sdk/common/helper.h"
#include "sdk/rpc/store_rpc_controller.h"
#include "sdk/transaction/txn_common.h"
#include "sdk/utils/rw_lock.h"

DECLARE_int64(txn_max_batch_count);

namespace dingodb {
namespace sdk {

Status TxnCommitTask::Init() {
  WriteLockGuard guard(rw_lock_);
  if (is_primary_) {
    CHECK(keys_.size() == 1) << fmt::format("[sdk.txn.{}] commit primary key size should be 1, but got: {}",
                                            txn_impl_->ID(), keys_.size());
  }
  next_keys_.clear();
  for (const auto& str : keys_) {
    if (!next_keys_.insert(str).second) {
      // duplicate key
      std::string msg = fmt::format("[sdk.txn.{}] duplicate key: {}", txn_impl_->ID(), str);
      DINGO_LOG(ERROR) << msg;
      return Status::InvalidArgument(msg);
    }
  }

  return Status::OK();
}

void TxnCommitTask::DoAsync() {
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

    auto rpc = std::make_unique<TxnCommitRpc>();
    rpc->MutableRequest()->Clear();
    rpc->MutableRequest()->set_start_ts(txn_impl_->GetStartTs());
    rpc->MutableRequest()->set_commit_ts(txn_impl_->GetCommitTs());
    FillRpcContext(*rpc->MutableRequest()->mutable_context(), region->RegionId(), region->GetEpoch(),
                   ToIsolationLevel(txn_impl_->GetOptions().isolation));

    for (const auto& key : entry.second) {
      if (rpc->MutableRequest()->keys_size() == FLAGS_txn_max_batch_count) {
        DINGO_LOG(INFO) << fmt::format("[sdk.txn.{}] commit key, region({}) keys({}) equal max batch count.",
                                       txn_impl_->ID(), region->RegionId(), rpc->MutableRequest()->keys_size());
        StoreRpcController controller(stub, *rpc, region);
        controllers_.push_back(std::move(controller));
        rpcs_.push_back(std::move(rpc));

        // reset rpc
        rpc = std::make_unique<TxnCommitRpc>();
        rpc->MutableRequest()->set_start_ts(txn_impl_->GetStartTs());
        rpc->MutableRequest()->set_commit_ts(txn_impl_->GetCommitTs());
        FillRpcContext(*rpc->MutableRequest()->mutable_context(), region->RegionId(), region->GetEpoch(),
                       ToIsolationLevel(txn_impl_->GetOptions().isolation));
      }
      auto* fill = rpc->MutableRequest()->add_keys();
      *fill = key;
    }

    StoreRpcController controller(stub, *rpc, region);
    controllers_.push_back(std::move(controller));
    rpcs_.push_back(std::move(rpc));
  }

  CHECK(rpcs_.size() == controllers_.size());

  sub_tasks_count_.store(rpcs_.size());

  for (int i = 0; i < rpcs_.size(); ++i) {
    auto& controller = controllers_[i];
    controller.AsyncCall([this, rpc = rpcs_[i].get()](const Status& s) { TxnCommitRpcCallback(s, rpc); });
  }
}

void TxnCommitTask::TxnCommitRpcCallback(const Status& status, TxnCommitRpc* rpc) {
  DINGO_LOG(DEBUG) << fmt::format("[sdk.txn.{}] rpc: {} request: {} response: {}", txn_impl_->ID(), rpc->Method(),
                                  rpc->Request()->ShortDebugString(), rpc->Response()->ShortDebugString());
  Status s;
  const auto* response = rpc->Response();
  if (!status.ok()) {
    DINGO_LOG(WARNING) << fmt::format("[sdk.txn.{}] rpc: {} send to region: {} fail: {}", txn_impl_->ID(),
                                      rpc->Method(), rpc->Request()->context().region_id(), status.ToString());

    s = status;
  } else {
    s = ProcessTxnCommitResponse(response, is_primary_);
    if (!s.ok()) {
      DINGO_LOG(WARNING) << fmt::format("[sdk.txn.{}] commit fail, region({}) response({}) status({}).",
                                        txn_impl_->ID(), rpc->Request()->context().region_id(),
                                        response->ShortDebugString(), s.ToString());
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
    bool tmp_need_retry = false;
    {
      ReadLockGuard guard(rw_lock_);
      tmp = status_;
    }
    DoAsyncDone(tmp);
  }
}

Status TxnCommitTask::ProcessTxnCommitResponse(const TxnCommitResponse* response, bool is_primary) {
  std::string pk = txn_impl_->GetPrimaryKey();
  int64_t txn_id = txn_impl_->ID();
  DINGO_LOG(DEBUG) << fmt::format("[sdk.txn.{}] commit response, pk({}) response({}).", txn_id, pk,
                                  response->ShortDebugString());

  if (!response->has_txn_result()) {
    return Status::OK();
  }

  const auto& txn_result = response->txn_result();
  if (txn_result.has_locked()) {
    const auto& lock_info = txn_result.locked();
    DINGO_LOG(WARNING) << fmt::format("[sdk.txn.{}] commit lock conflict, is_primary({}) pk({}) response({}).", txn_id,
                                      is_primary, StringToHex(pk), response->ShortDebugString());

  } else if (txn_result.has_txn_not_found()) {
    DINGO_LOG(FATAL) << fmt::format("[sdk.txn.{}] commit not found, is_primary({}) pk({}) response({}).", txn_id,
                                    is_primary, StringToHex(pk), response->ShortDebugString());

  } else if (txn_result.has_write_conflict()) {
    if (!is_primary) {
      DINGO_LOG(WARNING) << fmt::format("[sdk.txn.{}] commit write conlict, pk({}) response({}).", txn_id,
                                      StringToHex(pk), txn_result.write_conflict().ShortDebugString());
    }
    return Status::TxnWriteConflict("txn write conflict");

  } else if (txn_result.has_commit_ts_expired()) {
    DINGO_LOG(WARNING) << fmt::format("[sdk.txn.{}] commit ts expired, is_primary({}) pk({}) response({}).", txn_id,
                                      is_primary, StringToHex(pk), txn_result.commit_ts_expired().ShortDebugString());
    if (is_primary) {
      return Status::TxnCommitTsExpired("txn commit ts expired");
    }
  } else if (txn_result.has_primary_mismatch()) {
    DINGO_LOG(WARNING) << fmt::format("[sdk.txn.{}] commit primary mismatch, is_primary({}) pk({}) response({}).",
                                      txn_id, is_primary, StringToHex(pk),
                                      txn_result.primary_mismatch().ShortDebugString());
    return Status::TxnPrimaryMismatch("txn primary mismatch");
  }

  return Status::OK();
}

}  // namespace sdk

}  // namespace dingodb