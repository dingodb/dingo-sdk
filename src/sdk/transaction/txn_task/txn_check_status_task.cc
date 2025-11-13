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

#include "sdk/transaction/txn_task/txn_check_status_task.h"

#include <fmt/format.h>

#include <cstdint>

#include "common/logging.h"
#include "dingosdk/status.h"
#include "proto/store.pb.h"
#include "sdk/common/common.h"
#include "sdk/common/helper.h"
#include "sdk/region.h"
#include "sdk/rpc/store_rpc_controller.h"
#include "sdk/transaction/txn_common.h"
#include "sdk/transaction/txn_lock_resolver.h"
#include "sdk/utils/callback.h"

namespace dingodb {
namespace sdk {
void TxnCheckStatusTask::DoAsync() {
  status_ = Status::OK();
  auto meta_cache = stub.GetMetaCache();
  RegionPtr region;

  Status s = meta_cache->LookupRegionByKey(primary_key_, region);
  if (!s.ok()) {
    DoAsyncDone(s);
    return;
  }

  int64_t current_ts;
  s = stub.GetTsoProvider()->GenTs(2, current_ts);
  if (!s.ok()) {
    DoAsyncDone(s);
    return;
  }

  FillRpcContext(*rpc_.MutableRequest()->mutable_context(), region->RegionId(), region->GetEpoch(),
                 pb::store::IsolationLevel::SnapshotIsolation);
  rpc_.MutableRequest()->set_primary_key(primary_key_);
  rpc_.MutableRequest()->set_lock_ts(lock_ts_);
  rpc_.MutableRequest()->set_caller_start_ts(start_ts_);
  rpc_.MutableRequest()->set_current_ts(current_ts);
  rpc_.MutableRequest()->set_force_sync_commit(force_sync_commit_);

  store_rpc_controller_.ResetRegion(region);
  store_rpc_controller_.AsyncCall([this](auto&& s) { TxnCheckStatusRpcCallback(std::forward<decltype(s)>(s)); });
}

void TxnCheckStatusTask::TxnCheckStatusRpcCallback(const Status& status) {
  DINGO_LOG(DEBUG) << fmt::format("[sdk.txn.{}] rpc: {} request: {} response: {}", start_ts_, rpc_.Method(),
                                  rpc_.Request()->ShortDebugString(), rpc_.Response()->ShortDebugString());
  const auto* response = rpc_.Response();
  pb::store::LockInfo lock_info;
  if (!status.ok()) {
    DINGO_LOG(WARNING) << fmt::format("[sdk.txn.{}] rpc: {} send to region: {} fail: {}", start_ts_, rpc_.Method(),
                                      rpc_.Request()->context().region_id(), status.ToString());
    status_ = status;
  } else {
    if (response->has_txn_result()) {
      Status status = CheckTxnResultInfo(response->txn_result());
      const auto txn_result = response->txn_result();
      if (txn_result.has_locked()) {
        lock_info = txn_result.locked();
      }
      if (!status.ok()) {
        DINGO_LOG(WARNING) << fmt::format(
            "[sdk.txn.{}] check status fail, primary_key({}), lock_ts({}), start_ts({}),  status({}), "
            "txn_result({}).",
            start_ts_, StringToHex(primary_key_), rpc_.Request()->lock_ts(), rpc_.Request()->caller_start_ts(),
            status.ToString(), txn_result.ShortDebugString());
        if (!status.IsTxnLockConflict()) {
          status_ = status;
        }
      }
    }
  }

  if (status_.ok()) {
    txn_status_ = TxnStatus(response->lock_ttl(), response->commit_ts(), response->action(), lock_info);
  }

  DoAsyncDone(status_);
}

}  // namespace sdk

}  // namespace dingodb