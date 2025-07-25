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
void TxnResolveLockTask::DoAsync() {
  auto meta_cache = stub.GetMetaCache();
  RegionPtr region;

  Status s = meta_cache->LookupRegionByKey(key_, region);
  if (!s.ok()) {
    DoAsyncDone(s);
    return;
  }

  FillRpcContext(*rpc_.MutableRequest()->mutable_context(), region->RegionId(), region->Epoch(),
                 pb::store::IsolationLevel::SnapshotIsolation);
  rpc_.MutableRequest()->set_start_ts(lock_ts_);
  rpc_.MutableRequest()->set_commit_ts(commit_ts_);
  *rpc_.MutableRequest()->add_keys() = key_;

  store_rpc_controller_.ResetRegion(region);
  store_rpc_controller_.AsyncCall([this](auto&& s) { TxnResolveLockRpcCallback(std::forward<decltype(s)>(s)); });
}

void TxnResolveLockTask::TxnResolveLockRpcCallback(const Status& status) {
  DINGO_LOG(DEBUG) << "rpc : " << rpc_.Method() << " request : " << rpc_.Request()->ShortDebugString()
                   << " response : " << rpc_.Response()->ShortDebugString();
  const auto* response = rpc_.Response();
  if (!status.ok()) {
    DINGO_LOG(WARNING) << "rpc: " << rpc_.Method() << " send to region: " << rpc_.Request()->context().region_id()
                       << " fail: " << status.ToString();
    status_ = status;
  } else {
    if (rpc_.Response()->has_txn_result()) {
      status_ = CheckTxnResultInfo(rpc_.Response()->txn_result());
      if (!status_.ok()) {
        DINGO_LOG(WARNING) << fmt::format(
            "[sdk.txn] txn_resolve_lock fail, lock_ts: {},  primary_key: {}, response: {}, status: {}.", lock_ts_,
            StringToHex(key_), response->ShortDebugString(), status_.ToString());
      }
    }
  }

  DoAsyncDone(status_);
}

}  // namespace sdk

}  // namespace dingodb