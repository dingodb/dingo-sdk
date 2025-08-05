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

#include "sdk/transaction/txn_task/txn_get_task.h"

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
void TxnGetTask::DoAsync() {
  auto meta_cache = stub.GetMetaCache();
  RegionPtr region;
  Status s = meta_cache->LookupRegionByKey(key_, region);
  if (!s.ok()) {
    DoAsyncDone(s);
    return;
  }

  rpc_.MutableRequest()->Clear();
  rpc_.MutableRequest()->set_start_ts(txn_impl_->GetStartTs());
  rpc_.MutableRequest()->set_key(key_);
  FillRpcContext(*rpc_.MutableRequest()->mutable_context(), region->RegionId(), region->GetEpoch(), {resolved_lock_},
                 ToIsolationLevel(txn_impl_->GetOptions().isolation));

  store_rpc_controller_.ResetRegion(region);
  store_rpc_controller_.AsyncCall([this](auto&& s) { TxnGetRpcCallback(std::forward<decltype(s)>(s)); });
}

void TxnGetTask::TxnGetRpcCallback(const Status& status) {
  DINGO_LOG(DEBUG) << "rpc : " << rpc_.Method() << " request : " << rpc_.Request()->ShortDebugString()
                   << " response : " << rpc_.Response()->ShortDebugString();
  const auto* response = rpc_.Response();
  if (!status.ok()) {
    DINGO_LOG(WARNING) << "rpc: " << rpc_.Method() << " send to region: " << rpc_.Request()->context().region_id()
                       << " fail: " << status.ToString();
    status_ = status;
  } else {
    if (response->has_txn_result()) {
      auto status1 = CheckTxnResultInfo(response->txn_result());
      if (status1.IsTxnLockConflict()) {
        status1 = stub.GetTxnLockResolver()->ResolveLock(response->txn_result().locked(), txn_impl_->GetStartTs());
        // retry
        if (status1.ok()) {
          // need to retry
          DoAsyncRetry();
          return;
        } else if (status.IsPushMinCommitTs()) {
          resolved_lock_ = response->txn_result().locked().lock_ts();
          DoAsyncRetry();
          return;
        }
      } else if (!status1.ok()) {
        DINGO_LOG(WARNING) << fmt::format("[sdk.txn.{}] get fail, key({}) status({}), txn_result({}).", txn_impl_->ID(),
                                          StringToHex(rpc_.Request()->key()), status1.ToString(),
                                          response->txn_result().ShortDebugString());
      }

      status_ = status1;
    }
  }

  if (status_.ok()) {
    if (response->value().empty()) {
      status_ = Status::NotFound(fmt::format("key:{} not found", key_));
    } else {
      value_ = response->value();
    }
  }

  DoAsyncDone(status_);
}

}  // namespace sdk

}  // namespace dingodb