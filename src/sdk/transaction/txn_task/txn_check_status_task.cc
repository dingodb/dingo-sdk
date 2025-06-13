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

#include <cstdint>

#include "common/logging.h"
#include "dingosdk/status.h"
#include "sdk/common/common.h"
#include "sdk/common/helper.h"
#include "sdk/region.h"
#include "sdk/rpc/store_rpc_controller.h"
#include "sdk/transaction/txn_lock_resolver.h"
#include "sdk/utils/callback.h"

namespace dingodb {
namespace sdk {
void TxnCheckStatusTask::DoAsync() {
  auto meta_cache = stub.GetMetaCache();
  RegionPtr region;

  Status s = meta_cache->LookupRegionByKey(primary_key_, region);
  if (!s.ok()) {
    DoAsyncDone(s);
    return;
  }

  int64_t current_ts;
  s = stub.GetAdminTool()->GetCurrentTimeStamp(current_ts);
  if (!s.ok()) {
    DoAsyncDone(s);
    return;
  }

  FillRpcContext(*rpc_.MutableRequest()->mutable_context(), region->RegionId(), region->Epoch(),
                 pb::store::IsolationLevel::SnapshotIsolation);
  rpc_.MutableRequest()->set_primary_key(primary_key_);
  rpc_.MutableRequest()->set_lock_ts(lock_ts_);
  rpc_.MutableRequest()->set_caller_start_ts(start_ts_);
  rpc_.MutableRequest()->set_current_ts(current_ts);

  store_rpc_controller_.ResetRegion(region);
  store_rpc_controller_.AsyncCall([this](auto&& s) { TxnCheckStatusRpcCallback(std::forward<decltype(s)>(s)); });
}

void TxnCheckStatusTask::TxnCheckStatusRpcCallback(const Status& status) {
  const auto* response = rpc_.Response();
  if (!status.ok()) {
    DINGO_LOG(WARNING) << "rpc: " << rpc_.Method() << " send to region: " << rpc_.Request()->context().region_id()
                       << " fail: " << status.ToString();
    status_ = status;
  } else {
    if (response->has_txn_result()) {
      const auto txn_result = response->txn_result();
      if (txn_result.has_txn_not_found()) {
        const auto& not_found = txn_result.txn_not_found();
        status_ = Status::NotFound(fmt::format("start_ts({}) pk({}) key({})", not_found.start_ts(),
                                               StringToHex(not_found.primary_key()), StringToHex(not_found.key())));
      } else if (txn_result.has_primary_mismatch()) {
        status_ = Status::IllegalState("not match primary key");
      } else {
        DINGO_LOG(WARNING) << fmt::format("[sdk.txn.{}] check txn status response: {}.", start_ts_,
                                          response->ShortDebugString());
      }
    }
  }

  if (status_.ok()) {
    txn_status_ = TxnStatus(response->lock_ttl(), response->commit_ts(), response->action());
  }

  DoAsyncDone(status_);
}

}  // namespace sdk

}  // namespace dingodb