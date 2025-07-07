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

#include "sdk/transaction/txn_lock_resolver.h"

#include <fmt/format.h>

#include <cstdint>

#include "common/logging.h"
#include "dingosdk/status.h"
#include "glog/logging.h"
#include "sdk/client_stub.h"
#include "sdk/common/common.h"
#include "sdk/common/helper.h"
#include "sdk/region.h"
#include "sdk/rpc/store_rpc.h"
#include "sdk/rpc/store_rpc_controller.h"

namespace dingodb {
namespace sdk {

TxnLockResolver::TxnLockResolver(const ClientStub& stub) : stub_(stub) {}

// TODO: maybe support retry
Status TxnLockResolver::ResolveLock(const pb::store::LockInfo& lock_info, int64_t start_ts) {
  DINGO_LOG(INFO) << fmt::format("[sdk.txn.{}] resolve lock, lock_info({}).", start_ts, lock_info.ShortDebugString());

  // check primary key lock status
  TxnStatus txn_status;
  Status status = CheckTxnStatus(lock_info.lock_ts(), lock_info.primary_lock(), start_ts, txn_status);
  if (!status.ok()) {
    if (status.IsNotFound()) {
      DINGO_LOG(DEBUG) << fmt::format("[sdk.txn.{}] not exist txn when check status, status({}) lock({}).",
                                      status.ToString(), lock_info.ShortDebugString());

      return Status::OK();
    } else {
      return status;
    }
  }

  if (txn_status.IsMinCommitTSPushed()) {
    return Status::PushMinCommitTs("push min_commit_ts");
  }

  // primary key exist lock then outer txn rollback
  if (txn_status.IsLocked()) {
    return Status::TxnLockConflict(status.ToString());
  }

  CHECK(txn_status.IsCommitted() || txn_status.IsRollbacked()) << "unexpected txn_status:" << txn_status.ToString();

  // resolve conflict ordinary key
  status = ResolveLockKey(lock_info.lock_ts(), lock_info.key(), txn_status.commit_ts);
  if (!status.IsOK()) {
    DINGO_LOG(WARNING) << fmt::format("[sdk.txn.{}] resolve lock fail, lock_ts({}) key({}) txn_status({}) status({}).",
                                      start_ts, lock_info.lock_ts(), lock_info.key(), txn_status.ToString(),
                                      status.ToString());
    return status;
  }

  return Status::OK();
}

// TODO: use txn status cache
Status TxnLockResolver::CheckTxnStatus(int64_t lock_ts, const std::string& primary_key, int64_t start_ts,
                                       TxnStatus& txn_status) {
  std::shared_ptr<Region> region;
  DINGO_RETURN_NOT_OK(stub_.GetMetaCache()->LookupRegionByKey(primary_key, region));

  int64_t current_ts;
  DINGO_RETURN_NOT_OK(stub_.GetAdminTool()->GetCurrentTimeStamp(current_ts));

  TxnCheckTxnStatusRpc rpc;

  // NOTE: use randome isolation is ok?
  FillRpcContext(*rpc.MutableRequest()->mutable_context(), region->RegionId(), region->Epoch(),
                 pb::store::IsolationLevel::SnapshotIsolation);
  rpc.MutableRequest()->set_primary_key(primary_key);
  rpc.MutableRequest()->set_lock_ts(lock_ts);
  rpc.MutableRequest()->set_caller_start_ts(start_ts);
  rpc.MutableRequest()->set_current_ts(current_ts);

  StoreRpcController controller(stub_, rpc, region);
  DINGO_RETURN_NOT_OK(controller.Call());

  const auto& response = *rpc.Response();
  DINGO_LOG(INFO) << fmt::format("[sdk.txn.{}] check txn status response: {}.", start_ts, response.ShortDebugString());

  return ProcessTxnCheckStatusResponse(response, txn_status);
}

Status TxnLockResolver::ProcessTxnCheckStatusResponse(const pb::store::TxnCheckTxnStatusResponse& response,
                                                      TxnStatus& txn_status) {
  if (response.has_txn_result()) {
    const auto& txn_result = response.txn_result();
    if (txn_result.has_txn_not_found()) {
      const auto& not_found = txn_result.txn_not_found();
      return Status::NotFound(fmt::format("start_ts({}) pk({}) key({})", not_found.start_ts(),
                                          StringToHex(not_found.primary_key()), StringToHex(not_found.key())));

    } else if (txn_result.has_primary_mismatch()) {
      return Status::IllegalState("not match primary key");
    }
  }

  txn_status = TxnStatus(response.lock_ttl(), response.commit_ts(), response.action());
  return Status::OK();
}

Status TxnLockResolver::ResolveLockKey(int64_t lock_ts, const std::string& key, int64_t commit_ts) {
  std::shared_ptr<Region> region;
  DINGO_RETURN_NOT_OK(stub_.GetMetaCache()->LookupRegionByKey(key, region));

  TxnResolveLockRpc rpc;
  // NOTE: use randome isolation is ok?
  FillRpcContext(*rpc.MutableRequest()->mutable_context(), region->RegionId(), region->Epoch(),
                 pb::store::IsolationLevel::SnapshotIsolation);
  rpc.MutableRequest()->set_start_ts(lock_ts);
  rpc.MutableRequest()->set_commit_ts(commit_ts);
  *rpc.MutableRequest()->add_keys() = key;

  StoreRpcController controller(stub_, rpc, region);
  DINGO_RETURN_NOT_OK(controller.Call());

  return ProcessTxnResolveLockResponse(*rpc.Response());
}

Status TxnLockResolver::ProcessTxnResolveLockResponse(const pb::store::TxnResolveLockResponse& response) {
  // TODO: need to process lockinfo when support permissive txn
  if (response.has_error() || response.has_txn_result()) {
    DINGO_LOG(INFO) << fmt::format("[sdk.txn] txn_resolve_lock_response: {}.", response.ShortDebugString());
  }
  return Status::OK();
}

}  // namespace sdk
}  // namespace dingodb