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
#include "sdk/rpc/store_rpc_controller.h"

#include <fmt/format.h>

#include <string>
#include <utility>

#include "common/logging.h"
#include "dingosdk/status.h"
#include "fmt/core.h"
#include "glog/logging.h"
#include "proto/common.pb.h"
#include "proto/error.pb.h"
#include "sdk/client_stub.h"
#include "sdk/common/common.h"
#include "sdk/common/helper.h"
#include "sdk/common/param_config.h"
#include "sdk/utils/async_util.h"

namespace dingodb {
namespace sdk {

StoreRpcController::StoreRpcController(const ClientStub& stub, Rpc& rpc, RegionPtr region)
    : stub_(stub), rpc_(rpc), region_(std::move(region)), rpc_retry_times_(0) {}

StoreRpcController::StoreRpcController(const ClientStub& stub, Rpc& rpc)
    : stub_(stub), rpc_(rpc), region_(nullptr), rpc_retry_times_(0) {}

StoreRpcController::~StoreRpcController() = default;

Status StoreRpcController::Call() {
  Status ret;
  Synchronizer sync;
  AsyncCall(sync.AsStatusCallBack(ret));
  sync.Wait();

  return ret;
}

void StoreRpcController::AsyncCall(StatusCallback cb) {
  call_back_.swap(cb);
  DoAsyncCall();
}

void StoreRpcController::DoAsyncCall() {
  if (!PreCheck()) {
    FireCallback();
    return;
  }

  if (!PrepareRpc()) {
    FireCallback();
    return;
  }

  SendStoreRpc();
}

bool StoreRpcController::PreCheck() {
  if (region_->IsStale()) {
    status_ =
        Status::Incomplete(pb::error::Errno::EREGION_VERSION, fmt::format("region:{} is stale", region_->RegionId()));
    DINGO_LOG(INFO) << fmt::format("[sdk.rpc.{}]method:{} , store rpc fail, status({}).", rpc_.LogId(), rpc_.Method(),
                                   status_.ToString());
    return false;
  }
  return true;
}

bool StoreRpcController::PrepareRpc() {
  if (NeedPickLeader()) {
    EndPoint next_leader;
    if (!PickNextLeader(next_leader)) {
      status_ = Status::Aborted("not found leader");
      return false;
    }

    CHECK(next_leader.IsValid());
    rpc_.SetEndPoint(next_leader);
  }

  rpc_.Reset();

  return true;
}

void StoreRpcController::SendStoreRpc() {
  CHECK(region_.get() != nullptr) << "region should not nullptr.";

  MaybeDelay();
  stub_.GetRpcClient()->SendRpc(rpc_, [this] { SendStoreRpcCallBack(); });
}

void StoreRpcController::MaybeDelay() {
  if (NeedDelay(status_)) {
    auto delay_ms = FLAGS_store_rpc_retry_delay_ms;
    SleepUs(delay_ms * 1000);
  }
}

void StoreRpcController::SendStoreRpcCallBack() {
  Status status = rpc_.GetStatus();
  if (!status.ok()) {
    region_->MarkFollower(rpc_.GetEndPoint());
    DINGO_LOG(WARNING) << fmt::format("[sdk.rpc.{}] method:{} ,connect to store fail, region({}) status({}).",
                                      rpc_.LogId(), rpc_.Method(), region_->RegionId(), status.ToString());
    status_ = status;
    RetrySendRpcOrFireCallback();
    return;
  }
  
  auto end_point = rpc_.GetEndPoint();
  region_->MarkLeader(end_point);

  auto error = GetRpcResponseError(rpc_);
  if (error.errcode() == pb::error::Errno::OK) {
    status_ = Status::OK();
    RetrySendRpcOrFireCallback();
    return;
  }

  std::string msg = fmt::format("[sdk.rpc.{}] method:{} region:{} endpoint:{}, error({} {})", rpc_.LogId(),
                                rpc_.Method(), region_->RegionId(), rpc_.GetEndPoint().ToString(),
                                pb::error::Errno_Name(error.errcode()), error.errmsg());

  if (error.errcode() == pb::error::Errno::ERAFT_NOTLEADER) {
    region_->MarkFollower(rpc_.GetEndPoint());
    if (error.has_leader_location()) {
      auto endpoint = LocationToEndPoint(error.leader_location());
      if (!endpoint.IsValid()) {
        msg += fmt::format(", leader({}) is invalid", endpoint.ToString());
        status_ = Status::NoLeader(error.errcode(), error.errmsg());
      } else {
        region_->MarkLeader(endpoint);
        msg += fmt::format(", leader({}).", endpoint.ToString());
        status_ = Status::NotLeader(error.errcode(), error.errmsg());
      }
    } else {
      status_ = Status::NoLeader(error.errcode(), error.errmsg());
    }

  } else if (error.errcode() == pb::error::EREGION_VERSION) {
    stub_.GetMetaCache()->ClearRegion(region_);
    if (error.has_store_region_info()) {
      auto region = ProcessStoreRegionInfo(error.store_region_info());
      if (region->GetRange().start_key >= region->GetRange().end_key) {
        DINGO_LOG(WARNING) << fmt::format("[sdk.rpc.{}] region range is invaild, request:{} response:{}", rpc_.LogId(),
                                          rpc_.RawRequest()->ShortDebugString(),
                                          rpc_.RawResponse()->ShortDebugString());
      } else {
        stub_.GetMetaCache()->MaybeAddRegion(region);
      }
      msg += fmt::format(", region version({}).", region->DescribeEpoch());
    }
    status_ = Status::Incomplete(error.errcode(), error.errmsg());

  } else if (error.errcode() == pb::error::Errno::EREGION_NOT_FOUND) {
    stub_.GetMetaCache()->ClearRegion(region_);
    status_ = Status::Incomplete(error.errcode(), error.errmsg());

  } else if (error.errcode() == pb::error::Errno::EKEY_OUT_OF_RANGE) {
    stub_.GetMetaCache()->ClearRegion(region_);
    status_ = Status::Incomplete(error.errcode(), error.errmsg());

  } else if (error.errcode() == pb::error::Errno::EREQUEST_FULL) {
    status_ = Status::RemoteError(error.errcode(), error.errmsg());

  } else if (error.errcode() == pb::error::Errno::ETXN_MEMORY_LOCK_CONFLICT) {
    status_ = Status::TxnMemLockConflict(error.errcode(), error.errmsg());
  } else if (error.errcode() == pb::error::Errno::ERAFT_NOT_CONSISTENT_READ) {
    DINGO_LOG(WARNING) << fmt::format("[sdk.rpc.{}] method:{} , raft not consistent read, region({}) status({}).",
                                      rpc_.LogId(), rpc_.Method(), region_->RegionId(), error.errmsg());
    status_ = Status::RaftNotConsistentRead(error.errcode(), error.errmsg());
  } else if (error.errcode() == pb::error::Errno::ERAFT_COMMITLOG) {
    DINGO_LOG(WARNING) << fmt::format("[sdk.rpc.{}] method:{} , raft commit log error, region({}) status({}).",
                                      rpc_.LogId(), rpc_.Method(), region_->RegionId(), error.errmsg());
    status_ = Status::RaftCommitLog(error.errcode(), error.errmsg());
  } else {
    // NOTE: other error we not clean cache, caller decide how to process
    status_ = Status::Incomplete(error.errcode(), error.errmsg());
  }

  DINGO_LOG(WARNING) << msg;

  RetrySendRpcOrFireCallback();
}

void StoreRpcController::RetrySendRpcOrFireCallback() {
  if (rpc_.Method() == "StoreService.TxnPrewriteRpc") {
    DINGO_LOG(DEBUG) << fmt::format("[sdk.rpc.{}]method:{} , store rpc done, region({}) status({}).", rpc_.LogId(),
                                    rpc_.Method(), region_->RegionId(), status_.ToString());
    // prewrite task execute retry
    FireCallback();
    return;
  }

  if (!status_.IsOK() && (IsUniversalNeedRetryError(status_) || IsTxnNeedRetryError(status_))) {
    if (rpc_retry_times_ < FLAGS_store_rpc_max_retry) {
      rpc_retry_times_++;
      DoAsyncCall();
      return;

    } else {
      status_ = Status::Aborted("rpc retry times exceed");
    }
  }

  FireCallback();
}

void StoreRpcController::FireCallback() {
  if (!status_.ok()) {
    DINGO_LOG(WARNING) << fmt::format("[sdk.rpc.{}] method:{} , rpc fail, region({}) retry({}) status({}).",
                                      rpc_.LogId(), rpc_.Method(), region_->RegionId(), rpc_retry_times_,
                                      status_.ToString());
  }

  if (call_back_) {
    StatusCallback cb;
    call_back_.swap(cb);
    cb(status_);
  }
}

bool StoreRpcController::PickNextLeader(EndPoint& leader) {
  EndPoint tmp_leader;
  region_->GetLeader(tmp_leader);
  leader = tmp_leader;
  return true;
}

void StoreRpcController::ResetRegion(RegionPtr region) {
  if (region_) {
    if (!(EpochCompare(region_->GetEpoch(), region->GetEpoch()) > 0)) {
      DINGO_LOG(WARNING) << fmt::format("[sdk.rpc.{}] method:{} reset region fail, epoch not match, {} {}.",
                                        rpc_.LogId(), rpc_.Method(), region->DescribeEpoch(), region_->DescribeEpoch());
    }
  }
  region_.reset();
  region_ = std::move(region);
}

RegionPtr StoreRpcController::ProcessStoreRegionInfo(const pb::error::StoreRegionInfo& store_region_info) {
  CHECK_NOTNULL(region_);
  CHECK(store_region_info.has_current_region_epoch());
  CHECK(store_region_info.has_current_range());
  auto id = store_region_info.region_id();
  CHECK(id == region_->RegionId());

  std::vector<Replica> replicas;
  for (const auto& peer : store_region_info.peers()) {
    CHECK(peer.has_server_location());
    auto endpoint = LocationToEndPoint(peer.server_location());
    CHECK(endpoint.IsValid()) << fmt::format("[sdk.rpc.{}]endpoint is invalid, endpoint: {}", rpc_.LogId(),
                                             endpoint.ToString());
    replicas.push_back({endpoint, kFollower});
  }

  RegionPtr region =
      std::make_shared<Region>(id, store_region_info.current_range(), store_region_info.current_region_epoch(),
                               RegionTypeToPBRegionType(region_->GetRegionType()), replicas);

  EndPoint leader;
  if (region_->GetLeader(leader).IsOK()) {
    region->MarkLeader(leader);
  }

  return region;
}

}  // namespace sdk
}  // namespace dingodb