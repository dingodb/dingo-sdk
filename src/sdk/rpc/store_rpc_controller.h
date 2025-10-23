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

#ifndef DINGODB_SDK_STORE_RPC_CONTROLLER_H_
#define DINGODB_SDK_STORE_RPC_CONTROLLER_H_

#include "dingosdk/status.h"
#include "proto/error.pb.h"
#include "sdk/client_stub.h"
#include "sdk/utils/callback.h"
#include "sdk/utils/net_util.h"

namespace dingodb {
namespace sdk {

// TODO: support backoff strategy
class StoreRpcController {
 public:
  explicit StoreRpcController(const ClientStub& stub, Rpc& rpc);

  explicit StoreRpcController(const ClientStub& stub, Rpc& rpc, RegionPtr region);

  virtual ~StoreRpcController();

  // TODO: to remove
  Status Call();

  void AsyncCall(StatusCallback cb);

  void ResetRegion(RegionPtr region);

  static bool IsUniversalNeedRetryError(const Status& status) {
    return status.IsNetworkError() || status.IsRemoteError() || status.IsNotLeader() || status.IsNoLeader() ||
           status.IsRaftNotConsistentRead() || status.IsRaftCommitLog();
  }

  static bool IsTxnNeedRetryError(const Status& status) { return status.IsTxnMemLockConflict(); }

  static bool NeedDelay(const Status& status) {
    return status.IsRemoteError() || status.IsNoLeader() || status.IsTxnMemLockConflict() || status.IsNetworkError() ||
           status.IsNotLeader();
  }

 private:
  void DoAsyncCall();

  // send rpc flow
  bool PreCheck();
  bool PrepareRpc();
  void SendStoreRpc();
  void SendStoreRpcCallBack();
  void RetrySendRpcOrFireCallback();
  void FireCallback();

  // backoff
  void MaybeDelay();
  bool PickNextLeader(EndPoint& leader);

  RegionPtr ProcessStoreRegionInfo(const pb::error::StoreRegionInfo& store_region_info);

  // below funciton only works for store rpc controller
  bool NeedPickLeader() {
    return !rpc_.GetEndPoint().IsValid() || status_.IsNetworkError() || status_.IsNotLeader() || status_.IsNoLeader();
  }

  // above funciton only works for store rpc controller

  static const pb::error::Error& GetResponseError(Rpc& rpc);

  const ClientStub& stub_;
  Rpc& rpc_;
  RegionPtr region_;
  int rpc_retry_times_;
  int next_replica_index_;
  Status status_;
  StatusCallback call_back_;
};

}  // namespace sdk
}  // namespace dingodb

#endif  // DINGODB_SDK_STORE_RPC_CONTROLLER_H_