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

#ifndef DINGODB_SDK_RAW_KV_BATCH_GET_TASK_H_
#define DINGODB_SDK_RAW_KV_BATCH_GET_TASK_H_

#include <atomic>
#include <shared_mutex>
#include <string_view>

#include "dingosdk/client.h"
#include "sdk/client_stub.h"
#include "sdk/rawkv/raw_kv_task.h"
#include "sdk/rpc/store_rpc.h"
#include "sdk/rpc/store_rpc_controller.h"
#include "sdk/utils/rw_lock.h"

namespace dingodb {
namespace sdk {

class RawKvBatchGetTask : public RawKvTask {
 public:
  RawKvBatchGetTask(const ClientStub& stub, const std::vector<std::string>& keys, std::vector<KVPair>& out_kvs);

  ~RawKvBatchGetTask() override = default;

 private:
  Status Init() override;
  void DoAsync() override;
  void PostProcess() override;

  std::string Name() const override { return "RawKvBatchGetTask"; }

  void BatchGetRpcCallback(const Status& status, KvBatchGetRpc* rpc);

  const std::vector<std::string>& keys_;
  std::vector<KVPair>& out_kvs_;

  std::vector<StoreRpcController> controllers_;
  std::vector<std::unique_ptr<KvBatchGetRpc>> rpcs_;

  RWLock rw_lock_;
  std::vector<KVPair> tmp_out_kvs_;
  std::set<std::string_view> next_keys_;
  Status status_;

  std::atomic<int> sub_tasks_count_;
};

}  // namespace sdk
}  // namespace dingodb

#endif  // DINGODB_SDK_RAW_KV_BATCH_GET_TASK_H_