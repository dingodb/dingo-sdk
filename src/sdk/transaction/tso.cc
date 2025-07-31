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

#include "sdk/transaction/tso.h"

#include "dingosdk/status.h"
#include "fmt/format.h"
#include "sdk/client_stub.h"
#include "sdk/common/common.h"
#include "sdk/common/helper.h"
#include "sdk/rpc/coordinator_rpc.h"
#include "sdk/utils/async_util.h"
#include "sdk/utils/rw_lock.h"

namespace dingodb {
namespace sdk {

static const uint32_t kStalePeriodUs = 1000;  // 1000 microseconds
static const uint32_t kBatchSize = 256;       // default batch size

static bool IsNeedRetry(int& times) {
  bool retry = times++ < FLAGS_txn_op_max_retry;
  if (retry) {
    SleepUs(500);
  }

  return retry;
}

TsoProvider::TsoProvider(const ClientStub& stub) : stub_(stub), batch_size_(kBatchSize) {
  last_time_us_ = TimestampUs();
}

Status TsoProvider::GenTs(uint32_t count, int64_t& ts) {
  // lock
  WriteLockGuard guard(rwlock_);
  int retry = 0;
  Status status;
  do {
    if (max_logical_ >= count + next_logical_ && !IsStale()) {
      TsoTimestamp tso;
      tso.set_physical(physical_);
      tso.set_logical(next_logical_);

      ts = Tso2Timestamp(tso);

      next_logical_ += count;

      return Status::OK();
    }

    status = FetchTso(batch_size_);

  } while (IsNeedRetry(retry));

  DINGO_LOG(ERROR) << fmt::format("[sdk.tso] gen ts fail, retry({}), status({}).", retry, status.ToString());

  return status;
}

Status TsoProvider::GenPhysicalTs(int32_t count, int64_t& physical_ts) {
  // lock
  WriteLockGuard guard(rwlock_);
  // for txn heartbeat, we need to re-acquire physical ts
  Refresh();

  int retry = 0;
  Status status;
  do {
    status = FetchTso(batch_size_);

    if (max_logical_ >= count + next_logical_ && !IsStale()) {
      physical_ts = physical_;

      next_logical_ += count;

      return Status::OK();
    }

  } while (IsNeedRetry(retry));

  DINGO_LOG(ERROR) << fmt::format("[sdk.tso] gen ts fail, retry({}), status({}).", retry, status.ToString());

  return status;
}

void TsoProvider::Refresh() {
  last_time_us_ = TimestampUs();
  physical_ = 0;
  next_logical_ = 0;
  max_logical_ = 0;
}

bool TsoProvider::IsStale() {
  auto now_us = TimestampUs();
  bool is_stale = now_us > (last_time_us_ + kStalePeriodUs);
  if (is_stale) last_time_us_ = now_us;

  return is_stale;
}

Status TsoProvider::FetchTso(uint32_t count) {
  TsoServiceRpc rpc;
  rpc.MutableRequest()->set_op_type(pb::meta::TsoOpType::OP_GEN_TSO);
  rpc.MutableRequest()->set_count(count);

  auto status = stub_.GetMetaRpcController()->SyncCall(rpc);
  if (!status.IsOK()) {
    DINGO_LOG(ERROR) << fmt::format("[sdk.tso] fetch tso fail, status({}).", status.ToString());
    return status;
  }

  CHECK(rpc.Response()->has_start_timestamp()) << "tso response should has start_timestamp.";

  const auto& tso = rpc.Response()->start_timestamp();
  const auto& ts_count = rpc.Response()->count();
  physical_ = tso.physical();
  next_logical_ = tso.logical();
  max_logical_ = next_logical_ + ts_count - 1;

  DINGO_LOG(DEBUG) << fmt::format("[sdk.tso] fetch tso ts({}) count({}).", Tso2Timestamp(tso), ts_count);

  return Status::OK();
}

}  // namespace sdk
}  // namespace dingodb