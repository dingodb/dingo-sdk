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

#include <mutex>

#include "dingosdk/status.h"
#include "fmt/format.h"
#include "sdk/client_stub.h"
#include "sdk/common/common.h"
#include "sdk/common/helper.h"
#include "sdk/common/param_config.h"
#include "sdk/utils/rw_lock.h"

namespace dingodb {
namespace sdk {

TsoProvider::TsoProvider(const ClientStub& stub) : stub_(stub), batch_size_(FLAGS_tso_batch_size) {
  last_time_us_ = TimestampUs();
}

// caller must hold the write lock
bool TsoProvider::TryAllocate(uint32_t count, int64_t& ts, bool check_stale) {
  if ((check_stale && IsStale()) || max_logical_ < count + next_logical_) return false;

  TsoTimestamp tso;
  tso.set_physical(physical_);
  tso.set_logical(next_logical_);
  ts = Tso2Timestamp(tso);

  next_logical_ += count;
  CHECK(ts > 0) << "ts should be greater than 0 , ts:" << ts;

  return true;
}

Status TsoProvider::GenTs(uint32_t count, int64_t& ts) {
  Status status;

  int retry = 0;
  bool check_stale = true;
  do {
    {
      WriteLockGuard guard(rwlock_);
      if (TryAllocate(count, ts, check_stale)) return Status::OK();
    }

    // the coordinator round trip runs outside rwlock_, so callers that can
    // still serve from the cached batch are not blocked behind it
    status = RefillBatch(count);
    // a refill just happened, ours or a concurrent one: take what it produced
    // instead of re-testing the clock, otherwise a tiny stale window starves
    check_stale = false;

  } while (retry++ < FLAGS_txn_op_max_retry);

  DINGO_LOG(ERROR) << fmt::format(
      "[sdk.tso] gen ts fail, retry({}), status({}), max_logical({}), next_logical({}), physical_ts({}).", retry,
      status.ToString(), max_logical_, next_logical_, physical_);

  if (status.ok()) {
    status = Status::Incomplete(
        fmt::format("[sdk.tso] gen ts fail, retry({}), status({}), max_logical({}), next_logical({}), physical_ts({}).",
                    retry, status.ToString(), max_logical_, next_logical_, physical_));
  }

  return status;
}

Status TsoProvider::GenPhysicalTs(int32_t count, int64_t& physical_ts) {
  Status status;

  int retry = 0;
  do {
    // txn heartbeat needs a current physical time, so always go through a
    // refill; a batch another caller just installed is equally fresh
    status = RefillBatch(count);

    if (status.ok()) {
      WriteLockGuard guard(rwlock_);
      if (max_logical_ >= count + next_logical_) {
        physical_ts = physical_;

        next_logical_ += count;

        CHECK(physical_ts > 0) << "physical_ts should be greater than 0 , physical_ts:" << physical_ts;

        return Status::OK();
      }
    }

  } while (retry++ < FLAGS_txn_op_max_retry);

  DINGO_LOG(ERROR) << fmt::format(
      "[sdk.tso] gen ts fail, retry({}), status({}), max_logical({}), next_logical({}), physical_ts({}).", retry,
      status.ToString(), max_logical_, next_logical_, physical_);

  if (status.ok()) {
    status = Status::Incomplete(
        fmt::format("[sdk.tso] gen ts fail, retry({}), status({}), max_logical({}), next_logical({}), physical_ts({}).",
                    retry, status.ToString(), max_logical_, next_logical_, physical_));
  }

  return status;
}

static int64_t SteadyUs() {
  return std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now().time_since_epoch())
      .count();
}

Status TsoProvider::GetPhysicalTs(int64_t& physical_ts) {
  {
    ReadLockGuard guard(rwlock_);
    if (anchor_physical_ms_ > 0) {
      int64_t age_us = SteadyUs() - anchor_steady_us_;
      if (age_us >= 0 && age_us < FLAGS_tso_anchor_max_age_us) {
        int64_t est = anchor_physical_ms_ + (age_us / 1000);
        int64_t prev = max_physical_ms_.load(std::memory_order_relaxed);
        while (est > prev && !max_physical_ms_.compare_exchange_weak(prev, est)) {
        }
        physical_ts = est > prev ? est : prev;
        return Status::OK();
      }
    }
  }

  // no anchor yet or anchor too old: re-anchor with a real fetch
  return GenPhysicalTs(2, physical_ts);
}

bool TsoProvider::IsStale() const { return TimestampUs() > (last_time_us_ + FLAGS_stale_period_us); }

Status TsoProvider::RefillBatch(uint32_t count) {
  std::lock_guard<bthread::Mutex> guard(fetch_mutex_);

  {
    // somebody refilled while we waited for the mutex, reuse their batch
    ReadLockGuard read_guard(rwlock_);
    if (!IsStale() && max_logical_ >= count + next_logical_) return Status::OK();
  }

  return FetchTso(batch_size_);
}

Status TsoProvider::FetchTso(uint32_t count) {
  TsoServiceRpc rpc;
  rpc.MutableRequest()->set_op_type(pb::meta::TsoOpType::OP_GEN_TSO);
  rpc.MutableRequest()->set_count(count);

  auto status = stub_.GetTsoRpcController()->SyncCall(rpc);
  if (!status.IsOK()) {
    DINGO_LOG(ERROR) << fmt::format("[sdk.tso] fetch tso fail, status({}).", status.ToString());
    return status;
  }

  CHECK(rpc.Response()->has_start_timestamp()) << "tso response should has start_timestamp.";

  const auto& tso = rpc.Response()->start_timestamp();
  const auto& ts_count = rpc.Response()->count();

  {
    WriteLockGuard guard(rwlock_);
    physical_ = tso.physical();
    next_logical_ = tso.logical();
    max_logical_ = next_logical_ + ts_count - 1;
    // freshness is measured from the fetch itself, not from the last time
    // somebody noticed the batch had gone stale
    last_time_us_ = TimestampUs();

    anchor_physical_ms_ = tso.physical();
    anchor_steady_us_ = SteadyUs();
  }

  DINGO_LOG(DEBUG) << fmt::format("[sdk.tso] fetch tso ts({}) count({}).", Tso2Timestamp(tso), ts_count);

  return Status::OK();
}

}  // namespace sdk
}  // namespace dingodb