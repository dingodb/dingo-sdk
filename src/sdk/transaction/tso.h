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

#ifndef DINGODB_SDK_TRANSACTION_TSO_H_
#define DINGODB_SDK_TRANSACTION_TSO_H_

#include <atomic>
#include <cstdint>
#include <memory>

#include "bthread/mutex.h"
#include "dingosdk/status.h"
#include "proto/meta.pb.h"
#include "sdk/utils/rw_lock.h"

namespace dingodb {
namespace sdk {

class ClientStub;

class TsoProvider {
 public:
  TsoProvider(const ClientStub& stub);
  ~TsoProvider() = default;

  using TsoTimestamp = pb::meta::TsoTimestamp;

  Status GenTs(uint32_t count, int64_t& ts);

  Status GenPhysicalTs(int32_t count, int64_t& physical_ts);

  // Current tso physical time in ms, extrapolated locally from the last
  // FetchTso anchor. For lock-ttl style deadlines only: monotonic and
  // bounded-staleness, but NOT a globally unique timestamp — use GenTs for
  // start_ts/commit_ts. Falls back to a real fetch when the anchor is older
  // than FLAGS_tso_anchor_max_age_us.
  Status GetPhysicalTs(int64_t& physical_ts);

 private:
  // batch is only usable for FLAGS_stale_period_us after it was fetched
  bool IsStale() const;
  // allocates from the cached batch, caller must hold the write lock
  bool TryAllocate(uint32_t count, int64_t& ts, bool check_stale);
  // fetches a new batch, must NOT be called with rwlock_ held
  Status RefillBatch(uint32_t count);
  Status FetchTso(uint32_t count);

  const ClientStub& stub_;

  // fetch batch size
  const uint32_t batch_size_;

  RWLock rwlock_;

  // serializes fetchers so that concurrent callers share a single coordinator
  // round trip instead of each issuing one; never held together with rwlock_
  bthread::Mutex fetch_mutex_;

  int64_t physical_{0};
  int64_t next_logical_{0};
  int64_t max_logical_{0};

  uint64_t last_time_us_{0};

  // physical-time anchor, written on every successful FetchTso (under write
  // lock), read by GetPhysicalTs (under read lock)
  int64_t anchor_physical_ms_{0};
  int64_t anchor_steady_us_{0};
  // largest physical ever returned; deadlines must never regress
  std::atomic<int64_t> max_physical_ms_{0};
};

using TsoProviderSPtr = std::shared_ptr<TsoProvider>;

}  // namespace sdk
}  // namespace dingodb

#endif  // DINGODB_SDK_TRANSACTION_TSO_H_