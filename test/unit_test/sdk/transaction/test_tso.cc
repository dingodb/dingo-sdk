// Copyright (c) 2026 dingodb.com, Inc. All Rights Reserved
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

#include <atomic>
#include <iostream>
#include <chrono>
#include <set>
#include <thread>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "sdk/common/param_config.h"
#include "sdk/rpc/coordinator_rpc.h"
#include "sdk/transaction/tso.h"
#include "test_base.h"

namespace dingodb {
namespace sdk {

class TsoProviderTest : public TestBase {
 protected:
  void SetUp() override {
    fetch_count_.store(0);
    next_physical_.store(1);

    ON_CALL(*tso_rpc_controller, SyncCall).WillByDefault([this](Rpc& rpc) {
      auto& tso_rpc = dynamic_cast<TsoServiceRpc&>(rpc);
      const uint32_t count = tso_rpc.Request()->count();

      fetch_count_.fetch_add(1);
      std::this_thread::sleep_for(std::chrono::microseconds(fetch_delay_us_.load()));

      auto* start = tso_rpc.MutableResponse()->mutable_start_timestamp();
      start->set_physical(next_physical_.fetch_add(1));
      start->set_logical(0);
      tso_rpc.MutableResponse()->set_count(count);

      return Status::OK();
    });
  }

  std::atomic<int64_t> fetch_count_{0};
  std::atomic<int64_t> next_physical_{1};
  // a real coordinator round trip is ~100us; tests widen it to make blocking
  // behaviour observable
  std::atomic<int64_t> fetch_delay_us_{500};
};

// Timestamps must be unique and strictly increasing, and concurrent callers
// that need a refill must share one coordinator round trip instead of each
// issuing their own.
TEST_F(TsoProviderTest, ConcurrentGenTsSharesOneFetch) {
  constexpr int kThreadNum = 32;
  constexpr int kPerThread = 50;

  // window shorter than the workload, so refills happen while callers are
  // running: without single flight each of them would issue its own rpc
  FLAGS_stale_period_us = 2000;

  std::vector<std::vector<int64_t>> results(kThreadNum);
  std::vector<std::thread> threads;
  threads.reserve(kThreadNum);
  for (int i = 0; i < kThreadNum; i++) {
    threads.emplace_back([&, i]() {
      for (int j = 0; j < kPerThread; j++) {
        int64_t ts = 0;
        ASSERT_TRUE(tso_provider->GenTs(2, ts).ok());
        results[i].push_back(ts);
      }
    });
  }
  for (auto& thread : threads) thread.join();

  std::set<int64_t> all;
  for (const auto& per_thread : results) {
    for (size_t j = 1; j < per_thread.size(); j++) {
      EXPECT_GT(per_thread[j], per_thread[j - 1]) << "ts must increase within a caller";
    }
    all.insert(per_thread.begin(), per_thread.end());
  }
  EXPECT_EQ(all.size(), static_cast<size_t>(kThreadNum * kPerThread)) << "duplicate ts handed out";

  std::cout << "fetches=" << fetch_count_.load() << " allocations=" << kThreadNum * kPerThread << std::endl;
  // concurrent callers must share a round trip instead of each doing one
  EXPECT_LT(fetch_count_.load(), static_cast<int64_t>(kThreadNum * kPerThread) / 10);
}

// With a sane staleness window the batch is actually consumed, so the number
// of round trips is driven by batch exhaustion, not by the clock.
TEST_F(TsoProviderTest, FreshBatchIsReusedAcrossCalls) {
  FLAGS_stale_period_us = 1000000;

  const int64_t op_num = 100;  // well inside one batch of FLAGS_tso_batch_size logical numbers
  for (int64_t i = 0; i < op_num; i++) {
    int64_t ts = 0;
    ASSERT_TRUE(tso_provider->GenTs(2, ts).ok());
  }

  EXPECT_EQ(fetch_count_.load(), 1) << "one batch must cover " << op_num << " allocations";
}

// The physical-time fast path only reads the anchor, so it must not queue
// behind an in-flight coordinator round trip started by another caller.
TEST_F(TsoProviderTest, PhysicalTsNotBlockedByInFlightFetch) {
  FLAGS_stale_period_us = 1000;

  // prime the anchor with a quick fetch
  int64_t ts = 0;
  ASSERT_TRUE(tso_provider->GenTs(2, ts).ok());

  fetch_delay_us_.store(300000);  // 300ms round trip
  std::this_thread::sleep_for(std::chrono::milliseconds(5));  // batch goes stale

  std::thread fetcher([&]() {
    int64_t slow_ts = 0;
    EXPECT_TRUE(tso_provider->GenTs(2, slow_ts).ok());
  });

  std::this_thread::sleep_for(std::chrono::milliseconds(20));  // let the rpc start

  auto start = std::chrono::steady_clock::now();
  int64_t physical_ts = 0;
  ASSERT_TRUE(tso_provider->GetPhysicalTs(physical_ts).ok());
  const auto elapsed_ms =
      std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start).count();

  fetcher.join();

  std::cout << "GetPhysicalTs elapsed=" << elapsed_ms << "ms (fetch takes 300ms)" << std::endl;
  EXPECT_LT(elapsed_ms, 100) << "physical ts blocked behind the fetch rpc";
}

}  // namespace sdk
}  // namespace dingodb
