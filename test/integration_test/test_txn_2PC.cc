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

#include <gflags/gflags_declare.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include "common/helper.h"
#include "dingosdk/client.h"
#include "dingosdk/status.h"
#include "engine_type.h"
#include "fmt/core.h"
#include "glog/logging.h"
#include "gtest/gtest.h"
#include "helper.h"

DECLARE_string(coordinator_url);
DECLARE_int32(thread_count);
DECLARE_int32(txn_test_frequency_2PC);

DECLARE_string(txn_region_name);
DECLARE_string(txn_key_prefix);

namespace dingodb {

namespace integration_test {

static std::vector<std::pair<std::string, std::string>> extra_ranges = {{"01", "02"}, {"11", "12"}, {"21", "22"}};

template <class T>
class TxnTest2PC : public testing::Test {
 protected:
  static void SetUpTestSuite() {
    const std::vector<std::pair<std::string, std::string>> extra_ranges = {{"01", "02"}, {"11", "12"}, {"21", "22"}};
    for (size_t i = 0; i < extra_ranges.size(); ++i) {
      std::string start = FLAGS_txn_key_prefix + extra_ranges[i].first;
      std::string end = FLAGS_txn_key_prefix + extra_ranges[i].second;
      std::string region_name = FLAGS_txn_region_name + fmt::format("_s{}", i + 1);
      int64_t id = Helper::CreateTxnRegion(region_name, start, end, GetEngineType<T>());
      region_ids.push_back(id);
    }
  }
  static void TearDownTestSuite() { Helper::DropRawRegions(region_ids); }

  static std::vector<int64_t> region_ids;
};

template <class T>
std::vector<int64_t> TxnTest2PC<T>::region_ids{};

using Implementations = testing::Types<LsmEngine>;
TYPED_TEST_SUITE(TxnTest2PC, Implementations);

TYPED_TEST(TxnTest2PC, TxnMultiThreadAsyncCommit) {
  testing::Test::RecordProperty("description", "Test multi-threaded async commit txn case");
  {
    dingodb::sdk::Transaction* txn;
    sdk::TransactionOptions options;
    options.isolation = sdk::TransactionIsolation::kSnapshotIsolation;
    options.keep_alive_ms = 10000;
    options.kind = sdk::TransactionKind::kOptimistic;
    auto status = Environment::GetInstance().GetClient()->NewTransaction(options, &txn);
    CHECK(txn != nullptr) << "txn is nullptr";
    for (const auto& range : extra_ranges) {
      std::string key = Helper::EncodeTxnKey(FLAGS_txn_key_prefix + range.first);
      std::string value = "0";
      txn->Put(key, value);
    }
    status = txn->Commit();
    EXPECT_EQ(true, status.IsOK()) << status.ToString();
    delete txn;
  }

  std::vector<std::thread> threads;
  std::atomic<int64_t> success_count(0);
  for (int i = 0; i < FLAGS_thread_count; ++i) {
    threads.emplace_back([i, &success_count]() {
      int thread_success_count = 0;
      while (success_count.load() < FLAGS_txn_test_frequency_2PC) {
        dingodb::sdk::Transaction* txn;
        sdk::TransactionOptions options;
        options.isolation = sdk::TransactionIsolation::kSnapshotIsolation;
        options.keep_alive_ms = 10000;
        options.kind = sdk::TransactionKind::kOptimistic;
        auto status = Environment::GetInstance().GetClient()->NewTransaction(options, &txn);
        CHECK(txn != nullptr) << "txn is nullptr";

        std::vector<std::pair<std::string, std::string>> kvs;
        bool error = false;

        // random the order of keys for this transaction
        std::vector<std::pair<std::string, std::string>> ranges = extra_ranges;
        std::random_device rd;
        std::mt19937 g(rd());
        std::shuffle(ranges.begin(), ranges.end(), g);
        for (const auto& range : ranges) {
          std::string key = Helper::EncodeTxnKey(FLAGS_txn_key_prefix + range.first);
          std::string value;
          status = txn->Get(key, value);
          LOG(INFO) << fmt::format("get txn_id {}, key {}, value: {}, success count {}", txn->ID(), key, value,
                                   success_count.load());
          if (!status.ok() || value.empty()) {
            LOG(WARNING) << fmt::format("Failed to get key {} in thread {}: {}", key, i, status.ToString());
            error = true;
            break;
          }
          int v = std::stoi(value);
          v++;
          value = std::to_string(v);
          LOG(INFO) << fmt::format("before_commit txn_id {}, key {}, value: {}, success count {}", txn->ID(), key,
                                   value, success_count.load());
          txn->Put(key, value);
          kvs.emplace_back(key, value);
        }
        if (error) {
          delete txn;
          std::this_thread::sleep_for(std::chrono::milliseconds(100));
          continue;
        }

        status = txn->Commit();
        if (!status.IsOK()) {
          LOG(WARNING) << fmt::format("Failed to pre-commit transaction id {} in thread {}: {}", txn->ID(), i,
                                      status.ToString());
          txn->Rollback();
          delete txn;
          std::this_thread::sleep_for(std::chrono::milliseconds(100));
          continue;
        }
        thread_success_count++;
        // increase success_count once per committed txn, then log each key/value with the same count
        int64_t new_count = success_count.fetch_add(1) + 1;
        for (const auto& kv : kvs) {
          LOG(INFO) << fmt::format("after_commit success txn_id {}, key {}, value: {}, success count: {}", txn->ID(),
                                   kv.first, kv.second, new_count);
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        delete txn;
      }
      LOG(INFO) << fmt::format("Thread {} exit, success_count: {}", i, thread_success_count);
    });
  }

  for (auto& thread : threads) {
    thread.join();
  }

  {
    dingodb::sdk::Transaction* txn;
    sdk::TransactionOptions options;
    options.isolation = sdk::TransactionIsolation::kSnapshotIsolation;
    options.keep_alive_ms = 10000;
    options.kind = sdk::TransactionKind::kOptimistic;
    auto status = Environment::GetInstance().GetClient()->NewTransaction(options, &txn);
    CHECK(txn != nullptr) << "txn is nullptr";
    for (const auto& range : extra_ranges) {
      std::string key = Helper::EncodeTxnKey(FLAGS_txn_key_prefix + range.first);
      std::string value;
      status = txn->Get(key, value);
      CHECK(status.ok()) << status.ToString();
      EXPECT_EQ(success_count.load(), std::stoi(value))
          << fmt::format("Not match value, key {}, expect {}, actual {}", key, success_count.load(), value);
    }

    delete txn;
  }
}

}  // namespace integration_test
}  // namespace dingodb