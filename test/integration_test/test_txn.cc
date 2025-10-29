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

#include <atomic>
#include <cstdint>
#include <string>
#include <thread>

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
DECLARE_int32(txn_test_frequency);

DECLARE_string(txn_region_name);
DECLARE_string(txn_key_prefix);

namespace dingodb {

namespace integration_test {

template <class T>
class TxnTest : public testing::Test {
 protected:
  static void SetUpTestSuite() {
    region_id = Helper::CreateTxnRegion(FLAGS_txn_region_name, FLAGS_txn_key_prefix,
                                        Helper::PrefixNext(FLAGS_txn_key_prefix), GetEngineType<T>());
  }
  static void TearDownTestSuite() { Helper::DropRawRegion(region_id); }

  static int64_t region_id;
};

template <class T>
int64_t TxnTest<T>::region_id = 0;

using Implementations = testing::Types<LsmEngine>;
TYPED_TEST_SUITE(TxnTest, Implementations);

TYPED_TEST(TxnTest, TxnMultiThread1PC) {
  testing::Test::RecordProperty("description", "Test multi-threaded txn case");
  {
    dingodb::sdk::Transaction* txn;
    sdk::TransactionOptions options;
    options.isolation = sdk::TransactionIsolation::kSnapshotIsolation;
    options.keep_alive_ms = 10000;
    options.kind = sdk::TransactionKind::kOptimistic;
    auto status = Environment::GetInstance().GetClient()->NewTransaction(options, &txn);
    CHECK(txn != nullptr) << "txn is nullptr";
    std::string key = Helper::EncodeTxnKey(FLAGS_txn_key_prefix);
    std::string value = "0";
    txn->Put(key, value);
    status = txn->Commit();
    EXPECT_EQ(true, status.IsOK()) << status.ToString();
    delete txn;
  }

  std::vector<std::thread> threads;
  std::atomic<int64_t> success_count(0);
  for (int i = 0; i < FLAGS_thread_count; ++i) {
    threads.emplace_back([i, &success_count]() {
      int thread_success_count = 0;
      while (success_count.load() < FLAGS_txn_test_frequency) {
        dingodb::sdk::Transaction* txn;
        sdk::TransactionOptions options;
        options.isolation = sdk::TransactionIsolation::kSnapshotIsolation;
        options.keep_alive_ms = 10000;
        options.kind = sdk::TransactionKind::kOptimistic;
        auto status = Environment::GetInstance().GetClient()->NewTransaction(options, &txn);
        CHECK(txn != nullptr) << "txn is nullptr";
        std::string key = Helper::EncodeTxnKey(FLAGS_txn_key_prefix);
        std::string value;
        status = txn->Get(key, value);
        LOG(INFO) << fmt::format("get txn_id {}, value: {}, success count {}", txn->ID(), value, success_count.load());
        if (!status.ok() || value.empty()) {
          LOG(WARNING) << fmt::format("Failed to get key in thread {}: {}", i, status.ToString());
          delete txn;
          continue;
        }
        int v = std::stoi(value);
        v++;
        value = std::to_string(v);
        LOG(INFO) << fmt::format("before_commit txn_id {}, value: {}, success count {}", txn->ID(), value,
                                 success_count.load());
        txn->Put(key, value);
        status = txn->Commit();
        if (!status.IsOK()) {
          LOG(WARNING) << fmt::format("Failed to pre-commit transaction id {} in thread {}: {}", txn->ID(), i,
                                      status.ToString());
          txn->Rollback();
          delete txn;
          continue;
        }
        CHECK(txn->IsOnePc()) << "txn is not 1pc";
        thread_success_count++;
        LOG(INFO) << fmt::format("after_commit success txn_id {}, value: {}, success count: {}", txn->ID(), value,
                                 success_count.fetch_add(1) + 1);
        delete txn;
      }
      LOG(INFO) << fmt::format("Thread {} exit, success_count: {}", i, thread_success_count);
    });
  }

  for (auto& thread : threads) {
    thread.join();
  }

  {
    std::string key = Helper::EncodeTxnKey(FLAGS_txn_key_prefix);
    std::string value;
    dingodb::sdk::Transaction* txn;
    sdk::TransactionOptions options;
    options.isolation = sdk::TransactionIsolation::kSnapshotIsolation;
    options.keep_alive_ms = 10000;
    options.kind = sdk::TransactionKind::kOptimistic;
    auto status = Environment::GetInstance().GetClient()->NewTransaction(options, &txn);
    CHECK(txn != nullptr) << "txn is nullptr";
    status = txn->Get(key, value);
    CHECK(status.ok()) << status.ToString();
    delete txn;

    EXPECT_EQ(success_count.load(), std::stoi(value))
        << fmt::format("Not match value, expect {}, actual {}", success_count.load(), value);
  }
}

}  // namespace integration_test
}  // namespace dingodb