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

#include <memory>
#include <vector>

#include "gtest/gtest.h"
#include "gmock/gmock.h"
#include "sdk/transaction/txn_region_scanner_impl.h"
#include "mock_client_stub.h"
#include "mock_rpc_client.h"
#include "proto/error.pb.h"

namespace dingodb {
namespace sdk {

class TxnRegionScannerImplTest : public testing::Test {
 protected:
  void SetUp() override {
    stub = std::make_unique<MockClientStub>();
    RpcClientOptions rpc_options;
    rpc_client = std::make_shared<MockRpcClient>(rpc_options);

    EXPECT_CALL(*stub, GetRpcClient()).WillRepeatedly(testing::Return(rpc_client));

    dingodb::pb::common::Range range;
    range.set_start_key("a");
    range.set_end_key("z");
    dingodb::pb::common::RegionEpoch epoch;
    epoch.set_conf_version(1);
    epoch.set_version(1);
    Replica replica;
    replica.end_point = EndPoint("127.0.0.1", 8080);
    replica.role = kLeader;
    region = std::make_shared<Region>(1, range, epoch, dingodb::pb::common::RegionType::STORE_REGION, std::vector<Replica>{replica});
  }

  std::unique_ptr<MockClientStub> stub;
  std::shared_ptr<MockRpcClient> rpc_client;
  std::shared_ptr<Region> region;
};

TEST_F(TxnRegionScannerImplTest, NextBatchZeroCopy) {
  TransactionOptions options;
  options.kind = kOptimistic;
  options.isolation = kSnapshotIsolation;

  TxnRegionScannerImpl scanner(*stub, region, options, 100, "a", "c");

  EXPECT_CALL(*rpc_client, SendRpc).WillOnce([&](Rpc& rpc, std::function<void()> cb) {
    auto* txn_rpc = dynamic_cast<TxnScanRpc*>(&rpc);
    CHECK_NOTNULL(txn_rpc);

    auto* response = txn_rpc->MutableResponse();
    auto* kv1 = response->add_kvs();
    kv1->set_key("a1");
    // Ensure we are setting a value that has an underlying allocation we can steal
    std::string v1{"val1"};
    kv1->set_value(v1);

    auto* kv2 = response->add_kvs();
    kv2->set_key("a2");
    std::string v2{"val2"};
    kv2->set_value(v2);

    response->mutable_stream_meta()->set_has_more(false);

    cb();
  });

  EXPECT_TRUE(scanner.Open().ok());

  std::vector<KVPair> kvs;
  Status s = scanner.NextBatch(kvs);
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(kvs.size(), 2);
  EXPECT_EQ(kvs[0].key, "a1");
  EXPECT_EQ(kvs[0].value, "val1");
  EXPECT_EQ(kvs[1].key, "a2");
  EXPECT_EQ(kvs[1].value, "val2");

  EXPECT_FALSE(scanner.HasMore());
}

}  // namespace sdk
}  // namespace dingodb
