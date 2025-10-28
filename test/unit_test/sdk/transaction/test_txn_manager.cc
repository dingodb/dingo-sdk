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
#include <unistd.h>

#include <cmath>
#include <cstdint>
#include <memory>

#include "dingosdk/client.h"
#include "dingosdk/status.h"
#include "glog/logging.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "proto//meta.pb.h"
#include "proto/error.pb.h"
#include "proto/store.pb.h"
#include "sdk/common/common.h"
#include "sdk/common/param_config.h"
#include "sdk/rpc/coordinator_rpc.h"
#include "sdk/rpc/store_rpc.h"
#include "sdk/transaction/txn_impl.h"
#include "test_base.h"
#include "test_common.h"

namespace dingodb {
namespace sdk {

class SDKTxnManagerTest : public TestBase {
 public:
  SDKTxnManagerTest() = default;
  ~SDKTxnManagerTest() override = default;

  // TODO: test readcommited isolation
  void SetUp() override {
    TestBase::SetUp();
    options.kind = kOptimistic;
    options.isolation = kSnapshotIsolation;

    ON_CALL(*meta_rpc_controller, SyncCall).WillByDefault([&](Rpc& rpc) {
      auto* t_rpc = dynamic_cast<TsoServiceRpc*>(&rpc);
      EXPECT_EQ(t_rpc->Request()->op_type(), pb::meta::OP_GEN_TSO);
      t_rpc->MutableResponse()->set_count(FLAGS_tso_batch_size);
      auto* ts = t_rpc->MutableResponse()->mutable_start_timestamp();
      *ts = CurrentFakeTso();

      return Status::OK();
    });

    EXPECT_CALL(*meta_rpc_controller, SyncCall).Times(testing::AnyNumber());

    ON_CALL(*txn_lock_resolver, ResolveLock).WillByDefault(testing::Return(Status::OK()));
  }

  TransactionOptions options;
};

TEST_F(SDKTxnManagerTest, TransactionManagerEmpty) {
  auto txn1 = NewTransaction(options);
  auto txn2 = NewTransaction(options);
  auto txn3 = NewTransaction(options);
  EXPECT_NE(txn1->ID(), txn2->ID());
  EXPECT_NE(txn1->ID(), txn3->ID());
  EXPECT_NE(txn2->ID(), txn3->ID());
  EXPECT_GT(txn1->ID(), 0);
  EXPECT_GT(txn2->ID(), 0);
  EXPECT_GT(txn3->ID(), 0);

  txn1->PreCommit();
  txn1->Commit();

  txn2->PreCommit();
  txn2->Commit();

  txn3->PreCommit();
  txn3->Commit();
}

TEST_F(SDKTxnManagerTest, TransactionManagerWithData1pc) {
  EXPECT_CALL(*rpc_client, SendRpc(testing::_, testing::_)).WillRepeatedly([](Rpc& rpc, std::function<void()> cb) {
    (void)rpc;
    cb();
  });

  {
    auto txn1 = NewTransaction(options);
    txn1->Put("a", "a");
    txn1->PreCommit();
    txn1->Commit();
  }

  {
    auto txn2 = NewTransaction(options);
    txn2->Put("c", "c");
    txn2->PreCommit();
    txn2->Commit();
  }

  {
    auto txn3 = NewTransaction(options);
    txn3->PutIfAbsent("d", "d");
    txn3->PreCommit();
    txn3->Commit();
  }
}

TEST_F(SDKTxnManagerTest, TransactionManagerWithData2pc) {
  EXPECT_CALL(*rpc_client, SendRpc(testing::_, testing::_)).WillRepeatedly([](Rpc& rpc, std::function<void()> cb) {
    (void)rpc;
    cb();
  });

  {
    auto txn1 = NewTransaction(options);
    txn1->Put("a", "a");
    txn1->Put("d", "d");
    txn1->PreCommit();
    txn1->Commit();
  }

  {
    auto txn2 = NewTransaction(options);
    txn2->Put("b", "b");
    txn2->Put("e", "e");
    txn2->PreCommit();
    txn2->Commit();
  }

  {
    auto txn3 = NewTransaction(options);
    txn3->PutIfAbsent("a", "newa");
    txn3->PutIfAbsent("d", "newd");
    txn3->PreCommit();
    txn3->Commit();
  }
}

}  // namespace sdk
}  // namespace dingodb