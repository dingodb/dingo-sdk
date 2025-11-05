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

#include <glog/logging.h>

#include <cstdint>
#include <functional>
#include <memory>

#include "dingosdk/metric.h"
#include "gtest/gtest.h"
#include "sdk/common/common.h"
#include "sdk/common/param_config.h"
#include "sdk/rpc/coordinator_rpc.h"
#include "sdk/rpc/rpc.h"
#include "sdk/rpc/store_rpc.h"
#include "sdk/transaction/txn_lock_resolver.h"
#include "test_base.h"
#include "test_common.h"

namespace dingodb {
namespace sdk {

class SDKTxnLockResolverTest : public TestBase {
 public:
  SDKTxnLockResolverTest() = default;
  ~SDKTxnLockResolverTest() override = default;

  void SetUp() override {
    TestBase::SetUp();

    lock_resolver = std::make_shared<TxnLockResolver>(*stub);
    init_tso = CurrentFakeTso();
  }

  std::shared_ptr<TxnLockResolver> lock_resolver;
  pb::meta::TsoTimestamp init_tso;
};

TEST_F(SDKTxnLockResolverTest, TxnNotFound) {
  std::string key = "b";
  auto fake_lock = PrepareLockInfo();
  fake_lock.set_key(key);

  std::shared_ptr<Region> region;
  CHECK(meta_cache->LookupRegionByKey(fake_lock.primary_lock(), region).IsOK());
  CHECK_NOTNULL(region.get());

  auto fake_tso = CurrentFakeTso();

  EXPECT_CALL(*meta_rpc_controller, SyncCall).WillOnce([&](Rpc& rpc) {
    auto* t_rpc = dynamic_cast<TsoServiceRpc*>(&rpc);
    EXPECT_EQ(t_rpc->Request()->op_type(), pb::meta::OP_GEN_TSO);
    t_rpc->MutableResponse()->set_count(FLAGS_tso_batch_size);
    auto* ts = t_rpc->MutableResponse()->mutable_start_timestamp();
    *ts = fake_tso;

    return Status::OK();
  });

  EXPECT_CALL(*rpc_client, SendRpc).WillOnce([&](Rpc& rpc, std::function<void()> cb) {
    auto* txn_rpc = dynamic_cast<TxnCheckTxnStatusRpc*>(&rpc);
    CHECK_NOTNULL(txn_rpc);

    const auto* request = txn_rpc->Request();
    EXPECT_TRUE(request->has_context());
    auto context = request->context();
    EXPECT_EQ(context.region_id(), region->RegionId());
    EXPECT_TRUE(context.has_region_epoch());
    EXPECT_EQ(0, EpochCompare(RegionEpoch(context.region_epoch().version(), context.region_epoch().conf_version()),
                              region->GetEpoch()));

    EXPECT_EQ(request->primary_key(), fake_lock.primary_lock());
    EXPECT_EQ(request->lock_ts(), fake_lock.lock_ts());
    EXPECT_EQ(request->current_ts(), Tso2Timestamp(fake_tso));

    // txn_rpc->MutableResponse()->set_lock_ttl(10);
    auto* txn_result = txn_rpc->MutableResponse()->mutable_txn_result();
    auto* no_txn = txn_result->mutable_txn_not_found();
    no_txn->set_start_ts(request->lock_ts());
    no_txn->set_primary_key(request->primary_key());

    cb();
  });

  Status s = lock_resolver->ResolveLock(fake_lock, Tso2Timestamp(init_tso));
  EXPECT_TRUE(s.IsTxnNotFound());
}

TEST_F(SDKTxnLockResolverTest, Locked) {
  std::string key = "b";
  auto fake_lock = PrepareLockInfo();
  fake_lock.set_key(key);

  std::shared_ptr<Region> region;
  CHECK(meta_cache->LookupRegionByKey(fake_lock.primary_lock(), region).IsOK());
  CHECK_NOTNULL(region.get());

  auto fake_tso = CurrentFakeTso();

  EXPECT_CALL(*meta_rpc_controller, SyncCall).WillOnce([&](Rpc& rpc) {
    auto* t_rpc = dynamic_cast<TsoServiceRpc*>(&rpc);
    EXPECT_EQ(t_rpc->Request()->op_type(), pb::meta::OP_GEN_TSO);
    t_rpc->MutableResponse()->set_count(FLAGS_tso_batch_size);
    auto* ts = t_rpc->MutableResponse()->mutable_start_timestamp();
    *ts = fake_tso;

    return Status::OK();
  });

  EXPECT_CALL(*rpc_client, SendRpc).WillOnce([&](Rpc& rpc, std::function<void()> cb) {
    auto* txn_rpc = dynamic_cast<TxnCheckTxnStatusRpc*>(&rpc);
    CHECK_NOTNULL(txn_rpc);

    const auto* request = txn_rpc->Request();
    EXPECT_TRUE(request->has_context());
    auto context = request->context();
    EXPECT_EQ(context.region_id(), region->RegionId());
    EXPECT_TRUE(context.has_region_epoch());
    EXPECT_EQ(0, EpochCompare(RegionEpoch(context.region_epoch().version(), context.region_epoch().conf_version()),
                              region->GetEpoch()));

    EXPECT_EQ(request->primary_key(), fake_lock.primary_lock());
    EXPECT_EQ(request->lock_ts(), fake_lock.lock_ts());
    EXPECT_EQ(request->current_ts(), Tso2Timestamp(fake_tso));

    txn_rpc->MutableResponse()->set_lock_ttl(10);

    cb();
  });

  Status s = lock_resolver->ResolveLock(fake_lock, Tso2Timestamp(init_tso));
  EXPECT_TRUE(s.IsTxnLockConflict());
}

TEST_F(SDKTxnLockResolverTest, Committed) {
  // NOTE: careful!!! key and fake_lock primary key in same region
  std::string key = "b";
  auto fake_lock = PrepareLockInfo();
  fake_lock.set_key(key);

  std::shared_ptr<Region> region;
  CHECK(meta_cache->LookupRegionByKey(fake_lock.primary_lock(), region).IsOK());
  CHECK_NOTNULL(region.get());

  auto fake_tso = CurrentFakeTso();

  EXPECT_CALL(*meta_rpc_controller, SyncCall).WillOnce([&](Rpc& rpc) {
    auto* t_rpc = dynamic_cast<TsoServiceRpc*>(&rpc);
    EXPECT_EQ(t_rpc->Request()->op_type(), pb::meta::OP_GEN_TSO);
    t_rpc->MutableResponse()->set_count(FLAGS_tso_batch_size);
    auto* ts = t_rpc->MutableResponse()->mutable_start_timestamp();
    *ts = fake_tso;

    return Status::OK();
  });

  EXPECT_CALL(*rpc_client, SendRpc)
      .WillOnce([&](Rpc& rpc, std::function<void()> cb) {
        auto* txn_rpc = dynamic_cast<TxnCheckTxnStatusRpc*>(&rpc);
        CHECK_NOTNULL(txn_rpc);

        const auto* request = txn_rpc->Request();
        EXPECT_TRUE(request->has_context());
        auto context = request->context();
        EXPECT_EQ(context.region_id(), region->RegionId());
        EXPECT_TRUE(context.has_region_epoch());
        EXPECT_EQ(0, EpochCompare(RegionEpoch(context.region_epoch().version(), context.region_epoch().conf_version()),
                                  region->GetEpoch()));

        EXPECT_EQ(request->primary_key(), fake_lock.primary_lock());
        EXPECT_EQ(request->lock_ts(), fake_lock.lock_ts());
        EXPECT_EQ(request->current_ts(), Tso2Timestamp(fake_tso));
        EXPECT_EQ(request->caller_start_ts(), Tso2Timestamp(init_tso));

        // primary lock is committed
        txn_rpc->MutableResponse()->set_commit_ts(request->current_ts());

        cb();
      })
      .WillOnce([&](Rpc& rpc, std::function<void()> cb) {
        //  resolve conlict key

        auto* txn_rpc = dynamic_cast<TxnResolveLockRpc*>(&rpc);
        CHECK_NOTNULL(txn_rpc);

        const auto* request = txn_rpc->Request();
        EXPECT_TRUE(request->has_context());
        auto context = request->context();
        EXPECT_EQ(context.region_id(), region->RegionId());
        EXPECT_TRUE(context.has_region_epoch());
        EXPECT_EQ(0, EpochCompare(RegionEpoch(context.region_epoch().version(), context.region_epoch().conf_version()),
                                  region->GetEpoch()));

        EXPECT_EQ(request->start_ts(), fake_lock.lock_ts());
        EXPECT_EQ(request->commit_ts(), Tso2Timestamp(fake_tso));
        EXPECT_EQ(request->keys_size(), 1);

        const auto& key = request->keys(0);
        EXPECT_EQ(key, fake_lock.key());

        cb();
      });

  Status s = lock_resolver->ResolveLock(fake_lock, Tso2Timestamp(init_tso));
  EXPECT_TRUE(s.ok());
}

TEST_F(SDKTxnLockResolverTest, CommittedResolvePrimaryKeyFail) {
  // NOTE: careful!!! key and fake_lock primary key in same region
  std::string key = "b";
  auto fake_lock = PrepareLockInfo();
  fake_lock.set_key(key);

  std::shared_ptr<Region> region;
  CHECK(meta_cache->LookupRegionByKey(fake_lock.primary_lock(), region).IsOK());
  CHECK_NOTNULL(region.get());

  auto fake_tso = CurrentFakeTso();

  EXPECT_CALL(*meta_rpc_controller, SyncCall).WillOnce([&](Rpc& rpc) {
    auto* t_rpc = dynamic_cast<TsoServiceRpc*>(&rpc);
    EXPECT_EQ(t_rpc->Request()->op_type(), pb::meta::OP_GEN_TSO);
    t_rpc->MutableResponse()->set_count(FLAGS_tso_batch_size);
    auto* ts = t_rpc->MutableResponse()->mutable_start_timestamp();
    *ts = fake_tso;

    return Status::OK();
  });

  EXPECT_CALL(*rpc_client, SendRpc)
      .WillOnce([&](Rpc& rpc, std::function<void()> cb) {
        auto* txn_rpc = dynamic_cast<TxnCheckTxnStatusRpc*>(&rpc);
        CHECK_NOTNULL(txn_rpc);

        const auto* request = txn_rpc->Request();
        EXPECT_TRUE(request->has_context());
        auto context = request->context();
        EXPECT_EQ(context.region_id(), region->RegionId());
        EXPECT_TRUE(context.has_region_epoch());
        EXPECT_EQ(0, EpochCompare(RegionEpoch(context.region_epoch().version(), context.region_epoch().conf_version()),
                                  region->GetEpoch()));

        EXPECT_EQ(request->primary_key(), fake_lock.primary_lock());
        EXPECT_EQ(request->lock_ts(), fake_lock.lock_ts());
        EXPECT_EQ(request->current_ts(), Tso2Timestamp(fake_tso));
        EXPECT_EQ(request->caller_start_ts(), Tso2Timestamp(init_tso));

        txn_rpc->MutableResponse()->set_commit_ts(request->current_ts());

        cb();
      })
      .WillOnce([&](Rpc& rpc, std::function<void()> cb) {
        //  resolve primary key

        auto* txn_rpc = dynamic_cast<TxnResolveLockRpc*>(&rpc);
        CHECK_NOTNULL(txn_rpc);

        const auto* request = txn_rpc->Request();
        EXPECT_TRUE(request->has_context());
        auto context = request->context();
        EXPECT_EQ(context.region_id(), region->RegionId());
        EXPECT_TRUE(context.has_region_epoch());
        EXPECT_EQ(0, EpochCompare(RegionEpoch(context.region_epoch().version(), context.region_epoch().conf_version()),
                                  region->GetEpoch()));

        EXPECT_EQ(request->start_ts(), fake_lock.lock_ts());
        EXPECT_EQ(request->commit_ts(), Tso2Timestamp(fake_tso));
        EXPECT_EQ(request->keys_size(), 1);

        const auto& key = request->keys(0);
        EXPECT_EQ(key, fake_lock.key());

        auto* response = txn_rpc->MutableResponse();
        auto* error = response->mutable_error();
        error->set_errcode(pb::error::Errno::EILLEGAL_PARAMTETERS);

        cb();
      });

  Status s = lock_resolver->ResolveLock(fake_lock, Tso2Timestamp(init_tso));
  EXPECT_TRUE(!s.ok());
}

TEST_F(SDKTxnLockResolverTest, CommittedResolveConflictKeyFail) {
  // NOTE: careful!!! key and fake_lock primary key in same region
  std::string key = "b";
  auto fake_lock = PrepareLockInfo();
  fake_lock.set_key(key);

  std::shared_ptr<Region> region;
  CHECK(meta_cache->LookupRegionByKey(fake_lock.primary_lock(), region).IsOK());
  CHECK_NOTNULL(region.get());

  auto fake_tso = CurrentFakeTso();

  EXPECT_CALL(*meta_rpc_controller, SyncCall).WillOnce([&](Rpc& rpc) {
    auto* t_rpc = dynamic_cast<TsoServiceRpc*>(&rpc);
    EXPECT_EQ(t_rpc->Request()->op_type(), pb::meta::OP_GEN_TSO);
    t_rpc->MutableResponse()->set_count(FLAGS_tso_batch_size);
    auto* ts = t_rpc->MutableResponse()->mutable_start_timestamp();
    *ts = fake_tso;

    return Status::OK();
  });

  EXPECT_CALL(*rpc_client, SendRpc)
      .WillOnce([&](Rpc& rpc, std::function<void()> cb) {
        auto* txn_rpc = dynamic_cast<TxnCheckTxnStatusRpc*>(&rpc);
        CHECK_NOTNULL(txn_rpc);

        const auto* request = txn_rpc->Request();
        EXPECT_TRUE(request->has_context());
        auto context = request->context();
        EXPECT_EQ(context.region_id(), region->RegionId());
        EXPECT_TRUE(context.has_region_epoch());
        EXPECT_EQ(0, EpochCompare(RegionEpoch(context.region_epoch().version(), context.region_epoch().conf_version()),
                                  region->GetEpoch()));

        EXPECT_EQ(request->primary_key(), fake_lock.primary_lock());
        EXPECT_EQ(request->lock_ts(), fake_lock.lock_ts());
        EXPECT_EQ(request->current_ts(), Tso2Timestamp(fake_tso));
        EXPECT_EQ(request->caller_start_ts(), Tso2Timestamp(init_tso));

        txn_rpc->MutableResponse()->set_commit_ts(request->current_ts());

        cb();
      })
      .WillOnce([&](Rpc& rpc, std::function<void()> cb) {
        //  resolve primary key

        auto* txn_rpc = dynamic_cast<TxnResolveLockRpc*>(&rpc);
        CHECK_NOTNULL(txn_rpc);

        const auto* request = txn_rpc->Request();
        EXPECT_TRUE(request->has_context());
        auto context = request->context();
        EXPECT_EQ(context.region_id(), region->RegionId());
        EXPECT_TRUE(context.has_region_epoch());
        EXPECT_EQ(0, EpochCompare(RegionEpoch(context.region_epoch().version(), context.region_epoch().conf_version()),
                                  region->GetEpoch()));

        EXPECT_EQ(request->start_ts(), fake_lock.lock_ts());
        EXPECT_EQ(request->commit_ts(), Tso2Timestamp(fake_tso));
        EXPECT_EQ(request->keys_size(), 1);

        const auto& key = request->keys(0);
        EXPECT_EQ(key, fake_lock.key());

        auto* response = txn_rpc->MutableResponse();
        auto* error = response->mutable_error();
        error->set_errcode(pb::error::Errno::EILLEGAL_PARAMTETERS);

        cb();
      });

  Status s = lock_resolver->ResolveLock(fake_lock, Tso2Timestamp(init_tso));
  EXPECT_TRUE(!s.ok());
}

TEST_F(SDKTxnLockResolverTest, Rollbacked) {
  // NOTE: careful!!! key and fake_lock primary key in same region
  std::string key = "b";
  auto fake_lock = PrepareLockInfo();
  fake_lock.set_key(key);

  std::shared_ptr<Region> region;
  CHECK(meta_cache->LookupRegionByKey(fake_lock.primary_lock(), region).IsOK());
  CHECK_NOTNULL(region.get());

  auto fake_tso = CurrentFakeTso();

  EXPECT_CALL(*meta_rpc_controller, SyncCall).WillOnce([&](Rpc& rpc) {
    auto* t_rpc = dynamic_cast<TsoServiceRpc*>(&rpc);
    EXPECT_EQ(t_rpc->Request()->op_type(), pb::meta::OP_GEN_TSO);
    t_rpc->MutableResponse()->set_count(FLAGS_tso_batch_size);
    auto* ts = t_rpc->MutableResponse()->mutable_start_timestamp();
    *ts = fake_tso;

    return Status::OK();
  });

  EXPECT_CALL(*rpc_client, SendRpc)
      .WillOnce([&](Rpc& rpc, std::function<void()> cb) {
        auto* txn_rpc = dynamic_cast<TxnCheckTxnStatusRpc*>(&rpc);
        CHECK_NOTNULL(txn_rpc);

        const auto* request = txn_rpc->Request();
        EXPECT_TRUE(request->has_context());
        auto context = request->context();
        EXPECT_EQ(context.region_id(), region->RegionId());
        EXPECT_TRUE(context.has_region_epoch());
        EXPECT_EQ(0, EpochCompare(RegionEpoch(context.region_epoch().version(), context.region_epoch().conf_version()),
                                  region->GetEpoch()));

        EXPECT_EQ(request->primary_key(), fake_lock.primary_lock());
        EXPECT_EQ(request->lock_ts(), fake_lock.lock_ts());
        EXPECT_EQ(request->current_ts(), Tso2Timestamp(fake_tso));
        EXPECT_EQ(request->caller_start_ts(), Tso2Timestamp(init_tso));

        txn_rpc->MutableResponse()->set_lock_ttl(0);
        txn_rpc->MutableResponse()->set_commit_ts(0);

        cb();
      })
      .WillOnce([&](Rpc& rpc, std::function<void()> cb) {
        //  resolve conlict key

        auto* txn_rpc = dynamic_cast<TxnResolveLockRpc*>(&rpc);
        CHECK_NOTNULL(txn_rpc);

        const auto* request = txn_rpc->Request();
        EXPECT_TRUE(request->has_context());
        auto context = request->context();
        EXPECT_EQ(context.region_id(), region->RegionId());
        EXPECT_TRUE(context.has_region_epoch());
        EXPECT_EQ(0, EpochCompare(RegionEpoch(context.region_epoch().version(), context.region_epoch().conf_version()),
                                  region->GetEpoch()));

        EXPECT_EQ(request->start_ts(), fake_lock.lock_ts());
        EXPECT_EQ(request->commit_ts(), 0);
        EXPECT_EQ(request->keys_size(), 1);

        const auto& key = request->keys(0);
        EXPECT_EQ(key, fake_lock.key());

        cb();
      });

  Status s = lock_resolver->ResolveLock(fake_lock, Tso2Timestamp(init_tso));
  EXPECT_TRUE(s.ok());
}

TEST_F(SDKTxnLockResolverTest, AsyncCommitCasePrimaryLockNotExpired) {
  // NOTE: careful!!! key and fake_lock primary key in same region
  std::string key = "b0000000";
  auto fake_lock = PrepareAsyncCommitOrdinaryLockInfo();
  fake_lock.set_key(key);

  std::shared_ptr<Region> region;
  CHECK(meta_cache->LookupRegionByKey(fake_lock.primary_lock(), region).IsOK());
  CHECK_NOTNULL(region.get());

  auto fake_tso = CurrentFakeTso();

  EXPECT_CALL(*meta_rpc_controller, SyncCall).WillRepeatedly([&](Rpc& rpc) {
    auto* t_rpc = dynamic_cast<TsoServiceRpc*>(&rpc);
    EXPECT_EQ(t_rpc->Request()->op_type(), pb::meta::OP_GEN_TSO);
    t_rpc->MutableResponse()->set_count(FLAGS_tso_batch_size);
    auto* ts = t_rpc->MutableResponse()->mutable_start_timestamp();
    *ts = fake_tso;

    return Status::OK();
  });

  EXPECT_CALL(*rpc_client, SendRpc).WillOnce([&](Rpc& rpc, std::function<void()> cb) {
    auto* txn_rpc = dynamic_cast<TxnCheckTxnStatusRpc*>(&rpc);
    CHECK_NOTNULL(txn_rpc);

    const auto* request = txn_rpc->Request();
    EXPECT_TRUE(request->has_context());
    auto context = request->context();
    EXPECT_EQ(context.region_id(), region->RegionId());
    EXPECT_TRUE(context.has_region_epoch());
    EXPECT_EQ(0, EpochCompare(RegionEpoch(context.region_epoch().version(), context.region_epoch().conf_version()),
                              region->GetEpoch()));

    EXPECT_EQ(request->primary_key(), fake_lock.primary_lock());
    EXPECT_EQ(request->lock_ts(), fake_lock.lock_ts());
    EXPECT_EQ(request->current_ts(), Tso2Timestamp(fake_tso));
    EXPECT_EQ(request->caller_start_ts(), Tso2Timestamp(init_tso));

    // async commit primary lock info
    auto* lock_info = txn_rpc->MutableResponse()->mutable_txn_result()->mutable_locked();
    lock_info->set_key("a0000000");
    lock_info->set_primary_lock("a0000000");
    lock_info->set_lock_ts(1);
    // means lock ttl expired
    lock_info->set_lock_ttl(INT64_MAX);
    lock_info->set_txn_size(1);
    lock_info->set_lock_type(pb::store::Op::Put);
    lock_info->set_use_async_commit(true);
    lock_info->set_min_commit_ts(10);
    lock_info->add_secondaries("b0000000");
    lock_info->add_secondaries("d0000000");
    lock_info->add_secondaries("f0000000");

    cb();
  });
  Status s = lock_resolver->ResolveLock(fake_lock, Tso2Timestamp(init_tso));
  EXPECT_TRUE(s.IsTxnLockConflict());
}

TEST_F(SDKTxnLockResolverTest, AsyncCommitCaseCommittedPrimaryLock) {
  // NOTE: careful!!! key and fake_lock primary key in same region
  std::string key = "b0000000";
  auto fake_lock = PrepareAsyncCommitOrdinaryLockInfo();
  fake_lock.set_key(key);

  std::shared_ptr<Region> region;
  CHECK(meta_cache->LookupRegionByKey(fake_lock.primary_lock(), region).IsOK());
  CHECK_NOTNULL(region.get());

  auto fake_tso = CurrentFakeTso();

  EXPECT_CALL(*meta_rpc_controller, SyncCall).WillOnce([&](Rpc& rpc) {
    auto* t_rpc = dynamic_cast<TsoServiceRpc*>(&rpc);
    EXPECT_EQ(t_rpc->Request()->op_type(), pb::meta::OP_GEN_TSO);
    t_rpc->MutableResponse()->set_count(FLAGS_tso_batch_size);
    auto* ts = t_rpc->MutableResponse()->mutable_start_timestamp();
    *ts = fake_tso;

    return Status::OK();
  });

  EXPECT_CALL(*rpc_client, SendRpc)
      .WillOnce([&](Rpc& rpc, std::function<void()> cb) {
        auto* txn_rpc = dynamic_cast<TxnCheckTxnStatusRpc*>(&rpc);
        CHECK_NOTNULL(txn_rpc);

        const auto* request = txn_rpc->Request();
        EXPECT_TRUE(request->has_context());
        auto context = request->context();
        EXPECT_EQ(context.region_id(), region->RegionId());
        EXPECT_TRUE(context.has_region_epoch());
        EXPECT_EQ(0, EpochCompare(RegionEpoch(context.region_epoch().version(), context.region_epoch().conf_version()),
                                  region->GetEpoch()));

        EXPECT_EQ(request->primary_key(), fake_lock.primary_lock());
        EXPECT_EQ(request->lock_ts(), fake_lock.lock_ts());
        EXPECT_EQ(request->current_ts(), Tso2Timestamp(fake_tso));
        EXPECT_EQ(request->caller_start_ts(), Tso2Timestamp(init_tso));

        // primary lock is committed
        txn_rpc->MutableResponse()->set_commit_ts(10);

        cb();
      })
      .WillOnce([&](Rpc& rpc, std::function<void()> cb) {
        //  resolve conlict key

        auto* txn_rpc = dynamic_cast<TxnResolveLockRpc*>(&rpc);
        CHECK_NOTNULL(txn_rpc);

        const auto* request = txn_rpc->Request();
        EXPECT_TRUE(request->has_context());
        auto context = request->context();
        EXPECT_EQ(context.region_id(), region->RegionId());
        EXPECT_TRUE(context.has_region_epoch());
        EXPECT_EQ(0, EpochCompare(RegionEpoch(context.region_epoch().version(), context.region_epoch().conf_version()),
                                  region->GetEpoch()));

        EXPECT_EQ(request->start_ts(), fake_lock.lock_ts());
        EXPECT_EQ(request->commit_ts(), 10);
        EXPECT_EQ(request->keys_size(), 1);

        const auto& key = request->keys(0);
        EXPECT_EQ(key, fake_lock.key());

        cb();
      });
  Status s = lock_resolver->ResolveLock(fake_lock, Tso2Timestamp(init_tso));
  EXPECT_TRUE(s.ok());
}

TEST_F(SDKTxnLockResolverTest, AsyncCommitCaseNotCommittedKeys) {
  // NOTE: careful!!! key and fake_lock primary key in same region
  std::string key = "b0000000";
  auto fake_lock = PrepareAsyncCommitOrdinaryLockInfo();
  fake_lock.set_key(key);

  std::shared_ptr<Region> region;
  CHECK(meta_cache->LookupRegionByKey(fake_lock.primary_lock(), region).IsOK());
  CHECK_NOTNULL(region.get());

  auto fake_tso = CurrentFakeTso();

  EXPECT_CALL(*meta_rpc_controller, SyncCall).WillRepeatedly([&](Rpc& rpc) {
    auto* t_rpc = dynamic_cast<TsoServiceRpc*>(&rpc);
    EXPECT_EQ(t_rpc->Request()->op_type(), pb::meta::OP_GEN_TSO);
    t_rpc->MutableResponse()->set_count(FLAGS_tso_batch_size);
    auto* ts = t_rpc->MutableResponse()->mutable_start_timestamp();
    *ts = CurrentFakeTso();

    return Status::OK();
  });

  EXPECT_CALL(*rpc_client, SendRpc)
      .WillOnce([&](Rpc& rpc, std::function<void()> cb) {
        auto* txn_rpc = dynamic_cast<TxnCheckTxnStatusRpc*>(&rpc);
        CHECK_NOTNULL(txn_rpc);

        const auto* request = txn_rpc->Request();
        EXPECT_TRUE(request->has_context());
        auto context = request->context();
        EXPECT_EQ(context.region_id(), region->RegionId());
        EXPECT_TRUE(context.has_region_epoch());
        EXPECT_EQ(0, EpochCompare(RegionEpoch(context.region_epoch().version(), context.region_epoch().conf_version()),
                                  region->GetEpoch()));

        EXPECT_EQ(request->primary_key(), fake_lock.primary_lock());
        EXPECT_EQ(request->lock_ts(), fake_lock.lock_ts());

        // async commit primary lock info
        auto* lock_info = txn_rpc->MutableResponse()->mutable_txn_result()->mutable_locked();
        lock_info->set_key("a0000000");
        lock_info->set_primary_lock("a0000000");
        lock_info->set_lock_ts(1);
        // means lock ttl expired
        lock_info->set_lock_ttl(0);
        lock_info->set_txn_size(1);
        lock_info->set_lock_type(pb::store::Op::Put);
        lock_info->set_use_async_commit(true);
        lock_info->set_min_commit_ts(10);
        lock_info->add_secondaries("b0000000");
        lock_info->add_secondaries("d0000000");
        lock_info->add_secondaries("f0000000");

        cb();
      })
      .WillOnce([&](Rpc& rpc, std::function<void()> cb) {
        auto* txn_rpc = dynamic_cast<TxnCheckSecondaryLocksRpc*>(&rpc);
        CHECK_NOTNULL(txn_rpc);

        const auto* request = txn_rpc->Request();
        EXPECT_TRUE(request->has_context());
        auto context = request->context();
        EXPECT_EQ(fake_lock.lock_ts(), request->start_ts());

        EXPECT_TRUE(request->keys(0) == "b0000000" || request->keys(0) == "d0000000" || request->keys(0) == "f0000000");

        auto* response = txn_rpc->MutableResponse();
        auto* lock_info = response->add_locks();

        lock_info->set_lock_ts(request->start_ts());
        lock_info->set_primary_lock(fake_lock.primary_lock());
        lock_info->set_key(request->keys(0));
        lock_info->set_lock_ttl(0);
        lock_info->set_use_async_commit(true);
        lock_info->set_lock_type(pb::store::Op::Put);
        lock_info->set_min_commit_ts(11);
        lock_info->add_secondaries("b0000000");
        lock_info->add_secondaries("d0000000");
        lock_info->add_secondaries("f0000000");

        cb();
      })
      .WillOnce([&](Rpc& rpc, std::function<void()> cb) {
        auto* txn_rpc = dynamic_cast<TxnCheckSecondaryLocksRpc*>(&rpc);
        CHECK_NOTNULL(txn_rpc);

        const auto* request = txn_rpc->Request();
        EXPECT_TRUE(request->has_context());
        auto context = request->context();
        EXPECT_EQ(fake_lock.lock_ts(), request->start_ts());

        EXPECT_TRUE(request->keys(0) == "b0000000" || request->keys(0) == "d0000000" || request->keys(0) == "f0000000");

        auto* response = txn_rpc->MutableResponse();
        auto* lock_info = response->add_locks();

        lock_info->set_lock_ts(request->start_ts());
        lock_info->set_primary_lock(fake_lock.primary_lock());
        lock_info->set_key(request->keys(0));
        lock_info->set_lock_ttl(0);
        lock_info->set_use_async_commit(true);
        lock_info->set_lock_type(pb::store::Op::Put);
        lock_info->set_min_commit_ts(12);
        lock_info->add_secondaries("b0000000");
        lock_info->add_secondaries("d0000000");
        lock_info->add_secondaries("f0000000");
        cb();
      })
      .WillOnce([&](Rpc& rpc, std::function<void()> cb) {
        auto* txn_rpc = dynamic_cast<TxnCheckSecondaryLocksRpc*>(&rpc);
        CHECK_NOTNULL(txn_rpc);

        const auto* request = txn_rpc->Request();
        EXPECT_TRUE(request->has_context());
        auto context = request->context();
        EXPECT_EQ(fake_lock.lock_ts(), request->start_ts());

        EXPECT_TRUE(request->keys(0) == "b0000000" || request->keys(0) == "d0000000" || request->keys(0) == "f0000000");

        auto* response = txn_rpc->MutableResponse();
        auto* lock_info = response->add_locks();

        lock_info->set_lock_ts(request->start_ts());
        lock_info->set_primary_lock(fake_lock.primary_lock());
        lock_info->set_key(request->keys(0));
        lock_info->set_lock_ttl(0);
        lock_info->set_use_async_commit(true);
        lock_info->set_lock_type(pb::store::Op::Put);
        lock_info->set_min_commit_ts(13);
        lock_info->add_secondaries("b0000000");
        lock_info->add_secondaries("d0000000");
        lock_info->add_secondaries("f0000000");
        cb();
      })
      .WillRepeatedly([&](Rpc& rpc, std::function<void()> cb) {
        //  resolve keys(commit all keys)

        auto* txn_rpc = dynamic_cast<TxnResolveLockRpc*>(&rpc);
        CHECK_NOTNULL(txn_rpc);

        const auto* request = txn_rpc->Request();
        EXPECT_TRUE(request->has_context());
        auto context = request->context();

        EXPECT_EQ(request->start_ts(), fake_lock.lock_ts());
        EXPECT_EQ(request->commit_ts(), 13);

        for (const auto& k : request->keys()) {
          EXPECT_TRUE(k == "b0000000" || k == "d0000000" || k == "f0000000" || k == "a0000000");
        }

        cb();
      });
  Status s = lock_resolver->ResolveLock(fake_lock, Tso2Timestamp(init_tso));
  EXPECT_TRUE(s.ok());
}

TEST_F(SDKTxnLockResolverTest, AsyncCommitCaseCommittedPartOfOrdinaryKeys) {
  // NOTE: careful!!! key and fake_lock primary key in same region
  std::string key = "b0000000";
  auto fake_lock = PrepareAsyncCommitOrdinaryLockInfo();
  fake_lock.set_key(key);

  std::shared_ptr<Region> region;
  CHECK(meta_cache->LookupRegionByKey(fake_lock.primary_lock(), region).IsOK());
  CHECK_NOTNULL(region.get());

  auto fake_tso = CurrentFakeTso();

  EXPECT_CALL(*meta_rpc_controller, SyncCall).WillRepeatedly([&](Rpc& rpc) {
    auto* t_rpc = dynamic_cast<TsoServiceRpc*>(&rpc);
    EXPECT_EQ(t_rpc->Request()->op_type(), pb::meta::OP_GEN_TSO);
    t_rpc->MutableResponse()->set_count(FLAGS_tso_batch_size);
    auto* ts = t_rpc->MutableResponse()->mutable_start_timestamp();
    *ts = CurrentFakeTso();

    return Status::OK();
  });

  EXPECT_CALL(*rpc_client, SendRpc)
      .WillOnce([&](Rpc& rpc, std::function<void()> cb) {
        auto* txn_rpc = dynamic_cast<TxnCheckTxnStatusRpc*>(&rpc);
        CHECK_NOTNULL(txn_rpc);

        const auto* request = txn_rpc->Request();
        EXPECT_TRUE(request->has_context());
        auto context = request->context();
        EXPECT_EQ(context.region_id(), region->RegionId());
        EXPECT_TRUE(context.has_region_epoch());
        EXPECT_EQ(0, EpochCompare(RegionEpoch(context.region_epoch().version(), context.region_epoch().conf_version()),
                                  region->GetEpoch()));

        EXPECT_EQ(request->primary_key(), fake_lock.primary_lock());
        EXPECT_EQ(request->lock_ts(), fake_lock.lock_ts());

        // async commit primary lock info
        auto* lock_info = txn_rpc->MutableResponse()->mutable_txn_result()->mutable_locked();
        lock_info->set_key("a0000000");
        lock_info->set_primary_lock("a0000000");
        lock_info->set_lock_ts(1);
        // means lock ttl expired
        lock_info->set_lock_ttl(0);
        lock_info->set_txn_size(1);
        lock_info->set_lock_type(pb::store::Op::Put);
        lock_info->set_use_async_commit(true);
        lock_info->set_min_commit_ts(10);
        lock_info->add_secondaries("b0000000");
        lock_info->add_secondaries("d0000000");
        lock_info->add_secondaries("f0000000");

        cb();
      })
      .WillOnce([&](Rpc& rpc, std::function<void()> cb) {
        auto* txn_rpc = dynamic_cast<TxnCheckSecondaryLocksRpc*>(&rpc);
        CHECK_NOTNULL(txn_rpc);

        const auto* request = txn_rpc->Request();
        EXPECT_TRUE(request->has_context());
        auto context = request->context();
        EXPECT_EQ(fake_lock.lock_ts(), request->start_ts());

        EXPECT_TRUE(request->keys(0) == "b0000000" || request->keys(0) == "d0000000" || request->keys(0) == "f0000000");

        auto* response = txn_rpc->MutableResponse();
        auto* lock_info = response->add_locks();

        lock_info->set_lock_ts(request->start_ts());
        lock_info->set_primary_lock(fake_lock.primary_lock());
        lock_info->set_key(request->keys(0));
        lock_info->set_lock_ttl(0);
        lock_info->set_use_async_commit(true);
        lock_info->set_lock_type(pb::store::Op::Put);
        lock_info->set_min_commit_ts(11);
        lock_info->add_secondaries("b0000000");
        lock_info->add_secondaries("d0000000");
        lock_info->add_secondaries("f0000000");

        cb();
      })
      .WillOnce([&](Rpc& rpc, std::function<void()> cb) {
        auto* txn_rpc = dynamic_cast<TxnCheckSecondaryLocksRpc*>(&rpc);
        CHECK_NOTNULL(txn_rpc);

        const auto* request = txn_rpc->Request();
        EXPECT_TRUE(request->has_context());
        auto context = request->context();
        EXPECT_EQ(fake_lock.lock_ts(), request->start_ts());

        EXPECT_TRUE(request->keys(0) == "b0000000" || request->keys(0) == "d0000000" || request->keys(0) == "f0000000");

        auto* response = txn_rpc->MutableResponse();
        response->set_commit_ts(20);  // part of keys committed
        cb();
      })
      .WillOnce([&](Rpc& rpc, std::function<void()> cb) {
        auto* txn_rpc = dynamic_cast<TxnCheckSecondaryLocksRpc*>(&rpc);
        CHECK_NOTNULL(txn_rpc);

        const auto* request = txn_rpc->Request();
        EXPECT_TRUE(request->has_context());
        auto context = request->context();
        EXPECT_EQ(fake_lock.lock_ts(), request->start_ts());

        EXPECT_TRUE(request->keys(0) == "b0000000" || request->keys(0) == "d0000000" || request->keys(0) == "f0000000");

        auto* response = txn_rpc->MutableResponse();
        response->set_commit_ts(20);  // part of keys committed
        cb();
      })
      .WillRepeatedly([&](Rpc& rpc, std::function<void()> cb) {
        //  resolve keys(commit all keys)

        auto* txn_rpc = dynamic_cast<TxnResolveLockRpc*>(&rpc);
        CHECK_NOTNULL(txn_rpc);

        const auto* request = txn_rpc->Request();
        EXPECT_TRUE(request->has_context());
        auto context = request->context();

        EXPECT_EQ(request->start_ts(), fake_lock.lock_ts());
        EXPECT_EQ(request->commit_ts(), 20);

        for (const auto& k : request->keys()) {
          EXPECT_TRUE(k == "b0000000" || k == "d0000000" || k == "f0000000" || k == "a0000000");
        }

        cb();
      });
  Status s = lock_resolver->ResolveLock(fake_lock, Tso2Timestamp(init_tso));
  EXPECT_TRUE(s.ok());
}

TEST_F(SDKTxnLockResolverTest, AsyncCommitCaseRollbackedPrimaryLock) {
  // NOTE: careful!!! key and fake_lock primary key in same region
  std::string key = "b0000000";
  auto fake_lock = PrepareAsyncCommitOrdinaryLockInfo();
  fake_lock.set_key(key);

  std::shared_ptr<Region> region;
  CHECK(meta_cache->LookupRegionByKey(fake_lock.primary_lock(), region).IsOK());
  CHECK_NOTNULL(region.get());

  auto fake_tso = CurrentFakeTso();

  EXPECT_CALL(*meta_rpc_controller, SyncCall).WillOnce([&](Rpc& rpc) {
    auto* t_rpc = dynamic_cast<TsoServiceRpc*>(&rpc);
    EXPECT_EQ(t_rpc->Request()->op_type(), pb::meta::OP_GEN_TSO);
    t_rpc->MutableResponse()->set_count(FLAGS_tso_batch_size);
    auto* ts = t_rpc->MutableResponse()->mutable_start_timestamp();
    *ts = fake_tso;

    return Status::OK();
  });

  EXPECT_CALL(*rpc_client, SendRpc)
      .WillOnce([&](Rpc& rpc, std::function<void()> cb) {
        auto* txn_rpc = dynamic_cast<TxnCheckTxnStatusRpc*>(&rpc);
        CHECK_NOTNULL(txn_rpc);

        const auto* request = txn_rpc->Request();
        EXPECT_TRUE(request->has_context());
        auto context = request->context();
        EXPECT_EQ(context.region_id(), region->RegionId());
        EXPECT_TRUE(context.has_region_epoch());
        EXPECT_EQ(0, EpochCompare(RegionEpoch(context.region_epoch().version(), context.region_epoch().conf_version()),
                                  region->GetEpoch()));

        EXPECT_EQ(request->primary_key(), fake_lock.primary_lock());
        EXPECT_EQ(request->lock_ts(), fake_lock.lock_ts());
        EXPECT_EQ(request->current_ts(), Tso2Timestamp(fake_tso));
        EXPECT_EQ(request->caller_start_ts(), Tso2Timestamp(init_tso));

        // primary lock is committed
        txn_rpc->MutableResponse()->set_commit_ts(0);

        cb();
      })
      .WillOnce([&](Rpc& rpc, std::function<void()> cb) {
        //  resolve conlict key

        auto* txn_rpc = dynamic_cast<TxnResolveLockRpc*>(&rpc);
        CHECK_NOTNULL(txn_rpc);

        const auto* request = txn_rpc->Request();
        EXPECT_TRUE(request->has_context());
        auto context = request->context();
        EXPECT_EQ(context.region_id(), region->RegionId());
        EXPECT_TRUE(context.has_region_epoch());
        EXPECT_EQ(0, EpochCompare(RegionEpoch(context.region_epoch().version(), context.region_epoch().conf_version()),
                                  region->GetEpoch()));

        EXPECT_EQ(request->start_ts(), fake_lock.lock_ts());
        EXPECT_EQ(request->commit_ts(), 0);
        EXPECT_EQ(request->keys_size(), 1);

        const auto& key = request->keys(0);
        EXPECT_EQ(key, fake_lock.key());

        cb();
      });
  Status s = lock_resolver->ResolveLock(fake_lock, Tso2Timestamp(init_tso));
  EXPECT_TRUE(s.ok());
}

TEST_F(SDKTxnLockResolverTest, AsyncCommitCaseRollbackedPartOfOrdinaryKeys) {
  // NOTE: careful!!! key and fake_lock primary key in same region
  std::string key = "b0000000";
  auto fake_lock = PrepareAsyncCommitOrdinaryLockInfo();
  fake_lock.set_key(key);

  std::shared_ptr<Region> region;
  CHECK(meta_cache->LookupRegionByKey(fake_lock.primary_lock(), region).IsOK());
  CHECK_NOTNULL(region.get());

  auto fake_tso = CurrentFakeTso();

  EXPECT_CALL(*meta_rpc_controller, SyncCall).WillRepeatedly([&](Rpc& rpc) {
    auto* t_rpc = dynamic_cast<TsoServiceRpc*>(&rpc);
    EXPECT_EQ(t_rpc->Request()->op_type(), pb::meta::OP_GEN_TSO);
    t_rpc->MutableResponse()->set_count(FLAGS_tso_batch_size);
    auto* ts = t_rpc->MutableResponse()->mutable_start_timestamp();
    *ts = CurrentFakeTso();

    return Status::OK();
  });

  EXPECT_CALL(*rpc_client, SendRpc)
      .WillOnce([&](Rpc& rpc, std::function<void()> cb) {
        auto* txn_rpc = dynamic_cast<TxnCheckTxnStatusRpc*>(&rpc);
        CHECK_NOTNULL(txn_rpc);

        const auto* request = txn_rpc->Request();
        EXPECT_TRUE(request->has_context());
        auto context = request->context();
        EXPECT_EQ(context.region_id(), region->RegionId());
        EXPECT_TRUE(context.has_region_epoch());
        EXPECT_EQ(0, EpochCompare(RegionEpoch(context.region_epoch().version(), context.region_epoch().conf_version()),
                                  region->GetEpoch()));

        EXPECT_EQ(request->primary_key(), fake_lock.primary_lock());
        EXPECT_EQ(request->lock_ts(), fake_lock.lock_ts());

        // async commit primary lock info
        auto* lock_info = txn_rpc->MutableResponse()->mutable_txn_result()->mutable_locked();
        lock_info->set_key("a0000000");
        lock_info->set_primary_lock("a0000000");
        lock_info->set_lock_ts(1);
        // means lock ttl expired
        lock_info->set_lock_ttl(0);
        lock_info->set_txn_size(1);
        lock_info->set_lock_type(pb::store::Op::Put);
        lock_info->set_use_async_commit(true);
        lock_info->set_min_commit_ts(10);
        lock_info->add_secondaries("b0000000");
        lock_info->add_secondaries("d0000000");
        lock_info->add_secondaries("f0000000");

        cb();
      })
      .WillOnce([&](Rpc& rpc, std::function<void()> cb) {
        auto* txn_rpc = dynamic_cast<TxnCheckSecondaryLocksRpc*>(&rpc);
        CHECK_NOTNULL(txn_rpc);

        const auto* request = txn_rpc->Request();
        EXPECT_TRUE(request->has_context());
        auto context = request->context();
        EXPECT_EQ(fake_lock.lock_ts(), request->start_ts());

        EXPECT_TRUE(request->keys(0) == "b0000000" || request->keys(0) == "d0000000" || request->keys(0) == "f0000000");

        auto* response = txn_rpc->MutableResponse();
        auto* lock_info = response->add_locks();

        lock_info->set_lock_ts(request->start_ts());
        lock_info->set_primary_lock(fake_lock.primary_lock());
        lock_info->set_key(request->keys(0));
        lock_info->set_lock_ttl(0);
        lock_info->set_use_async_commit(true);
        lock_info->set_lock_type(pb::store::Op::Put);
        lock_info->set_min_commit_ts(11);
        lock_info->add_secondaries("b0000000");
        lock_info->add_secondaries("d0000000");
        lock_info->add_secondaries("f0000000");

        cb();
      })
      .WillOnce([&](Rpc& rpc, std::function<void()> cb) {
        auto* txn_rpc = dynamic_cast<TxnCheckSecondaryLocksRpc*>(&rpc);
        CHECK_NOTNULL(txn_rpc);

        const auto* request = txn_rpc->Request();
        EXPECT_TRUE(request->has_context());
        auto context = request->context();
        EXPECT_EQ(fake_lock.lock_ts(), request->start_ts());

        EXPECT_TRUE(request->keys(0) == "b0000000" || request->keys(0) == "d0000000" || request->keys(0) == "f0000000");

        auto* response = txn_rpc->MutableResponse();
        response->set_commit_ts(0);  // part of keys rollbacked
        cb();
      })
      .WillOnce([&](Rpc& rpc, std::function<void()> cb) {
        auto* txn_rpc = dynamic_cast<TxnCheckSecondaryLocksRpc*>(&rpc);
        CHECK_NOTNULL(txn_rpc);

        const auto* request = txn_rpc->Request();
        EXPECT_TRUE(request->has_context());
        auto context = request->context();
        EXPECT_EQ(fake_lock.lock_ts(), request->start_ts());

        EXPECT_TRUE(request->keys(0) == "b0000000" || request->keys(0) == "d0000000" || request->keys(0) == "f0000000");

        auto* response = txn_rpc->MutableResponse();
        response->set_commit_ts(0);  // part of keys rollbacked
        cb();
      })
      .WillRepeatedly([&](Rpc& rpc, std::function<void()> cb) {
        //  resolve keys(commit all keys)

        auto* txn_rpc = dynamic_cast<TxnResolveLockRpc*>(&rpc);
        CHECK_NOTNULL(txn_rpc);

        const auto* request = txn_rpc->Request();
        EXPECT_TRUE(request->has_context());
        auto context = request->context();

        EXPECT_EQ(request->start_ts(), fake_lock.lock_ts());
        EXPECT_EQ(request->commit_ts(), 0);

        for (const auto& k : request->keys()) {
          EXPECT_TRUE(k == "b0000000" || k == "d0000000" || k == "f0000000" || k == "a0000000");
        }

        cb();
      });
  Status s = lock_resolver->ResolveLock(fake_lock, Tso2Timestamp(init_tso));
  EXPECT_TRUE(s.ok());
}

TEST_F(SDKTxnLockResolverTest, AsyncCommitCasePartOfOrdinaryKeysNotFound) {
  // NOTE: careful!!! key and fake_lock primary key in same region
  std::string key = "b0000000";
  auto fake_lock = PrepareAsyncCommitOrdinaryLockInfo();
  fake_lock.set_key(key);

  std::shared_ptr<Region> region;
  CHECK(meta_cache->LookupRegionByKey(fake_lock.primary_lock(), region).IsOK());
  CHECK_NOTNULL(region.get());

  auto fake_tso = CurrentFakeTso();

  EXPECT_CALL(*meta_rpc_controller, SyncCall).WillRepeatedly([&](Rpc& rpc) {
    auto* t_rpc = dynamic_cast<TsoServiceRpc*>(&rpc);
    EXPECT_EQ(t_rpc->Request()->op_type(), pb::meta::OP_GEN_TSO);
    t_rpc->MutableResponse()->set_count(FLAGS_tso_batch_size);
    auto* ts = t_rpc->MutableResponse()->mutable_start_timestamp();
    *ts = CurrentFakeTso();

    return Status::OK();
  });

  EXPECT_CALL(*rpc_client, SendRpc)
      .WillOnce([&](Rpc& rpc, std::function<void()> cb) {
        auto* txn_rpc = dynamic_cast<TxnCheckTxnStatusRpc*>(&rpc);
        CHECK_NOTNULL(txn_rpc);

        const auto* request = txn_rpc->Request();
        EXPECT_TRUE(request->has_context());
        auto context = request->context();
        EXPECT_EQ(context.region_id(), region->RegionId());
        EXPECT_TRUE(context.has_region_epoch());
        EXPECT_EQ(0, EpochCompare(RegionEpoch(context.region_epoch().version(), context.region_epoch().conf_version()),
                                  region->GetEpoch()));

        EXPECT_EQ(request->primary_key(), fake_lock.primary_lock());
        EXPECT_EQ(request->lock_ts(), fake_lock.lock_ts());

        // async commit primary lock info
        auto* lock_info = txn_rpc->MutableResponse()->mutable_txn_result()->mutable_locked();
        lock_info->set_key("a0000000");
        lock_info->set_primary_lock("a0000000");
        lock_info->set_lock_ts(1);
        // means lock ttl expired
        lock_info->set_lock_ttl(0);
        lock_info->set_txn_size(1);
        lock_info->set_lock_type(pb::store::Op::Put);
        lock_info->set_use_async_commit(true);
        lock_info->set_min_commit_ts(10);
        lock_info->add_secondaries("b0000000");
        lock_info->add_secondaries("d0000000");
        lock_info->add_secondaries("f0000000");

        cb();
      })
      .WillOnce([&](Rpc& rpc, std::function<void()> cb) {
        auto* txn_rpc = dynamic_cast<TxnCheckSecondaryLocksRpc*>(&rpc);
        CHECK_NOTNULL(txn_rpc);

        const auto* request = txn_rpc->Request();
        EXPECT_TRUE(request->has_context());
        auto context = request->context();
        EXPECT_EQ(fake_lock.lock_ts(), request->start_ts());

        EXPECT_TRUE(request->keys(0) == "b0000000" || request->keys(0) == "d0000000" || request->keys(0) == "f0000000");

        auto* response = txn_rpc->MutableResponse();
        auto* lock_info = response->add_locks();

        lock_info->set_lock_ts(request->start_ts());
        lock_info->set_primary_lock(fake_lock.primary_lock());
        lock_info->set_key(request->keys(0));
        lock_info->set_lock_ttl(0);
        lock_info->set_use_async_commit(true);
        lock_info->set_lock_type(pb::store::Op::Put);
        lock_info->set_min_commit_ts(11);
        lock_info->add_secondaries("b0000000");
        lock_info->add_secondaries("d0000000");
        lock_info->add_secondaries("f0000000");

        cb();
      })
      .WillOnce([&](Rpc& rpc, std::function<void()> cb) {
        auto* txn_rpc = dynamic_cast<TxnCheckSecondaryLocksRpc*>(&rpc);
        CHECK_NOTNULL(txn_rpc);

        const auto* request = txn_rpc->Request();
        EXPECT_TRUE(request->has_context());
        auto context = request->context();
        EXPECT_EQ(fake_lock.lock_ts(), request->start_ts());

        EXPECT_TRUE(request->keys(0) == "b0000000" || request->keys(0) == "d0000000" || request->keys(0) == "f0000000");

        auto* response = txn_rpc->MutableResponse();
        auto* txn_not_found = response->mutable_txn_result()->mutable_txn_not_found();
        txn_not_found->set_key(request->keys(0));
        txn_not_found->set_start_ts(fake_lock.lock_ts());
        txn_not_found->set_primary_key(fake_lock.primary_lock());
        cb();
      })
      .WillOnce([&](Rpc& rpc, std::function<void()> cb) {
        auto* txn_rpc = dynamic_cast<TxnCheckSecondaryLocksRpc*>(&rpc);
        CHECK_NOTNULL(txn_rpc);

        const auto* request = txn_rpc->Request();
        EXPECT_TRUE(request->has_context());
        auto context = request->context();
        EXPECT_EQ(fake_lock.lock_ts(), request->start_ts());

        EXPECT_TRUE(request->keys(0) == "b0000000" || request->keys(0) == "d0000000" || request->keys(0) == "f0000000");

        auto* response = txn_rpc->MutableResponse();
        auto* lock_info = response->add_locks();

        lock_info->set_lock_ts(request->start_ts());
        lock_info->set_primary_lock(fake_lock.primary_lock());
        lock_info->set_key(request->keys(0));
        lock_info->set_lock_ttl(0);
        lock_info->set_use_async_commit(true);
        lock_info->set_lock_type(pb::store::Op::Put);
        lock_info->set_min_commit_ts(13);
        lock_info->add_secondaries("b0000000");
        lock_info->add_secondaries("d0000000");
        lock_info->add_secondaries("f0000000");
        cb();
      })
      .WillRepeatedly([&](Rpc& rpc, std::function<void()> cb) {
        //  resolve keys(commit all keys)

        auto* txn_rpc = dynamic_cast<TxnResolveLockRpc*>(&rpc);
        CHECK_NOTNULL(txn_rpc);

        const auto* request = txn_rpc->Request();
        EXPECT_TRUE(request->has_context());
        auto context = request->context();

        EXPECT_EQ(request->start_ts(), fake_lock.lock_ts());
        EXPECT_EQ(request->commit_ts(), 0);

        for (const auto& k : request->keys()) {
          EXPECT_TRUE(k == "b0000000" || k == "d0000000" || k == "f0000000" || k == "a0000000");
        }

        cb();
      });
  Status s = lock_resolver->ResolveLock(fake_lock, Tso2Timestamp(init_tso));
  EXPECT_TRUE(s.ok());
}

TEST_F(SDKTxnLockResolverTest, AsyncCommitCaseTransferSyncCommit) {
  // NOTE: careful!!! key and fake_lock primary key in same region
  std::string key = "b0000000";
  auto fake_lock = PrepareAsyncCommitOrdinaryLockInfo();
  fake_lock.set_key(key);

  std::shared_ptr<Region> region;
  CHECK(meta_cache->LookupRegionByKey(fake_lock.primary_lock(), region).IsOK());
  CHECK_NOTNULL(region.get());

  auto fake_tso = CurrentFakeTso();

  EXPECT_CALL(*meta_rpc_controller, SyncCall).WillRepeatedly([&](Rpc& rpc) {
    auto* t_rpc = dynamic_cast<TsoServiceRpc*>(&rpc);
    EXPECT_EQ(t_rpc->Request()->op_type(), pb::meta::OP_GEN_TSO);
    t_rpc->MutableResponse()->set_count(FLAGS_tso_batch_size);
    auto* ts = t_rpc->MutableResponse()->mutable_start_timestamp();
    *ts = CurrentFakeTso();

    return Status::OK();
  });

  EXPECT_CALL(*rpc_client, SendRpc)
      .WillOnce([&](Rpc& rpc, std::function<void()> cb) {
        auto* txn_rpc = dynamic_cast<TxnCheckTxnStatusRpc*>(&rpc);
        CHECK_NOTNULL(txn_rpc);

        const auto* request = txn_rpc->Request();
        EXPECT_TRUE(request->has_context());
        auto context = request->context();
        EXPECT_EQ(context.region_id(), region->RegionId());
        EXPECT_TRUE(context.has_region_epoch());
        EXPECT_EQ(0, EpochCompare(RegionEpoch(context.region_epoch().version(), context.region_epoch().conf_version()),
                                  region->GetEpoch()));

        EXPECT_EQ(request->primary_key(), fake_lock.primary_lock());
        EXPECT_EQ(request->lock_ts(), fake_lock.lock_ts());

        // async commit primary lock info
        auto* lock_info = txn_rpc->MutableResponse()->mutable_txn_result()->mutable_locked();
        lock_info->set_key("a0000000");
        lock_info->set_primary_lock("a0000000");
        lock_info->set_lock_ts(1);
        // means lock ttl expired
        lock_info->set_lock_ttl(0);
        lock_info->set_txn_size(1);
        lock_info->set_lock_type(pb::store::Op::Put);
        lock_info->set_use_async_commit(true);  
        lock_info->set_min_commit_ts(10);
        lock_info->add_secondaries("b0000000");
        lock_info->add_secondaries("d0000000");
        lock_info->add_secondaries("f0000000");

        cb();
      })
      .WillOnce([&](Rpc& rpc, std::function<void()> cb) {
        auto* txn_rpc = dynamic_cast<TxnCheckSecondaryLocksRpc*>(&rpc);
        CHECK_NOTNULL(txn_rpc);

        const auto* request = txn_rpc->Request();
        EXPECT_TRUE(request->has_context());
        auto context = request->context();
        EXPECT_EQ(fake_lock.lock_ts(), request->start_ts());

        EXPECT_TRUE(request->keys(0) == "b0000000" || request->keys(0) == "d0000000" || request->keys(0) == "f0000000");

        auto* response = txn_rpc->MutableResponse();
        auto* lock_info = response->add_locks();

        lock_info->set_lock_ts(request->start_ts());
        lock_info->set_primary_lock(fake_lock.primary_lock());
        lock_info->set_key(request->keys(0));
        lock_info->set_lock_ttl(0);
        lock_info->set_use_async_commit(false);// transfer to sync commit
        lock_info->set_lock_type(pb::store::Op::Put);
        lock_info->set_min_commit_ts(11);
        lock_info->add_secondaries("b0000000");
        lock_info->add_secondaries("d0000000");
        lock_info->add_secondaries("f0000000");

        cb();
      })
      .WillOnce([&](Rpc& rpc, std::function<void()> cb) {
        auto* txn_rpc = dynamic_cast<TxnCheckSecondaryLocksRpc*>(&rpc);
        CHECK_NOTNULL(txn_rpc);

        const auto* request = txn_rpc->Request();
        EXPECT_TRUE(request->has_context());
        auto context = request->context();
        EXPECT_EQ(fake_lock.lock_ts(), request->start_ts());

        EXPECT_TRUE(request->keys(0) == "b0000000" || request->keys(0) == "d0000000" || request->keys(0) == "f0000000");

        auto* response = txn_rpc->MutableResponse();
        auto* lock_info = response->add_locks();

        lock_info->set_lock_ts(request->start_ts());
        lock_info->set_primary_lock(fake_lock.primary_lock());
        lock_info->set_key(request->keys(0));
        lock_info->set_lock_ttl(0);
        lock_info->set_use_async_commit(true);
        lock_info->set_lock_type(pb::store::Op::Put);
        lock_info->set_min_commit_ts(12);
        lock_info->add_secondaries("b0000000");
        lock_info->add_secondaries("d0000000");
        lock_info->add_secondaries("f0000000");
        cb();
      })
      .WillOnce([&](Rpc& rpc, std::function<void()> cb) {
        auto* txn_rpc = dynamic_cast<TxnCheckSecondaryLocksRpc*>(&rpc);
        CHECK_NOTNULL(txn_rpc);

        const auto* request = txn_rpc->Request();
        EXPECT_TRUE(request->has_context());
        auto context = request->context();
        EXPECT_EQ(fake_lock.lock_ts(), request->start_ts());

        EXPECT_TRUE(request->keys(0) == "b0000000" || request->keys(0) == "d0000000" || request->keys(0) == "f0000000");

        auto* response = txn_rpc->MutableResponse();
        auto* lock_info = response->add_locks();

        lock_info->set_lock_ts(request->start_ts());
        lock_info->set_primary_lock(fake_lock.primary_lock());
        lock_info->set_key(request->keys(0));
        lock_info->set_lock_ttl(0);
        lock_info->set_use_async_commit(true);
        lock_info->set_lock_type(pb::store::Op::Put);
        lock_info->set_min_commit_ts(13);
        lock_info->add_secondaries("b0000000");
        lock_info->add_secondaries("d0000000");
        lock_info->add_secondaries("f0000000");
        cb();
      })
      .WillOnce([&](Rpc& rpc, std::function<void()> cb) {
        auto* txn_rpc = dynamic_cast<TxnCheckTxnStatusRpc*>(&rpc);
        CHECK_NOTNULL(txn_rpc);

        const auto* request = txn_rpc->Request();
        EXPECT_TRUE(request->has_context());
        auto context = request->context();
        EXPECT_EQ(context.region_id(), region->RegionId());
        EXPECT_TRUE(context.has_region_epoch());
        EXPECT_EQ(0, EpochCompare(RegionEpoch(context.region_epoch().version(), context.region_epoch().conf_version()),
                                  region->GetEpoch()));

        EXPECT_EQ(request->primary_key(), fake_lock.primary_lock());
        EXPECT_EQ(request->lock_ts(), fake_lock.lock_ts());
        EXPECT_EQ(request->force_sync_commit(), true);

        txn_rpc->MutableResponse()->set_commit_ts(0);

        cb();
      })
      .WillRepeatedly([&](Rpc& rpc, std::function<void()> cb) {
        //  resolve keys(commit all keys)

        auto* txn_rpc = dynamic_cast<TxnResolveLockRpc*>(&rpc);
        CHECK_NOTNULL(txn_rpc);

        const auto* request = txn_rpc->Request();
        EXPECT_TRUE(request->has_context());
        auto context = request->context();

        EXPECT_EQ(request->start_ts(), fake_lock.lock_ts());
        EXPECT_EQ(request->commit_ts(), 0);

        for (const auto& k : request->keys()) {
          EXPECT_TRUE(k == "b0000000" || k == "d0000000" || k == "f0000000" || k == "a0000000");
        }

        cb();
      });
  Status s = lock_resolver->ResolveLock(fake_lock, Tso2Timestamp(init_tso));
  EXPECT_TRUE(s.ok());
}

}  // namespace sdk

}  // namespace dingodb