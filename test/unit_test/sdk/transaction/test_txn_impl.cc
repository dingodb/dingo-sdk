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

#include <unistd.h>

#include <cstdint>
#include <memory>

#include "dingosdk/client.h"
#include "dingosdk/status.h"
#include "glog/logging.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "proto//meta.pb.h"
#include "proto/store.pb.h"
#include "sdk/common/common.h"
#include "sdk/common/param_config.h"
#include "sdk/rpc/coordinator_rpc.h"
#include "sdk/rpc/store_rpc.h"
#include "sdk/transaction/txn_impl.h"
#include "test_base.h"
#include "test_common.h"

static const int64_t kStep = 10;

namespace dingodb {
namespace sdk {

class SDKTxnImplTest : public TestBase {
 public:
  SDKTxnImplTest() = default;
  ~SDKTxnImplTest() override = default;

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

TEST_F(SDKTxnImplTest, BeginFail) {
  ON_CALL(*meta_rpc_controller, SyncCall).WillByDefault([&](Rpc& rpc) {
    auto* t_rpc = dynamic_cast<TsoServiceRpc*>(&rpc);
    EXPECT_EQ(t_rpc->Request()->op_type(), pb::meta::OP_GEN_TSO);
    return Status::NetworkError("mock error");
  });

  Transaction* txn = nullptr;
  EXPECT_FALSE(client->NewTransaction(options, &txn).ok());
  delete txn;
}

TEST_F(SDKTxnImplTest, BeginSuccess) {
  Transaction* txn;
  EXPECT_TRUE(client->NewTransaction(options, &txn).ok());
  delete txn;
}

TEST_F(SDKTxnImplTest, Get) {
  std::string key = "b";
  std::string value;

  std::shared_ptr<Region> region;
  CHECK(meta_cache->LookupRegionByKey(key, region).IsOK());
  CHECK_NOTNULL(region.get());

  auto txn = NewTransactionImpl(options);

  EXPECT_CALL(*rpc_client, SendRpc).WillOnce([&](Rpc& rpc, std::function<void()> cb) {
    auto* txn_rpc = dynamic_cast<TxnGetRpc*>(&rpc);
    CHECK_NOTNULL(txn_rpc);

    const auto* request = txn_rpc->Request();
    EXPECT_TRUE(request->has_context());
    auto context = request->context();
    EXPECT_EQ(context.region_id(), region->RegionId());
    EXPECT_TRUE(context.has_region_epoch());
    EXPECT_EQ(0, EpochCompare(RegionEpoch(context.region_epoch().version(), context.region_epoch().conf_version()),
                              region->GetEpoch()));

    EXPECT_EQ(request->key(), key);
    EXPECT_EQ(request->start_ts(), txn->TEST_GetStartTs());

    txn_rpc->MutableResponse()->set_value("pong");
    cb();
  });

  EXPECT_EQ(txn->TEST_IsActiveState(), true);

  Status tmp = txn->Get("b", value);
  EXPECT_TRUE(tmp.ok());
  EXPECT_EQ(value, "pong");
}

TEST_F(SDKTxnImplTest, SingleOP) {
  auto txn = NewTransactionImpl(options);

  EXPECT_CALL(*rpc_client, SendRpc).WillOnce([&](Rpc& rpc, std::function<void()> cb) {
    auto* txn_rpc = dynamic_cast<TxnGetRpc*>(&rpc);
    CHECK_NOTNULL(txn_rpc);

    const auto* request = txn_rpc->Request();
    EXPECT_TRUE(request->has_context());

    EXPECT_EQ(request->start_ts(), txn->TEST_GetStartTs());
    cb();
  });

  EXPECT_EQ(txn->TEST_IsActiveState(), true);

  {
    std::string value;
    Status tmp = txn->Get("a", value);
    EXPECT_TRUE(tmp.IsNotFound());
  }

  {
    txn->Put("a", "ra");
    std::string value;
    Status tmp = txn->Get("a", value);
    EXPECT_TRUE(tmp.ok());
    EXPECT_EQ(value, "ra");
  }

  {
    txn->PutIfAbsent("a", "newa");
    std::string value;
    Status tmp = txn->Get("a", value);
    EXPECT_TRUE(tmp.ok());
    EXPECT_EQ(value, "ra");

    txn->PutIfAbsent("b", "rb");
    tmp = txn->Get("b", value);
    EXPECT_TRUE(tmp.ok());
    EXPECT_EQ(value, "rb");
  }

  {
    txn->Delete("a");

    std::string value;
    Status tmp = txn->Get("a", value);
    EXPECT_TRUE(tmp.IsNotFound());

    tmp = txn->Get("b", value);
    EXPECT_TRUE(tmp.ok());
    EXPECT_EQ(value, "rb");
  }

  {
    txn->Delete("b");
    std::string value;
    Status tmp = txn->Get("b", value);
    EXPECT_TRUE(tmp.IsNotFound());
  }
}

TEST_F(SDKTxnImplTest, BatchGet) {
  std::vector<std::string> keys;
  keys.emplace_back("b");
  keys.emplace_back("d");
  keys.emplace_back("f");

  auto txn = NewTransactionImpl(options);

  EXPECT_CALL(*rpc_client, SendRpc).WillRepeatedly([&](Rpc& rpc, std::function<void()> cb) {
    auto* txn_rpc = dynamic_cast<TxnBatchGetRpc*>(&rpc);
    CHECK_NOTNULL(txn_rpc);

    const auto* request = txn_rpc->Request();
    EXPECT_TRUE(request->has_context());
    EXPECT_EQ(request->start_ts(), txn->TEST_GetStartTs());

    EXPECT_EQ(1, txn_rpc->Request()->keys_size());
    const auto& key = txn_rpc->Request()->keys(0);
    if (key == "b") {
      auto* kv = txn_rpc->MutableResponse()->add_kvs();
      kv->set_key("b");
      kv->set_value("b");
    } else if (key == "d") {
      auto* kv = txn_rpc->MutableResponse()->add_kvs();
      kv->set_key("d");
      kv->set_value("d");
    } else if (key == "f") {
      auto* kv = txn_rpc->MutableResponse()->add_kvs();
      kv->set_key("f");
      kv->set_value("f");
    }

    cb();
  });

  EXPECT_EQ(txn->TEST_IsActiveState(), true);

  std::vector<KVPair> kvs;
  Status tmp = txn->BatchGet(keys, kvs);
  EXPECT_EQ(keys.size(), kvs.size());

  for (const auto& kv : kvs) {
    EXPECT_EQ(kv.key, kv.value);
  }
}

TEST_F(SDKTxnImplTest, BatchGetFromBuffer) {
  std::vector<KVPair> kvs;
  kvs.push_back({"b", "b"});
  kvs.push_back({"d", "d"});
  kvs.push_back({"f", "f"});

  EXPECT_CALL(*rpc_client, SendRpc).Times(0);

  auto txn = NewTransactionImpl(options);

  EXPECT_EQ(txn->TEST_IsActiveState(), true);

  txn->BatchPut(kvs);

  std::vector<std::string> keys;
  keys.reserve(kvs.size());
  for (const auto& kv : kvs) {
    keys.push_back(kv.key);
  }

  std::vector<KVPair> tmp;
  Status s = txn->BatchGet(keys, tmp);
  EXPECT_EQ(keys.size(), tmp.size());

  for (const auto& kv : tmp) {
    EXPECT_EQ(kv.key, kv.value);
  }
}

TEST_F(SDKTxnImplTest, BatchOp) {
  std::vector<KVPair> kvs;
  kvs.push_back({"b", "b"});
  kvs.push_back({"d", "d"});
  kvs.push_back({"f", "f"});

  std::vector<std::string> keys;
  keys.reserve(kvs.size());
  for (const auto& kv : kvs) {
    keys.push_back(kv.key);
  }

  EXPECT_CALL(*rpc_client, SendRpc).Times(0);

  auto txn = NewTransactionImpl(options);

  EXPECT_EQ(txn->TEST_IsActiveState(), true);

  {
    txn->BatchPut(kvs);
    txn->BatchDelete(keys);

    std::vector<KVPair> tmp;
    Status s = txn->BatchGet(keys, tmp);
    EXPECT_TRUE(s.ok());
    EXPECT_EQ(tmp.size(), 0);
  }

  {
    kvs.push_back({"a", "a"});
    txn->BatchPutIfAbsent(kvs);

    std::vector<KVPair> tmp;
    keys.push_back("a");
    Status s = txn->BatchGet(keys, tmp);
    EXPECT_EQ(tmp.size(), keys.size());

    for (const auto& kv : tmp) {
      EXPECT_EQ(kv.key, kv.value);
    }

    txn->BatchDelete(keys);
    s = txn->BatchGet(keys, tmp);
    EXPECT_TRUE(s.ok());
    EXPECT_EQ(tmp.size(), 0);
  }
}

TEST_F(SDKTxnImplTest, CommitEmpty) {
  EXPECT_CALL(*rpc_client, SendRpc).Times(0);

  auto txn = NewTransactionImpl(options);

  EXPECT_EQ(txn->TEST_IsActiveState(), true);

  Status s = txn->PreCommit();
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(txn->TEST_IsPreCommittedState(), true);
  s = txn->Commit();
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(txn->TEST_IsCommittedState(), true);
}

TEST_F(SDKTxnImplTest, CommitWithData) {
  auto txn = NewTransactionImpl(options);

  EXPECT_EQ(txn->TEST_IsActiveState(), true);

  {
    txn->Put("a", "a");
    txn->Delete("a");

    txn->Put("b", "b");
    txn->PutIfAbsent("b", "newb");

    txn->PutIfAbsent("d", "d");
  }

  EXPECT_CALL(*rpc_client, SendRpc)
      .WillOnce([&](Rpc& rpc, std::function<void()> cb) {
        // precommit primary key
        TxnPrewriteRpc* txn_rpc = dynamic_cast<TxnPrewriteRpc*>(&rpc);
        CHECK_NOTNULL(txn_rpc);
        const auto* request = txn_rpc->Request();
        EXPECT_TRUE(request->has_context());
        EXPECT_EQ(request->start_ts(), txn->TEST_GetStartTs());
        EXPECT_EQ(request->txn_size(), 1);
        EXPECT_EQ(request->primary_lock(), txn->TEST_GetPrimaryKey());

        for (const auto& mutation : request->mutations()) {
          if (mutation.key() == "a") {
            EXPECT_EQ(mutation.op(), pb::store::Op::Delete);
          } else if (mutation.key() == "b") {
            EXPECT_EQ(mutation.op(), pb::store::Op::Put);
            EXPECT_EQ(mutation.value(), "b");
          } else if (mutation.key() == "d") {
            EXPECT_EQ(mutation.op(), pb::store::Op::PutIfAbsent);
            EXPECT_EQ(mutation.value(), "d");
          } else {
            CHECK(false) << "unknow test mutation:" << mutation.DebugString();
          }
        }

        cb();
      })
      .WillRepeatedly([&](Rpc& rpc, std::function<void()> cb) {
        // precommit ordinary key
        TxnPrewriteRpc* txn_rpc = dynamic_cast<TxnPrewriteRpc*>(&rpc);
        if (nullptr == txn_rpc) {
          // commit
          TxnCommitRpc* txn_rpc = dynamic_cast<TxnCommitRpc*>(&rpc);
          CHECK_NOTNULL(txn_rpc);
          const auto* request = txn_rpc->Request();
          EXPECT_TRUE(request->has_context());
          EXPECT_EQ(request->start_ts(), txn->TEST_GetStartTs());
          EXPECT_EQ(request->commit_ts(), txn->TEST_GetCommitTs());

          for (const auto& key : request->keys()) {
            EXPECT_TRUE(key == "a" || key == "b" || key == "d");
          }

          cb();
        } else {
          // precommit
          CHECK_NOTNULL(txn_rpc);
          const auto* request = txn_rpc->Request();
          EXPECT_TRUE(request->has_context());
          EXPECT_EQ(request->start_ts(), txn->TEST_GetStartTs());
          EXPECT_EQ(request->txn_size(), 2);
          EXPECT_EQ(request->primary_lock(), txn->TEST_GetPrimaryKey());

          for (const auto& mutation : request->mutations()) {
            if (mutation.key() == "a") {
              EXPECT_EQ(mutation.op(), pb::store::Op::Delete);
            } else if (mutation.key() == "b") {
              EXPECT_EQ(mutation.op(), pb::store::Op::Put);
              EXPECT_EQ(mutation.value(), "b");
            } else if (mutation.key() == "d") {
              EXPECT_EQ(mutation.op(), pb::store::Op::PutIfAbsent);
              EXPECT_EQ(mutation.value(), "d");
            } else {
              CHECK(false) << "unknow test mutation:" << mutation.DebugString();
            }
          }
          cb();
        }
      });

  Status s = txn->PreCommit();
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(txn->TEST_IsPreCommittedState(), true);

  s = txn->Commit();
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(txn->TEST_IsCommittedState(), true);
}

TEST_F(SDKTxnImplTest, PrimaryKeyLockConflict) {
  auto txn = NewTransactionImpl(options);

  EXPECT_EQ(txn->TEST_IsActiveState(), true);

  {
    txn->Put("a", "a");
    txn->Delete("a");

    txn->Put("b", "b");
    txn->PutIfAbsent("b", "newb");

    txn->PutIfAbsent("d", "d");
  }

  auto mock_lock = PrepareLockInfo();
  mock_lock.set_key(txn->TEST_GetPrimaryKey());

  EXPECT_CALL(*rpc_client, SendRpc)
      .WillOnce([&](Rpc& rpc, std::function<void()> cb) {
        TxnPrewriteRpc* txn_rpc = dynamic_cast<TxnPrewriteRpc*>(&rpc);
        // precommit primary key lock confil
        CHECK_NOTNULL(txn_rpc);
        const auto* request = txn_rpc->Request();
        EXPECT_TRUE(request->has_context());
        EXPECT_EQ(request->start_ts(), txn->TEST_GetStartTs());
        EXPECT_EQ(request->txn_size(), 1);
        EXPECT_EQ(request->primary_lock(), txn->TEST_GetPrimaryKey());

        const auto& mutation = request->mutations(0);
        EXPECT_EQ(mutation.key(), txn->TEST_GetPrimaryKey());

        auto* txn_result = txn_rpc->MutableResponse()->add_txn_result();
        auto* lock_info = txn_result->mutable_locked();
        *lock_info = mock_lock;

        cb();
      })
      .WillOnce([&](Rpc& rpc, std::function<void()> cb) {
        TxnPrewriteRpc* txn_rpc = dynamic_cast<TxnPrewriteRpc*>(&rpc);
        // precommit primary key after resolve lock conflict
        CHECK_NOTNULL(txn_rpc);
        const auto* request = txn_rpc->Request();
        EXPECT_TRUE(request->has_context());
        EXPECT_EQ(request->start_ts(), txn->TEST_GetStartTs());
        EXPECT_EQ(request->txn_size(), 1);
        EXPECT_EQ(request->primary_lock(), txn->TEST_GetPrimaryKey());

        EXPECT_EQ(request->mutations_size(), 1);

        const auto& mutation = request->mutations(0);
        EXPECT_EQ(mutation.key(), txn->TEST_GetPrimaryKey());

        cb();
      })
      .WillOnce([&](Rpc& rpc, std::function<void()> cb) {
        TxnPrewriteRpc* txn_rpc = dynamic_cast<TxnPrewriteRpc*>(&rpc);
        // precommit ordinary key
        CHECK_NOTNULL(txn_rpc);
        const auto* request = txn_rpc->Request();
        EXPECT_TRUE(request->has_context());
        EXPECT_EQ(request->start_ts(), txn->TEST_GetStartTs());
        EXPECT_EQ(request->txn_size(), 2);
        EXPECT_EQ(request->primary_lock(), txn->TEST_GetPrimaryKey());

        EXPECT_EQ(request->mutations_size(), 1);

        cb();
      })
      .WillOnce([&](Rpc& rpc, std::function<void()> cb) {
        TxnPrewriteRpc* txn_rpc = dynamic_cast<TxnPrewriteRpc*>(&rpc);
        // precommit ordinary key
        CHECK_NOTNULL(txn_rpc);
        const auto* request = txn_rpc->Request();
        EXPECT_TRUE(request->has_context());
        EXPECT_EQ(request->start_ts(), txn->TEST_GetStartTs());
        EXPECT_EQ(request->txn_size(), 2);
        EXPECT_EQ(request->primary_lock(), txn->TEST_GetPrimaryKey());

        EXPECT_EQ(request->mutations_size(), 1);

        cb();
      })
      .WillRepeatedly([&](Rpc& rpc, std::function<void()> cb) {
        (void)rpc;

        cb();
      });

  EXPECT_CALL(*txn_lock_resolver, ResolveLock)
      .WillOnce([&](const pb::store::LockInfo& lock_info, int64_t caller_start_ts) {
        EXPECT_TRUE(LockInfoEqual(lock_info, mock_lock));
        EXPECT_EQ(caller_start_ts, txn->TEST_GetStartTs());
        return Status::OK();
      });

  Status s = txn->PreCommit();
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(txn->TEST_IsPreCommittedState(), true);

  s = txn->Commit();
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(txn->TEST_IsCommittedState(), true);
}

TEST_F(SDKTxnImplTest, PrimaryKeyLockConflictExceed) {
  auto txn = NewTransactionImpl(options);

  EXPECT_EQ(txn->TEST_IsActiveState(), true);

  {
    txn->Put("a", "a");
    txn->Delete("a");

    txn->Put("b", "b");
    txn->PutIfAbsent("b", "newb");
    txn->PutIfAbsent("d", "d");
  }

  auto mock_lock = PrepareLockInfo();
  mock_lock.set_key(txn->TEST_GetPrimaryKey());

  EXPECT_CALL(*rpc_client, SendRpc).WillOnce([&](Rpc& rpc, std::function<void()> cb) {
    TxnPrewriteRpc* txn_rpc = dynamic_cast<TxnPrewriteRpc*>(&rpc);
    // precommit primary key lock conflict
    CHECK_NOTNULL(txn_rpc);
    const auto* request = txn_rpc->Request();
    EXPECT_TRUE(request->has_context());
    EXPECT_EQ(request->start_ts(), txn->TEST_GetStartTs());
    EXPECT_EQ(request->txn_size(), 1);
    EXPECT_EQ(request->primary_lock(), txn->TEST_GetPrimaryKey());

    EXPECT_EQ(request->mutations_size(), 1);

    const auto& mutation = request->mutations(0);
    EXPECT_EQ(mutation.key(), txn->TEST_GetPrimaryKey());

    auto* txn_result = txn_rpc->MutableResponse()->add_txn_result();
    auto* lock_info = txn_result->mutable_locked();
    *lock_info = mock_lock;

    cb();
  });

  EXPECT_CALL(*txn_lock_resolver, ResolveLock)
      .WillRepeatedly([&](const pb::store::LockInfo& lock_info, int64_t caller_start_ts) {
        EXPECT_TRUE(LockInfoEqual(lock_info, mock_lock));
        EXPECT_EQ(caller_start_ts, txn->TEST_GetStartTs());
        return Status::TxnLockConflict("");
      });

  Status s = txn->PreCommit();
  EXPECT_TRUE(s.IsTxnLockConflict());
  EXPECT_EQ(txn->TEST_IsPreCommittingState(), true);
}

TEST_F(SDKTxnImplTest, PrimaryKeyWriteLockConfict) {
  auto txn = NewTransactionImpl(options);

  EXPECT_EQ(txn->TEST_IsActiveState(), true);

  {
    txn->Put("a", "a");
    txn->Delete("a");

    txn->Put("b", "b");
    txn->PutIfAbsent("b", "newb");
  }

  pb::store::WriteConflict conflict;
  conflict.set_reason(pb::store::WriteConflict_Reason::WriteConflict_Reason_Optimistic);
  conflict.set_start_ts(txn->TEST_GetStartTs() + kStep);
  conflict.set_key(txn->TEST_GetPrimaryKey());

  EXPECT_CALL(*rpc_client, SendRpc).WillRepeatedly([&](Rpc& rpc, std::function<void()> cb) {
    TxnPrewriteRpc* txn_rpc = dynamic_cast<TxnPrewriteRpc*>(&rpc);
    // precommit
    CHECK_NOTNULL(txn_rpc);
    const auto* request = txn_rpc->Request();
    EXPECT_TRUE(request->has_context());
    EXPECT_EQ(request->start_ts(), txn->TEST_GetStartTs());
    EXPECT_EQ(request->txn_size(), txn->TEST_MutationsSize());
    EXPECT_EQ(request->primary_lock(), txn->TEST_GetPrimaryKey());

    EXPECT_EQ(request->mutations_size(), 2);

    const auto& mutation = request->mutations(0);
    EXPECT_EQ(mutation.key(), txn->TEST_GetPrimaryKey());

    auto* txn_result = txn_rpc->MutableResponse()->add_txn_result();
    auto* write_confilict = txn_result->mutable_write_conflict();
    *write_confilict = conflict;

    cb();
  });

  EXPECT_CALL(*txn_lock_resolver, ResolveLock).Times(0);

  Status s = txn->PreCommit();
  EXPECT_TRUE(s.IsTxnWriteConflict());
  EXPECT_EQ(txn->TEST_IsPreCommittingState(), true);
}

TEST_F(SDKTxnImplTest, PreWriteSecondLockConflict) {
  auto txn = NewTransactionImpl(options);

  EXPECT_EQ(txn->TEST_IsActiveState(), true);

  {
    txn->Put("a", "a");
    txn->Delete("a");

    txn->Put("b", "b");
    txn->PutIfAbsent("b", "newb");

    txn->PutIfAbsent("d", "d");
  }

  EXPECT_CALL(*rpc_client, SendRpc)
      .WillOnce([&](Rpc& rpc, std::function<void()> cb) {
        TxnPrewriteRpc* txn_rpc = dynamic_cast<TxnPrewriteRpc*>(&rpc);
        // precommit primary key
        CHECK_NOTNULL(txn_rpc);
        const auto* request = txn_rpc->Request();
        EXPECT_TRUE(request->has_context());
        EXPECT_EQ(request->start_ts(), txn->TEST_GetStartTs());
        EXPECT_EQ(request->txn_size(), 1);
        EXPECT_EQ(request->primary_lock(), txn->TEST_GetPrimaryKey());

        EXPECT_EQ(request->mutations_size(), 1);

        const auto& mutation = request->mutations(0);
        EXPECT_EQ(mutation.key(), txn->TEST_GetPrimaryKey());

        cb();
      })
      .WillRepeatedly([&](Rpc& rpc, std::function<void()> cb) {
        TxnPrewriteRpc* txn_rpc = dynamic_cast<TxnPrewriteRpc*>(&rpc);
        // precommit
        CHECK_NOTNULL(txn_rpc);
        const auto* request = txn_rpc->Request();
        EXPECT_TRUE(request->has_context());
        EXPECT_EQ(request->start_ts(), txn->TEST_GetStartTs());
        EXPECT_EQ(request->txn_size(), 2);
        EXPECT_EQ(request->primary_lock(), txn->TEST_GetPrimaryKey());

        for (const auto& mutation : request->mutations()) {
          auto mock_lock = PrepareLockInfo();
          mock_lock.set_key(mutation.key());

          auto* txn_result = txn_rpc->MutableResponse()->add_txn_result();
          auto* lock_info = txn_result->mutable_locked();
          *lock_info = mock_lock;
        }

        cb();
      });

  EXPECT_CALL(*txn_lock_resolver, ResolveLock)
      .WillRepeatedly([&](const pb::store::LockInfo& /*lock_info*/, int64_t caller_start_ts) {
        EXPECT_EQ(caller_start_ts, txn->TEST_GetStartTs());
        return Status::TxnLockConflict("");
      });

  Status s = txn->PreCommit();
  EXPECT_TRUE(s.IsTxnLockConflict());
  EXPECT_EQ(txn->TEST_IsPreCommittingState(), true);
}

TEST_F(SDKTxnImplTest, PreWriteSecondWriteConflict) {
  auto txn = NewTransactionImpl(options);

  EXPECT_EQ(txn->TEST_IsActiveState(), true);

  {
    txn->Put("a", "a");
    txn->Delete("a");

    txn->Put("b", "b");
    txn->PutIfAbsent("b", "newb");

    txn->PutIfAbsent("d", "d");
  }

  EXPECT_CALL(*rpc_client, SendRpc)
      .WillOnce([&](Rpc& rpc, std::function<void()> cb) {
        TxnPrewriteRpc* txn_rpc = dynamic_cast<TxnPrewriteRpc*>(&rpc);
        // precommit
        CHECK_NOTNULL(txn_rpc);
        const auto* request = txn_rpc->Request();
        EXPECT_TRUE(request->has_context());
        EXPECT_EQ(request->start_ts(), txn->TEST_GetStartTs());
        EXPECT_EQ(request->txn_size(), 1);
        EXPECT_EQ(request->primary_lock(), txn->TEST_GetPrimaryKey());

        EXPECT_EQ(request->mutations_size(), 1);
        const auto& mutation = request->mutations(0);
        EXPECT_EQ(mutation.key(), txn->TEST_GetPrimaryKey());

        cb();
      })
      .WillRepeatedly([&](Rpc& rpc, std::function<void()> cb) {
        TxnPrewriteRpc* txn_rpc = dynamic_cast<TxnPrewriteRpc*>(&rpc);
        // precommit
        CHECK_NOTNULL(txn_rpc);
        const auto* request = txn_rpc->Request();
        EXPECT_TRUE(request->has_context());
        EXPECT_EQ(request->start_ts(), txn->TEST_GetStartTs());
        EXPECT_EQ(request->txn_size(), 2);
        EXPECT_EQ(request->primary_lock(), txn->TEST_GetPrimaryKey());

        for (const auto& mutation : request->mutations()) {
          auto mock_lock = PrepareLockInfo();
          mock_lock.set_key(mutation.key());

          pb::store::WriteConflict conflict;
          conflict.set_reason(pb::store::WriteConflict_Reason::WriteConflict_Reason_Optimistic);
          conflict.set_start_ts(txn->TEST_GetStartTs() + kStep);
          conflict.set_key(txn->TEST_GetPrimaryKey());

          auto* txn_result = txn_rpc->MutableResponse()->add_txn_result();
          auto* write_confilict = txn_result->mutable_write_conflict();
          *write_confilict = conflict;
        }

        cb();
      });

  EXPECT_CALL(*txn_lock_resolver, ResolveLock).Times(0);

  Status s = txn->PreCommit();
  EXPECT_TRUE(s.IsTxnWriteConflict());
  EXPECT_EQ(txn->TEST_IsPreCommittingState(), true);
}

TEST_F(SDKTxnImplTest, CommitPrimaryKeyMeetRollback) {
  auto txn = NewTransactionImpl(options);

  EXPECT_EQ(txn->TEST_IsActiveState(), true);

  {
    txn->Put("a", "a");
    txn->Delete("a");

    txn->Put("b", "b");
    txn->PutIfAbsent("b", "newb");

    txn->PutIfAbsent("d", "d");
  }

  EXPECT_CALL(*rpc_client, SendRpc).WillRepeatedly([&](Rpc& rpc, std::function<void()> cb) {
    TxnPrewriteRpc* txn_rpc = dynamic_cast<TxnPrewriteRpc*>(&rpc);
    if (nullptr != txn_rpc) {
      // precommit
      cb();
    } else {
      // only commit primarykey rpc
      TxnCommitRpc* txn_rpc = dynamic_cast<TxnCommitRpc*>(&rpc);
      CHECK_NOTNULL(txn_rpc);
      const auto* request = txn_rpc->Request();
      EXPECT_TRUE(request->has_context());
      EXPECT_EQ(request->start_ts(), txn->TEST_GetStartTs());
      EXPECT_EQ(request->commit_ts(), txn->TEST_GetCommitTs());

      EXPECT_EQ(request->keys_size(), 1);

      const auto& key = request->keys(0);
      EXPECT_EQ(key, txn->TEST_GetPrimaryKey());

      auto* response = txn_rpc->MutableResponse();
      response->set_commit_ts(txn->TEST_GetCommitTs());

      auto* txn_result = txn_rpc->MutableResponse()->mutable_txn_result();
      auto* write_conflict = txn_result->mutable_write_conflict();
      write_conflict->set_reason(::dingodb::pb::store::WriteConflict_Reason::WriteConflict_Reason_SelfRolledBack);
      write_conflict->set_start_ts(txn->TEST_GetStartTs());
      write_conflict->set_conflict_ts(txn->TEST_GetStartTs());
      write_conflict->set_key(key);

      cb();
    }
  });

  EXPECT_CALL(*txn_lock_resolver, ResolveLock).Times(0);

  Status s = txn->PreCommit();
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(txn->TEST_IsPreCommittedState(), true);

  s = txn->Commit();
  EXPECT_TRUE(s.IsTxnRolledBack());
  EXPECT_EQ(txn->TEST_IsRollbacktedState(), true);
}

TEST_F(SDKTxnImplTest, CommitSencondError) {
  auto txn = NewTransactionImpl(options);

  EXPECT_EQ(txn->TEST_IsActiveState(), true);

  {
    txn->Put("a", "a");
    txn->Delete("a");

    txn->Put("b", "b");
    txn->PutIfAbsent("b", "newb");

    txn->PutIfAbsent("d", "d");
  }

  EXPECT_CALL(*rpc_client, SendRpc).WillRepeatedly([&](Rpc& rpc, std::function<void()> cb) {
    TxnPrewriteRpc* txn_rpc = dynamic_cast<TxnPrewriteRpc*>(&rpc);
    if (nullptr != txn_rpc) {
      // precommit
      cb();
    } else {
      TxnCommitRpc* txn_rpc = dynamic_cast<TxnCommitRpc*>(&rpc);
      CHECK_NOTNULL(txn_rpc);
      const auto* request = txn_rpc->Request();
      EXPECT_TRUE(request->has_context());
      EXPECT_EQ(request->start_ts(), txn->TEST_GetStartTs());
      EXPECT_EQ(request->commit_ts(), txn->TEST_GetCommitTs());

      auto* response = txn_rpc->MutableResponse();
      response->set_commit_ts(txn->TEST_GetCommitTs());
      if (request->keys_size() == 1 && request->keys(0) == txn->TEST_GetPrimaryKey()) {
        // commit primary key
        cb();
      } else {
        auto* with_err = response->mutable_error();
        with_err->set_errcode(pb::error::EREGION_NOT_FOUND);
        cb();
      }
    }
  });

  EXPECT_CALL(*txn_lock_resolver, ResolveLock).Times(0);

  Status s = txn->PreCommit();
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(txn->TEST_IsPreCommittedState(), true);

  s = txn->Commit();
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(txn->TEST_IsCommittedState(), true);
}

TEST_F(SDKTxnImplTest, PreCommitFailThenRollback) {
  auto txn = NewTransactionImpl(options);

  EXPECT_EQ(txn->TEST_IsActiveState(), true);

  {
    txn->Put("a", "a");
    txn->Delete("a");

    txn->Put("b", "b");
    txn->PutIfAbsent("b", "newb");

    txn->PutIfAbsent("d", "d");
  }

  EXPECT_CALL(*rpc_client, SendRpc).WillRepeatedly([&](Rpc& rpc, std::function<void()> cb) {
    TxnPrewriteRpc* txn_rpc = dynamic_cast<TxnPrewriteRpc*>(&rpc);
    if (nullptr != txn_rpc) {
      // precommit
      const auto* request = txn_rpc->Request();
      EXPECT_TRUE(request->has_context());
      EXPECT_EQ(request->start_ts(), txn->TEST_GetStartTs());
      EXPECT_EQ(request->txn_size(), 1);
      EXPECT_EQ(request->primary_lock(), txn->TEST_GetPrimaryKey());

      for (const auto& mutation : request->mutations()) {
        auto mock_lock = PrepareLockInfo();
        mock_lock.set_key(mutation.key());

        pb::store::WriteConflict conflict;
        conflict.set_reason(pb::store::WriteConflict_Reason::WriteConflict_Reason_Optimistic);
        conflict.set_start_ts(txn->TEST_GetStartTs() + kStep);
        conflict.set_key(txn->TEST_GetPrimaryKey());

        auto* txn_result = txn_rpc->MutableResponse()->add_txn_result();
        auto* write_confilict = txn_result->mutable_write_conflict();
        *write_confilict = conflict;
      }

      cb();
    } else {
      // rollback
      TxnBatchRollbackRpc* txn_rpc = dynamic_cast<TxnBatchRollbackRpc*>(&rpc);
      const auto* request = txn_rpc->Request();
      EXPECT_TRUE(request->has_context());
      EXPECT_EQ(request->start_ts(), txn->TEST_GetStartTs());

      for (const auto& key : request->keys()) {
        EXPECT_TRUE(key == "a" || key == "b" || key == "d");
      }
      cb();
    }
  });

  EXPECT_CALL(*txn_lock_resolver, ResolveLock).Times(0);

  Status s = txn->PreCommit();
  EXPECT_TRUE(s.IsTxnWriteConflict());
  EXPECT_EQ(txn->TEST_IsPreCommittingState(), true);

  s = txn->Rollback();
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(txn->TEST_IsRollbacktedState(), true);
}

TEST_F(SDKTxnImplTest, RollbackPrimaryKeyFail) {
  auto txn = NewTransactionImpl(options);

  EXPECT_EQ(txn->TEST_IsActiveState(), true);

  {
    txn->Put("a", "a");
    txn->Delete("a");

    txn->Put("b", "b");
    txn->PutIfAbsent("b", "newb");

    txn->PutIfAbsent("d", "d");
  }

  EXPECT_CALL(*rpc_client, SendRpc).WillRepeatedly([&](Rpc& rpc, std::function<void()> cb) {
    TxnPrewriteRpc* txn_rpc = dynamic_cast<TxnPrewriteRpc*>(&rpc);
    if (nullptr != txn_rpc) {
      // precommit primary key
      const auto* request = txn_rpc->Request();
      EXPECT_TRUE(request->has_context());
      EXPECT_EQ(request->start_ts(), txn->TEST_GetStartTs());
      EXPECT_EQ(request->txn_size(), 1);
      EXPECT_EQ(request->primary_lock(), txn->TEST_GetPrimaryKey());

      for (const auto& mutation : request->mutations()) {
        auto mock_lock = PrepareLockInfo();
        mock_lock.set_key(mutation.key());

        pb::store::WriteConflict conflict;
        conflict.set_reason(pb::store::WriteConflict_Reason::WriteConflict_Reason_Optimistic);
        conflict.set_start_ts(txn->TEST_GetStartTs() + kStep);
        conflict.set_key(txn->TEST_GetPrimaryKey());

        auto* txn_result = txn_rpc->MutableResponse()->add_txn_result();
        auto* write_confilict = txn_result->mutable_write_conflict();
        *write_confilict = conflict;
      }

      cb();
    } else {
      // rollback
      TxnBatchRollbackRpc* txn_rpc = dynamic_cast<TxnBatchRollbackRpc*>(&rpc);
      const auto* request = txn_rpc->Request();
      EXPECT_TRUE(request->has_context());
      EXPECT_EQ(request->start_ts(), txn->TEST_GetStartTs());

      EXPECT_EQ(request->keys_size(), 1);
      const auto& key = request->keys(0);
      EXPECT_EQ(key, txn->TEST_GetPrimaryKey());

      auto mock_lock = PrepareLockInfo();
      mock_lock.set_key(key);

      auto* txn_result = txn_rpc->MutableResponse()->mutable_txn_result();
      auto* lock_info = txn_result->mutable_locked();
      *lock_info = mock_lock;
      cb();
    }
  });

  EXPECT_CALL(*txn_lock_resolver, ResolveLock).Times(0);

  Status s = txn->PreCommit();
  EXPECT_TRUE(s.IsTxnWriteConflict());
  EXPECT_EQ(txn->TEST_IsPreCommittingState(), true);

  s = txn->Rollback();
  EXPECT_TRUE(s.IsTxnLockConflict());
  EXPECT_EQ(txn->TEST_IsRollbackingState(), true);
}

TEST_F(SDKTxnImplTest, RollbackSecondKeysFail) {
  auto txn = NewTransactionImpl(options);

  EXPECT_EQ(txn->TEST_IsActiveState(), true);

  {
    txn->Put("a", "a");
    txn->Delete("a");

    txn->Put("b", "b");
    txn->PutIfAbsent("b", "newb");

    txn->PutIfAbsent("d", "d");
  }

  EXPECT_CALL(*rpc_client, SendRpc).WillRepeatedly([&](Rpc& rpc, std::function<void()> cb) {
    TxnPrewriteRpc* txn_rpc = dynamic_cast<TxnPrewriteRpc*>(&rpc);
    if (nullptr != txn_rpc) {
      // precommit primary key
      const auto* request = txn_rpc->Request();
      EXPECT_TRUE(request->has_context());
      EXPECT_EQ(request->start_ts(), txn->TEST_GetStartTs());
      EXPECT_EQ(request->txn_size(), 1);
      EXPECT_EQ(request->primary_lock(), txn->TEST_GetPrimaryKey());

      for (const auto& mutation : request->mutations()) {
        auto mock_lock = PrepareLockInfo();
        mock_lock.set_key(mutation.key());

        pb::store::WriteConflict conflict;
        conflict.set_reason(pb::store::WriteConflict_Reason::WriteConflict_Reason_Optimistic);
        conflict.set_start_ts(txn->TEST_GetStartTs() + kStep);
        conflict.set_key(txn->TEST_GetPrimaryKey());

        auto* txn_result = txn_rpc->MutableResponse()->add_txn_result();
        auto* write_confilict = txn_result->mutable_write_conflict();
        *write_confilict = conflict;
      }

      cb();
    } else {
      // rollback
      TxnBatchRollbackRpc* txn_rpc = dynamic_cast<TxnBatchRollbackRpc*>(&rpc);
      const auto* request = txn_rpc->Request();
      EXPECT_TRUE(request->has_context());
      EXPECT_EQ(request->start_ts(), txn->TEST_GetStartTs());

      for (const auto& key : request->keys()) {
        if (key == txn->TEST_GetPrimaryKey()) {
          cb();
        } else {
          auto mock_lock = PrepareLockInfo();
          mock_lock.set_key(key);

          auto* txn_result = txn_rpc->MutableResponse()->mutable_txn_result();
          auto* lock_info = txn_result->mutable_locked();
          *lock_info = mock_lock;
        }
      }

      cb();
    }
  });

  EXPECT_CALL(*txn_lock_resolver, ResolveLock).Times(0);

  Status s = txn->PreCommit();
  EXPECT_TRUE(s.IsTxnWriteConflict());
  EXPECT_EQ(txn->TEST_IsPreCommittingState(), true);

  s = txn->Rollback();
  EXPECT_TRUE(s.ok());
  EXPECT_EQ(txn->TEST_IsRollbacktedState(), true);
}

TEST_F(SDKTxnImplTest, LockHeartbeat) {
  auto txn = NewTransactionImpl(options);

  EXPECT_EQ(txn->TEST_IsActiveState(), true);

  {
    txn->Put("a", "a");
    txn->Delete("a");

    txn->Put("b", "b");
    txn->PutIfAbsent("b", "newb");

    txn->PutIfAbsent("d", "d");
  }

  EXPECT_CALL(*rpc_client, SendRpc)
      .WillOnce([&](Rpc& rpc, std::function<void()> cb) {
        TxnPrewriteRpc* txn_rpc = dynamic_cast<TxnPrewriteRpc*>(&rpc);
        // precommit primary key
        CHECK_NOTNULL(txn_rpc);
        const auto* request = txn_rpc->Request();
        EXPECT_TRUE(request->has_context());
        EXPECT_EQ(request->start_ts(), txn->TEST_GetStartTs());
        EXPECT_EQ(request->txn_size(), 1);
        EXPECT_EQ(request->primary_lock(), txn->TEST_GetPrimaryKey());

        const auto& mutation = request->mutations(0);
        EXPECT_EQ(mutation.key(), txn->TEST_GetPrimaryKey());

        cb();
      })
      .WillOnce([&](Rpc& rpc, std::function<void()> cb) {
        TxnPrewriteRpc* txn_rpc = dynamic_cast<TxnPrewriteRpc*>(&rpc);
        // precommit
        CHECK_NOTNULL(txn_rpc);
        const auto* request = txn_rpc->Request();
        EXPECT_TRUE(request->has_context());
        EXPECT_EQ(request->start_ts(), txn->TEST_GetStartTs());
        EXPECT_EQ(request->txn_size(), 2);
        EXPECT_EQ(request->primary_lock(), txn->TEST_GetPrimaryKey());

        EXPECT_EQ(request->mutations_size(), 1);

        cb();
      })
      .WillOnce([&](Rpc& rpc, std::function<void()> cb) {
        TxnPrewriteRpc* txn_rpc = dynamic_cast<TxnPrewriteRpc*>(&rpc);
        // precommit
        CHECK_NOTNULL(txn_rpc);
        const auto* request = txn_rpc->Request();
        EXPECT_TRUE(request->has_context());
        EXPECT_EQ(request->start_ts(), txn->TEST_GetStartTs());
        EXPECT_EQ(request->txn_size(), 2);
        EXPECT_EQ(request->primary_lock(), txn->TEST_GetPrimaryKey());

        EXPECT_EQ(request->mutations_size(), 1);

        cb();
      })
      .WillRepeatedly([&](Rpc& rpc, std::function<void()> cb) {
        TxnHeartBeatRpc* txn_rpc = dynamic_cast<TxnHeartBeatRpc*>(&rpc);
        CHECK_NOTNULL(txn_rpc);
        const auto* request = txn_rpc->Request();
        EXPECT_TRUE(request->has_context());
        EXPECT_EQ(request->start_ts(), txn->TEST_GetStartTs());
        EXPECT_EQ(request->primary_lock(), txn->TEST_GetPrimaryKey());

        auto* response = txn_rpc->MutableResponse();
        response->set_lock_ttl(request->advise_lock_ttl());

        cb();
      });

  Status s = txn->PreCommit();
  EXPECT_TRUE(s.ok());

  sleep(25);
}

TEST_F(SDKTxnImplTest, LockHeartbeatFail) {
  auto txn = NewTransactionImpl(options);

  EXPECT_EQ(txn->TEST_IsActiveState(), true);

  {
    txn->Put("a", "a");
    txn->Delete("a");

    txn->Put("b", "b");
    txn->PutIfAbsent("b", "newb");

    txn->PutIfAbsent("d", "d");
  }

  EXPECT_CALL(*rpc_client, SendRpc)
      .WillOnce([&](Rpc& rpc, std::function<void()> cb) {
        TxnPrewriteRpc* txn_rpc = dynamic_cast<TxnPrewriteRpc*>(&rpc);
        // precommit
        CHECK_NOTNULL(txn_rpc);
        const auto* request = txn_rpc->Request();
        EXPECT_TRUE(request->has_context());
        EXPECT_EQ(request->start_ts(), txn->TEST_GetStartTs());
        EXPECT_EQ(request->txn_size(), 1);
        EXPECT_EQ(request->primary_lock(), txn->TEST_GetPrimaryKey());

        EXPECT_EQ(request->mutations_size(), 1);
        const auto& mutation = request->mutations(0);
        EXPECT_EQ(mutation.key(), txn->TEST_GetPrimaryKey());

        cb();
      })
      .WillOnce([&](Rpc& rpc, std::function<void()> cb) {
        TxnPrewriteRpc* txn_rpc = dynamic_cast<TxnPrewriteRpc*>(&rpc);
        // precommit
        CHECK_NOTNULL(txn_rpc);
        const auto* request = txn_rpc->Request();
        EXPECT_TRUE(request->has_context());
        EXPECT_EQ(request->start_ts(), txn->TEST_GetStartTs());
        EXPECT_EQ(request->txn_size(), 2);
        EXPECT_EQ(request->primary_lock(), txn->TEST_GetPrimaryKey());

        cb();
      })
      .WillOnce([&](Rpc& rpc, std::function<void()> cb) {
        TxnPrewriteRpc* txn_rpc = dynamic_cast<TxnPrewriteRpc*>(&rpc);
        // precommit
        CHECK_NOTNULL(txn_rpc);
        const auto* request = txn_rpc->Request();
        EXPECT_TRUE(request->has_context());
        EXPECT_EQ(request->start_ts(), txn->TEST_GetStartTs());
        EXPECT_EQ(request->txn_size(), 2);
        EXPECT_EQ(request->primary_lock(), txn->TEST_GetPrimaryKey());

        cb();
      })
      .WillOnce([&](Rpc& rpc, std::function<void()> cb) {
        TxnHeartBeatRpc* txn_rpc = dynamic_cast<TxnHeartBeatRpc*>(&rpc);
        CHECK_NOTNULL(txn_rpc);
        const auto* request = txn_rpc->Request();
        EXPECT_TRUE(request->has_context());
        EXPECT_EQ(request->start_ts(), txn->TEST_GetStartTs());
        EXPECT_EQ(request->primary_lock(), txn->TEST_GetPrimaryKey());

        auto* response = txn_rpc->MutableResponse();
        auto* txn_not_found = txn_rpc->MutableResponse()->mutable_txn_result()->mutable_txn_not_found();
        auto* key = txn_not_found->mutable_key();
        *key = txn->TEST_GetPrimaryKey();

        cb();
      });

  Status s = txn->PreCommit();
  EXPECT_TRUE(s.ok());

  sleep(25);
}

}  // namespace sdk
}  // namespace dingodb
