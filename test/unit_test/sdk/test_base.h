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

#ifndef DINGODB_SDK_TEST_TEST_BASE_H_
#define DINGODB_SDK_TEST_TEST_BASE_H_

#include <memory>

#include "dingosdk/client.h"
#include "dingosdk/status.h"
#include "dingosdk/vector.h"
#include "glog/logging.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "mock_client_stub.h"
#include "mock_coordinator_rpc_controller.h"
#include "mock_region_scanner.h"
#include "mock_rpc_client.h"
#include "sdk/admin_tool.h"
#include "sdk/auto_increment_manager.h"
#include "sdk/client_internal_data.h"
#include "sdk/meta_cache.h"
#include "sdk/transaction/tso.h"
#include "sdk/transaction/txn_impl.h"
#include "sdk/transaction/txn_manager.h"
#include "sdk/utils/actuator.h"
#include "sdk/utils/thread_pool_actuator.h"
#include "sdk/vector/vector_index_cache.h"
#include "test_common.h"
#include "transaction/mock_txn_lock_resolver.h"

namespace dingodb {
namespace sdk {

class TestBase : public ::testing::Test {
 public:
  TestBase() {
    std::unique_ptr<MockClientStub> tmp = std::make_unique<MockClientStub>();
    stub = tmp.get();

    coordinator_rpc_controller = std::make_shared<MockCoordinatorRpcController>(*stub);
    ON_CALL(*stub, GetCoordinatorRpcController).WillByDefault(testing::Return(coordinator_rpc_controller));
    EXPECT_CALL(*stub, GetCoordinatorRpcController).Times(testing::AnyNumber());
    ON_CALL(*coordinator_rpc_controller, SyncCall).WillByDefault(testing::Return(Status::OK()));

    meta_rpc_controller = std::make_shared<MockCoordinatorRpcController>(*stub);
    ON_CALL(*stub, GetMetaRpcController).WillByDefault(testing::Return(meta_rpc_controller));
    EXPECT_CALL(*stub, GetMetaRpcController).Times(testing::AnyNumber());
    ON_CALL(*meta_rpc_controller, SyncCall).WillByDefault(testing::Return(Status::OK()));

    meta_cache = std::make_shared<MetaCache>(coordinator_rpc_controller);
    ON_CALL(*stub, GetMetaCache).WillByDefault(testing::Return(meta_cache));
    EXPECT_CALL(*stub, GetMetaCache).Times(testing::AnyNumber());

    RpcClientOptions options;
    options.connect_timeout_ms = 3000;
    options.timeout_ms = 5000;
    rpc_client = std::make_shared<MockRpcClient>(options);
    ON_CALL(*stub, GetRpcClient).WillByDefault(testing::Return(rpc_client));
    EXPECT_CALL(*stub, GetRpcClient).Times(testing::AnyNumber());

    region_scanner_factory = std::make_shared<MockRegionScannerFactory>();
    ON_CALL(*stub, GetRawKvRegionScannerFactory).WillByDefault(testing::Return(region_scanner_factory));
    EXPECT_CALL(*stub, GetRawKvRegionScannerFactory).Times(testing::AnyNumber());

    admin_tool = std::make_shared<AdminTool>(*stub);
    ON_CALL(*stub, GetAdminTool).WillByDefault(testing::Return(admin_tool));
    EXPECT_CALL(*stub, GetAdminTool).Times(testing::AnyNumber());

    txn_lock_resolver = std::make_shared<MockTxnLockResolver>(*stub);
    ON_CALL(*stub, GetTxnLockResolver).WillByDefault(testing::Return(txn_lock_resolver));
    EXPECT_CALL(*stub, GetTxnLockResolver).Times(testing::AnyNumber());

    actuator = std::make_shared<ThreadPoolActuator>();
    actuator->Start(FLAGS_actuator_thread_num);
    ON_CALL(*stub, GetActuator).WillByDefault(testing::Return(actuator));
    EXPECT_CALL(*stub, GetActuator).Times(testing::AnyNumber());

    index_cache = std::make_shared<VectorIndexCache>(*stub);
    ON_CALL(*stub, GetVectorIndexCache).WillByDefault(testing::Return(index_cache));
    EXPECT_CALL(*stub, GetVectorIndexCache).Times(testing::AnyNumber());

    auto_increment_manager = std::make_shared<AutoIncrementerManager>(*stub);
    ON_CALL(*stub, GetAutoIncrementerManager).WillByDefault(testing::Return(auto_increment_manager));
    EXPECT_CALL(*stub, GetAutoIncrementerManager).Times(testing::AnyNumber());

    tso_provider = std::make_shared<TsoProvider>(*stub);
    ON_CALL(*stub, GetTsoProvider).WillByDefault(testing::Return(tso_provider));
    EXPECT_CALL(*stub, GetTsoProvider).Times(testing::AnyNumber());

    txn_manager = std::make_unique<TxnManager>();
    ON_CALL(*stub, GetTxnManager).WillByDefault(testing::Return(txn_manager.get()));
    EXPECT_CALL(*stub, GetTxnManager).Times(testing::AnyNumber());

    client = new Client();
    client->data_->stub = std::move(tmp);
  }

  ~TestBase() override {
    if (txn_manager) {
      txn_manager->Stop();
    }
    if (actuator) {
      actuator->Stop();
    }

    delete client;
  }

  void SetUp() override { PreFillMetaCache(); }

  std::shared_ptr<TxnImpl> NewTransactionImpl(const TransactionOptions& options) const {
    auto txn = std::make_shared<TxnImpl>(*stub, options, client->data_->stub->GetTxnManager());
    CHECK_NOTNULL(txn.get());
    CHECK(txn->Begin().ok());
    return std::move(txn);
  }

  std::unique_ptr<Transaction> NewTransaction(const TransactionOptions& options) const {
    Transaction* txn = nullptr;
    Status s = client->NewTransaction(options, &txn);
    CHECK_NOTNULL(txn);
    CHECK(s.ok());
    return std::unique_ptr<Transaction>(txn);
  }

  std::shared_ptr<MockCoordinatorRpcController> coordinator_rpc_controller;
  std::shared_ptr<MockCoordinatorRpcController> meta_rpc_controller;
  std::shared_ptr<MetaCache> meta_cache;
  std::shared_ptr<MockRpcClient> rpc_client;
  std::shared_ptr<MockRegionScannerFactory> region_scanner_factory;
  std::shared_ptr<AdminTool> admin_tool;
  std::shared_ptr<MockTxnLockResolver> txn_lock_resolver;
  std::shared_ptr<Actuator> actuator;
  std::shared_ptr<VectorIndexCache> index_cache;
  std::shared_ptr<AutoIncrementerManager> auto_increment_manager;
  std::shared_ptr<TsoProvider> tso_provider;
  std::unique_ptr<TxnManager> txn_manager;

  // client own stub
  MockClientStub* stub;
  Client* client;

 private:
  void PreFillMetaCache() {
    meta_cache->MaybeAddRegion(RegionA2C());
    meta_cache->MaybeAddRegion(RegionC2E());
    meta_cache->MaybeAddRegion(RegionE2G());
  }
};

}  // namespace sdk
}  // namespace dingodb
#endif  // DINGODB_SDK_TEST_TEST_BASE_H_