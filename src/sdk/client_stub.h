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

#ifndef DINGODB_SDK_CLIENT_STUB_H_
#define DINGODB_SDK_CLIENT_STUB_H_

#include <memory>

#include "glog/logging.h"
#include "sdk/admin_tool.h"
#include "sdk/auto_increment_manager.h"
#include "sdk/document/document_index_cache.h"
#include "sdk/meta_cache.h"
#include "sdk/region_scanner.h"
#include "sdk/rpc/coordinator_rpc_controller.h"
#include "sdk/rpc/rpc_client.h"
#include "sdk/transaction/tso.h"
#include "sdk/transaction/txn_lock_resolver.h"
#include "sdk/vector/vector_index_cache.h"
#include "utils/actuator.h"

namespace dingodb {
namespace sdk {

class TxnManager;

class ClientStub {
 public:
  ClientStub();

  virtual ~ClientStub();

  Status Open(const std::vector<EndPoint>& endpoints);

  void Stop();

  virtual std::shared_ptr<CoordinatorRpcController> GetCoordinatorRpcController() const {
    DCHECK_NOTNULL(coordinator_rpc_controller_.get());
    return coordinator_rpc_controller_;
  }

  virtual std::shared_ptr<CoordinatorRpcController> GetMetaRpcController() const {
    DCHECK_NOTNULL(meta_rpc_controller_.get());
    return meta_rpc_controller_;
  }

  virtual std::shared_ptr<CoordinatorRpcController> GetVersionRpcController() const {
    DCHECK_NOTNULL(version_rpc_controller_.get());
    return version_rpc_controller_;
  }

  virtual std::shared_ptr<MetaCache> GetMetaCache() const {
    DCHECK_NOTNULL(meta_cache_.get());
    return meta_cache_;
  }

  virtual std::shared_ptr<RpcClient> GetRpcClient() const {
    DCHECK_NOTNULL(rpc_client_.get());
    return rpc_client_;
  }

  virtual std::shared_ptr<RegionScannerFactory> GetRawKvRegionScannerFactory() const {
    DCHECK_NOTNULL(raw_kv_region_scanner_factory_.get());
    return raw_kv_region_scanner_factory_;
  }

  virtual std::shared_ptr<RegionScannerFactory> GetTxnRegionScannerFactory() const {
    DCHECK_NOTNULL(txn_region_scanner_factory_.get());
    return txn_region_scanner_factory_;
  }

  virtual std::shared_ptr<AdminTool> GetAdminTool() const {
    DCHECK_NOTNULL(admin_tool_.get());
    return admin_tool_;
  }

  virtual std::shared_ptr<TxnLockResolver> GetTxnLockResolver() const {
    DCHECK_NOTNULL(txn_lock_resolver_.get());
    return txn_lock_resolver_;
  }

  virtual std::shared_ptr<Actuator> GetActuator() const {
    DCHECK_NOTNULL(actuator_.get());
    return actuator_;
  }

  virtual std::shared_ptr<Actuator> GetTxnActuator() const {
    DCHECK_NOTNULL(txn_actuator_.get());
    return txn_actuator_;
  }

  virtual std::shared_ptr<VectorIndexCache> GetVectorIndexCache() const {
    DCHECK_NOTNULL(vector_index_cache_.get());
    return vector_index_cache_;
  }

  virtual std::shared_ptr<DocumentIndexCache> GetDocumentIndexCache() const {
    DCHECK_NOTNULL(document_index_cache_.get());
    return document_index_cache_;
  }

  virtual std::shared_ptr<AutoIncrementerManager> GetAutoIncrementerManager() const {
    DCHECK_NOTNULL(auto_increment_manager_.get());
    return auto_increment_manager_;
  }

  virtual TsoProviderSPtr GetTsoProvider() const {
    DCHECK_NOTNULL(tso_provider_.get());
    return tso_provider_;
  }

  virtual TxnManager* GetTxnManager() const {
    DCHECK_NOTNULL(txn_manager_.get());
    return txn_manager_.get();
  }

 private:
  // TODO: use unique ptr
  std::shared_ptr<CoordinatorRpcController> coordinator_rpc_controller_;
  std::shared_ptr<CoordinatorRpcController> meta_rpc_controller_;
  std::shared_ptr<CoordinatorRpcController> version_rpc_controller_;
  std::shared_ptr<MetaCache> meta_cache_;
  std::shared_ptr<RpcClient> rpc_client_;
  std::shared_ptr<RegionScannerFactory> raw_kv_region_scanner_factory_;
  std::shared_ptr<RegionScannerFactory> txn_region_scanner_factory_;
  std::shared_ptr<AdminTool> admin_tool_;
  std::shared_ptr<TxnLockResolver> txn_lock_resolver_;
  std::shared_ptr<Actuator> actuator_;
  std::shared_ptr<Actuator> txn_actuator_;
  std::shared_ptr<VectorIndexCache> vector_index_cache_;
  std::shared_ptr<DocumentIndexCache> document_index_cache_;
  std::shared_ptr<AutoIncrementerManager> auto_increment_manager_;
  TsoProviderSPtr tso_provider_;
  std::unique_ptr<TxnManager> txn_manager_;
};

}  // namespace sdk
}  // namespace dingodb

#endif  // DINGODB_SDK_CLIENT_STUB_H_