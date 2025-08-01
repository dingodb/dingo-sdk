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

#include "sdk/admin_tool.h"

#include "common/logging.h"
#include "dingosdk/status.h"
#include "glog/logging.h"
#include "rpc/coordinator_rpc.h"
#include "sdk/client_stub.h"
#include "sdk/common/common.h"
#include "sdk/rpc/coordinator_rpc.h"

namespace dingodb {
namespace sdk {

Status AdminTool::IsCreateRegionInProgress(int64_t region_id, bool& out_create_in_progress) {
  QueryRegionRpc rpc;
  rpc.MutableRequest()->set_region_id(region_id);

  Status status = stub_.GetCoordinatorRpcController()->SyncCall(rpc);
  if (!status.ok()) {
    return status;
  }

  CHECK(rpc.Response()->has_region()) << "query region internal error, req:" << rpc.Request()->ShortDebugString()
                                      << ", resp:" << rpc.Response()->ShortDebugString();
  CHECK_EQ(rpc.Response()->region().id(), region_id);
  out_create_in_progress = (rpc.Response()->region().state() == pb::common::REGION_NEW);

  return Status::OK();
}

Status AdminTool::DropRegion(int64_t region_id) {
  DropRegionRpc rpc;
  rpc.MutableRequest()->set_region_id(region_id);

  Status ret = stub_.GetCoordinatorRpcController()->SyncCall(rpc);
  if (ret.IsNotFound()) {
    ret = Status::OK();
  }

  return ret;
}

Status AdminTool::CreateTableIds(int64_t count, std::vector<int64_t>& out_table_ids) {
  CHECK(count > 0) << "count must greater 0";
  CreateTableIdsRpc rpc;
  auto* schema_id = rpc.MutableRequest()->mutable_schema_id();
  schema_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_SCHEMA);
  schema_id->set_entity_id(::dingodb::pb::meta::ReservedSchemaIds::DINGO_SCHEMA);
  schema_id->set_parent_entity_id(::dingodb::pb::meta::ReservedSchemaIds::ROOT_SCHEMA);

  rpc.MutableRequest()->set_count(count);

  Status ret = stub_.GetMetaRpcController()->SyncCall(rpc);
  if (!ret.ok()) {
    return ret;
  }

  CHECK_EQ(rpc.MutableResponse()->table_ids_size(), count);

  for (const auto& id : rpc.MutableResponse()->table_ids()) {
    out_table_ids.push_back(id.entity_id());
  }

  return Status::OK();
}

Status AdminTool::DropIndex(int64_t index_id) {
  if (index_id <= 0) {
    return Status::InvalidArgument("index_id must greater than 0");
  }

  DropIndexRpc rpc;
  auto* index_pb = rpc.MutableRequest()->mutable_index_id();
  index_pb->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_INDEX);
  index_pb->set_parent_entity_id(::dingodb::pb::meta::ReservedSchemaIds::DINGO_SCHEMA);
  index_pb->set_entity_id(index_id);

  Status s = stub_.GetMetaRpcController()->SyncCall(rpc);
  if (s.IsNotFound()) {
    s = Status::OK();
  }
  return s;
}

}  // namespace sdk
}  // namespace dingodb