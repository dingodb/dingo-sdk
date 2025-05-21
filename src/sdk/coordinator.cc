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

#include "dingosdk/coordinator.h"

#include <cstdint>
#include <vector>

#include "fmt/core.h"
#include "glog/logging.h"
#include "proto/common.pb.h"
#include "sdk/client_stub.h"
#include "sdk/rpc/coordinator_rpc.h"

namespace dingodb {
namespace sdk {

Status Coordinator::ScanRegions(const std::string& start_key, const std::string& end_key,
                                std::vector<int64_t>& region_ids) {
  ScanRegionsRpc rpc;
  auto* mut_request = rpc.MutableRequest();
  mut_request->set_key(start_key);
  mut_request->set_range_end(end_key);

  Status status = stub_.GetCoordinatorRpcController()->SyncCall(rpc);
  if (!status.IsOK()) {
    DINGO_LOG(ERROR) << fmt::format("Scan regions fail, error: {} {}", status.Errno(), status.ToString());
    return status;
  }

  for (const auto& region : rpc.Response()->regions()) {
    region_ids.push_back(region.region_id());
  }

  return status;
}

Status Coordinator::CreateAutoIncrement(int64_t table_id, int64_t start_id) {
  CreateAutoIncrementRpc rpc;
  auto* mut_request = rpc.MutableRequest();
  auto* mut_table_id = mut_request->mutable_table_id();
  mut_table_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_TABLE);
  mut_table_id->set_entity_id(table_id);
  mut_table_id->set_parent_entity_id(0);

  mut_request->set_start_id(start_id);

  Status status = stub_.GetMetaRpcController()->SyncCall(rpc);
  if (!status.IsOK()) {
    DINGO_LOG(ERROR) << fmt::format("Create auto increment fail, error: {} {}", status.Errno(), status.ToString());
    return status;
  }

  return status;
}

Status Coordinator::DeleteAutoIncrement(int64_t table_id) {
  DeleteAutoIncrementRpc rpc;
  auto* mut_request = rpc.MutableRequest();
  auto* mut_table_id = mut_request->mutable_table_id();
  mut_table_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_TABLE);
  mut_table_id->set_entity_id(table_id);
  mut_table_id->set_parent_entity_id(0);

  Status status = stub_.GetMetaRpcController()->SyncCall(rpc);
  if (!status.IsOK()) {
    DINGO_LOG(ERROR) << fmt::format("Delete auto increment fail, error: {} {}", status.Errno(), status.ToString());
    return status;
  }

  return status;
}

Status Coordinator::UpdateAutoIncrement(int64_t table_id, int64_t start_id) {
  UpdateAutoIncrementRpc rpc;
  auto* mut_request = rpc.MutableRequest();
  auto* mut_table_id = mut_request->mutable_table_id();
  mut_table_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_TABLE);
  mut_table_id->set_entity_id(table_id);
  mut_table_id->set_parent_entity_id(0);

  mut_request->set_start_id(start_id);
  mut_request->set_force(false);

  Status status = stub_.GetMetaRpcController()->SyncCall(rpc);
  if (!status.IsOK()) {
    DINGO_LOG(ERROR) << fmt::format("Update auto increment fail, error: {} {}", status.Errno(), status.ToString());
    return status;
  }

  return status;
}

Status Coordinator::GenerateAutoIncrement(int64_t table_id, int64_t count, int64_t& start_id, int64_t& end_id) {
  GenerateAutoIncrementRpc rpc;
  auto* mut_request = rpc.MutableRequest();
  auto* mut_table_id = mut_request->mutable_table_id();
  mut_table_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_TABLE);
  mut_table_id->set_entity_id(table_id);
  mut_table_id->set_parent_entity_id(0);

  mut_request->set_count(count);
  mut_request->set_auto_increment_increment(1);
  mut_request->set_auto_increment_offset(1);

  Status status = stub_.GetMetaRpcController()->SyncCall(rpc);
  if (!status.IsOK()) {
    DINGO_LOG(ERROR) << fmt::format("Generate auto increment fail, error: {} {}", status.Errno(), status.ToString());
    return status;
  }

  start_id = rpc.Response()->start_id();
  end_id = rpc.Response()->end_id();

  return status;
}

Status Coordinator::GetAutoIncrement(int64_t table_id, int64_t& start_id) {
  GetAutoIncrementRpc rpc;
  auto* mut_request = rpc.MutableRequest();
  auto* mut_table_id = mut_request->mutable_table_id();
  mut_table_id->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_TABLE);
  mut_table_id->set_entity_id(table_id);
  mut_table_id->set_parent_entity_id(0);

  Status status = stub_.GetMetaRpcController()->SyncCall(rpc);
  if (!status.IsOK()) {
    if (status.Errno() == pb::error::EAUTO_INCREMENT_NOT_FOUND) {
      return Status::NotFound("auto increment not found");
    }
    DINGO_LOG(ERROR) << fmt::format("Get auto increment fail, error: {} {}", status.Errno(), status.ToString());
    return status;
  }

  start_id = rpc.Response()->start_id();

  return status;
}

Status Coordinator::GetAutoIncrements(std::vector<TableIncrement>& table_increments) {
  GetAutoIncrementsRpc rpc;
  Status status = stub_.GetMetaRpcController()->SyncCall(rpc);
  if (!status.IsOK()) {
    DINGO_LOG(ERROR) << fmt::format("Get all auto increment fail, error: {} {}", status.Errno(), status.ToString());
    return status;
  }

  for (const auto& table_increment : rpc.Response()->table_increments()) {
    table_increments.push_back({table_increment.table_id(), table_increment.start_id()});
  }

  return status;
}

static MDS::State PbState2State(pb::common::MDS::State state) {
  switch (state) {
    case pb::common::MDS::State::MDS_State_INIT:
      return MDS::State::kInit;
    case pb::common::MDS::State::MDS_State_NORMAL:
      return MDS::State::kNormal;
    case pb::common::MDS::State::MDS_State_ABNORMAL:
      return MDS::State::kAbnormal;
    default:
      DINGO_LOG(FATAL) << "state is match.";
      break;
  }

  return MDS::State::kInit;
}

static pb::common::MDS::State State2PbState(MDS::State state) {
  switch (state) {
    case MDS::State::kInit:
      return pb::common::MDS::State::MDS_State_INIT;
    case MDS::State::kNormal:
      return pb::common::MDS::State::MDS_State_NORMAL;
    case MDS::State::kAbnormal:
      return pb::common::MDS::State::MDS_State_ABNORMAL;
    default:
      DINGO_LOG(FATAL) << "state is match.";
      break;
  }

  return pb::common::MDS::State::MDS_State_INIT;
}

static MDS PbMds2Mds(const pb::common::MDS& pb_mds) {
  MDS mds;
  mds.id = pb_mds.id();
  mds.location.host = pb_mds.location().host();
  mds.location.port = pb_mds.location().port();

  mds.state = PbState2State(pb_mds.state());
  mds.register_time_ms = pb_mds.register_time_ms();
  mds.last_online_time_ms = pb_mds.last_online_time_ms();

  return mds;
}

static pb::common::MDS Mds2PbMds(const MDS& mds) {
  pb::common::MDS pb_mds;
  pb_mds.set_id(mds.id);
  auto* mut_location = pb_mds.mutable_location();
  mut_location->set_host(mds.location.host);
  mut_location->set_port(mds.location.port);
  pb_mds.set_state(State2PbState(mds.state));
  pb_mds.set_register_time_ms(mds.register_time_ms);
  pb_mds.set_last_online_time_ms(mds.last_online_time_ms);

  return pb_mds;
}

Status Coordinator::MDSHeartbeat(const MDS& mds, std::vector<MDS>& out_mdses) {
  MDSHeartbeatRpc rpc;
  *rpc.MutableRequest()->mutable_mds() = Mds2PbMds(mds);

  Status status = stub_.GetCoordinatorRpcController()->SyncCall(rpc);
  if (!status.IsOK()) {
    DINGO_LOG(ERROR) << fmt::format("mds heartbeat fail, error: {} {}", status.Errno(), status.ToString());
    return status;
  }

  for (const auto& mds : rpc.Response()->mdses()) {
    out_mdses.push_back(PbMds2Mds(mds));
  }

  return status;
}

Status Coordinator::GetMDSList(std::vector<MDS>& mdses) {
  GetMDSListRpc rpc;
  Status status = stub_.GetCoordinatorRpcController()->SyncCall(rpc);
  if (!status.IsOK()) {
    DINGO_LOG(ERROR) << fmt::format("Get mds list fail, error: {} {}", status.Errno(), status.ToString());
    return status;
  }

  for (const auto& pb_mds : rpc.Response()->mdses()) {
    mdses.push_back(PbMds2Mds(pb_mds));
  }

  return status;
}

}  // namespace sdk
}  // namespace dingodb
