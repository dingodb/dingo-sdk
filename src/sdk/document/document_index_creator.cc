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

#include <cstdint>

#include "glog/logging.h"
#include "proto/meta.pb.h"
#include "dingosdk/document.h"
#include "sdk/document/document_index_creator_internal_data.h"
#include "sdk/document/document_translater.h"
#include "sdk/rpc/coordinator_rpc.h"
#include "dingosdk/status.h"

namespace dingodb {
namespace sdk {

DocumentIndexCreator::DocumentIndexCreator(Data* data) : data_(data) {}

DocumentIndexCreator::~DocumentIndexCreator() { delete data_; }

DocumentIndexCreator& DocumentIndexCreator::SetSchemaId(int64_t schema_id) {
  data_->schema_id = schema_id;
  return *this;
}

DocumentIndexCreator& DocumentIndexCreator::SetName(const std::string& name) {
  data_->doc_name = name;
  return *this;
}

DocumentIndexCreator& DocumentIndexCreator::SetRangePartitions(std::vector<int64_t> separator_id) {
  data_->range_partition_seperator_ids = std::move(separator_id);
  return *this;
}

DocumentIndexCreator& DocumentIndexCreator::SetReplicaNum(int64_t num) {
  data_->replica_num = num;
  return *this;
}

DocumentIndexCreator& DocumentIndexCreator::SetAutoIncrementStart(int64_t start_id) {
  if (start_id > 0) {
    data_->auto_incr_start = start_id;
  }
  return *this;
}

DocumentIndexCreator& DocumentIndexCreator::SetSchema(const DocumentSchema& schema) {
  data_->schema = schema;
  return *this;
}

DocumentIndexCreator& DocumentIndexCreator::SetJsonParams(std::string& json_params) {
  data_->json_params = json_params;
  return *this;
}

// TODO: check partition is illegal
// TODO: support hash partitions
Status DocumentIndexCreator::Create(int64_t& out_index_id) {
  if (data_->schema_id <= 0) {
    return Status::InvalidArgument("Invalid schema_id");
  }
  if (data_->doc_name.empty()) {
    return Status::InvalidArgument("Missing index name");
  }
  if (data_->replica_num <= 0) {
    return Status::InvalidArgument("replica num must greater 0");
  }
  if (!data_->schema.has_value()) {
    return Status::InvalidArgument("Missing schema");
  }

  auto part_count = data_->range_partition_seperator_ids.size() + 1;
  std::vector<int64_t> new_ids;
  // +1 for index id
  DINGO_RETURN_NOT_OK(data_->stub.GetAdminTool()->CreateTableIds(part_count + 1, new_ids));
  int64_t new_index_id = new_ids[0];

  CreateIndexRpc rpc;
  auto* schema_id_pb = rpc.MutableRequest()->mutable_schema_id();
  schema_id_pb->set_entity_type(pb::meta::EntityType::ENTITY_TYPE_SCHEMA);
  schema_id_pb->set_entity_id(pb::meta::ReservedSchemaIds::DINGO_SCHEMA);
  schema_id_pb->set_parent_entity_id(pb::meta::ReservedSchemaIds::ROOT_SCHEMA);
  schema_id_pb->set_entity_id(data_->schema_id);

  auto* index_id_pb = rpc.MutableRequest()->mutable_index_id();
  index_id_pb->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_INDEX);
  index_id_pb->set_parent_entity_id(schema_id_pb->entity_id());
  index_id_pb->set_entity_id(new_index_id);

  auto* index_definition_pb = rpc.MutableRequest()->mutable_index_definition();
  index_definition_pb->set_name(data_->doc_name);
  index_definition_pb->set_replica(data_->replica_num);

  if (data_->auto_incr_start.has_value()) {
    index_definition_pb->set_with_auto_incrment(true);
    int64_t start = data_->auto_incr_start.value();
    CHECK_GT(start, 0);
    index_definition_pb->set_auto_increment(start);
  }

  // TODO: support hash
  DocumentTranslater::FillRangePartitionRule(index_definition_pb->mutable_index_partition(),
                                             data_->range_partition_seperator_ids, new_ids);

  // vector index parameter
  data_->BuildIndexParameter(index_definition_pb->mutable_index_parameter());

  out_index_id = new_index_id;

  return data_->stub.GetMetaRpcController()->SyncCall(rpc);
}

}  // namespace sdk
}  // namespace dingodb