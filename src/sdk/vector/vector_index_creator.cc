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
#include "sdk/rpc/coordinator_rpc.h"
#include "dingosdk/status.h"
#include "dingosdk/vector.h"
#include "sdk/vector/vector_common.h"
#include "sdk/vector/vector_index_creator_internal_data.h"

namespace dingodb {
namespace sdk {

VectorIndexCreator::VectorIndexCreator(Data* data) : data_(data) {}

VectorIndexCreator::~VectorIndexCreator() { delete data_; }

VectorIndexCreator& VectorIndexCreator::SetSchemaId(int64_t schema_id) {
  data_->schema_id = schema_id;
  return *this;
}

VectorIndexCreator& VectorIndexCreator::SetName(const std::string& name) {
  data_->index_name = name;
  return *this;
}

VectorIndexCreator& VectorIndexCreator::SetRangePartitions(std::vector<int64_t> separator_id) {
  data_->range_partition_seperator_ids = std::move(separator_id);
  return *this;
}

VectorIndexCreator& VectorIndexCreator::SetReplicaNum(int64_t num) {
  data_->replica_num = num;
  return *this;
}

VectorIndexCreator& VectorIndexCreator::SetFlatParam(const FlatParam& params) {
  data_->index_type = kFlat;
  data_->flat_param = params;
  return *this;
}

VectorIndexCreator& VectorIndexCreator::SetIvfFlatParam(const IvfFlatParam& params) {
  data_->index_type = kIvfFlat;
  data_->ivf_flat_param = params;
  return *this;
}

VectorIndexCreator& VectorIndexCreator::SetIvfPqParam(const IvfPqParam& params) {
  data_->index_type = kIvfPq;
  data_->ivf_pq_param = params;
  return *this;
}

VectorIndexCreator& VectorIndexCreator::SetHnswParam(const HnswParam& params) {
  data_->index_type = kHnsw;
  data_->hnsw_param = params;
  return *this;
}

VectorIndexCreator& VectorIndexCreator::SetDiskAnnParam(const DiskAnnParam& params) {
  data_->index_type = kDiskAnn;
  data_->diskann_param = params;
  return *this;
}

VectorIndexCreator& VectorIndexCreator::SetBruteForceParam(const BruteForceParam& params) {
  data_->index_type = kBruteForce;
  data_->brute_force_param = params;
  return *this;
}

VectorIndexCreator& VectorIndexCreator::SetBinaryFlatParam(const BinaryFlatParam& params) {
  data_->index_type = kBinaryFlat;
  data_->binary_flat_param = params;
  return *this;
}

VectorIndexCreator& VectorIndexCreator::SetBinaryIvfFlatParam(const BinaryIvfFlatParam& params) {
  data_->index_type = kBinaryIvfFlat;
  data_->binary_ivf_flat_param = params;
  return *this;
}

VectorIndexCreator& VectorIndexCreator::SetScalarSchema(const VectorScalarSchema& schema) {
  data_->schema = schema;
  return *this;
}

VectorIndexCreator& VectorIndexCreator::SetAutoIncrementStart(int64_t start_id) {
  if (start_id > 0) {
    data_->auto_incr_start = start_id;
  }
  return *this;
}

// TODO: check partition is illegal
// TODO: support hash partitions
Status VectorIndexCreator::Create(int64_t& out_index_id) {
  if (data_->schema_id <= 0) {
    return Status::InvalidArgument("Invalid schema_id");
  }
  if (data_->index_name.empty()) {
    return Status::InvalidArgument("Missing index name");
  }
  // TODO: support hash region
  // if (data_->range_partition_seperator_ids.empty()) {
  //   return Status::InvalidArgument("Missing Range Partition");
  // }
  if (data_->version <= 0) {
    return Status::InvalidArgument("index version must greater 0");
  }
  if (data_->replica_num <= 0) {
    return Status::InvalidArgument("replica num must greater 0");
  }
  if (data_->index_type == kNoneIndexType) {
    return Status::InvalidArgument("Missing Vector Param");
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
  index_definition_pb->set_name(data_->index_name);
  index_definition_pb->set_replica(data_->replica_num);

  if (data_->auto_incr_start.has_value()) {
    index_definition_pb->set_with_auto_incrment(true);
    int64_t start = data_->auto_incr_start.value();
    CHECK_GT(start, 0);
    index_definition_pb->set_auto_increment(start);
  }

  // vector index parameter
  data_->BuildIndexParameter(index_definition_pb->mutable_index_parameter());

  // TODO: support hash
  FillRangePartitionRule(index_definition_pb->mutable_index_partition(), data_->range_partition_seperator_ids, new_ids);

  out_index_id = new_index_id;

  return data_->stub.GetMetaRpcController()->SyncCall(rpc);
}

}  // namespace sdk
}  // namespace dingodb