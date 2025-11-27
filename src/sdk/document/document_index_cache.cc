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

#include "sdk/document/document_index_cache.h"

#include <cstdint>
#include <memory>
#include <mutex>
#include <shared_mutex>

#include "dingosdk/status.h"
#include "glog/logging.h"
#include "proto/meta.pb.h"
#include "sdk/client_stub.h"
#include "sdk/rpc/coordinator_rpc.h"

namespace dingodb {
namespace sdk {

DocumentIndexCache::DocumentIndexCache(const ClientStub& stub) : stub_(stub) {}

Status DocumentIndexCache::GetIndexIdByKey(const DocumentIndexCacheKey& index_key, int64_t& index_id) {
  {
    ReadLockGuard guard(rw_lock_);
    auto iter = index_key_to_id_.find(index_key);
    if (iter != index_key_to_id_.end()) {
      index_id = iter->second;
      return Status::OK();
    }
  }

  std::shared_ptr<DocumentIndex> tmp;
  DINGO_RETURN_NOT_OK(SlowGetDocumentIndexByKey(index_key, tmp));

  index_id = tmp->GetId();
  return Status::OK();
}

Status DocumentIndexCache::GetDocumentIndexByKey(const DocumentIndexCacheKey& index_key,
                                                 std::shared_ptr<DocumentIndex>& out_doc_index) {
  {
    ReadLockGuard guard(rw_lock_);
    auto iter = index_key_to_id_.find(index_key);
    if (iter != index_key_to_id_.end()) {
      auto index_iter = id_to_index_.find(iter->second);
      CHECK(index_iter != id_to_index_.end());
      out_doc_index = index_iter->second;
      return Status::OK();
    }
  }

  return SlowGetDocumentIndexByKey(index_key, out_doc_index);
}

Status DocumentIndexCache::GetDocumentIndexById(int64_t index_id, std::shared_ptr<DocumentIndex>& out_doc_index) {
  {
    ReadLockGuard guard(rw_lock_);
    auto iter = id_to_index_.find(index_id);
    if (iter != id_to_index_.end()) {
      out_doc_index = iter->second;
      return Status::OK();
    }
  }

  return SlowGetDocumentIndexById(index_id, out_doc_index);
}

void DocumentIndexCache::RemoveDocumentIndexById(int64_t index_id) {
  WriteLockGuard guard(rw_lock_);
  auto id_iter = id_to_index_.find(index_id);
  if (id_iter != id_to_index_.end()) {
    auto doc_index = id_iter->second;
    auto name_iter = index_key_to_id_.find(GetDocumentIndexCacheKey(*doc_index));
    CHECK(name_iter != index_key_to_id_.end());

    id_iter->second->MarkStale();
    id_to_index_.erase(id_iter);
    index_key_to_id_.erase(name_iter);
  }
}

void DocumentIndexCache::RemoveDocumentIndexByKey(const DocumentIndexCacheKey& index_key) {
  WriteLockGuard guard(rw_lock_);

  auto name_iter = index_key_to_id_.find(index_key);
  if (name_iter != index_key_to_id_.end()) {
    auto id_iter = id_to_index_.find(name_iter->second);
    CHECK(id_iter != id_to_index_.end());

    id_iter->second->MarkStale();
    id_to_index_.erase(id_iter);
    index_key_to_id_.erase(name_iter);
  }
}

Status DocumentIndexCache::SlowGetDocumentIndexByKey(const DocumentIndexCacheKey& index_key,
                                                     std::shared_ptr<DocumentIndex>& out_doc_index) {
  int64_t schema_id{0};
  std::string index_name;
  DecodeDocumentIndexCacheKey(index_key, schema_id, index_name);
  CHECK(!index_name.empty()) << "illegal index key: " << index_key;
  CHECK_NE(schema_id, 0) << "illegal index key: " << index_key;

  GetIndexByNameRpc rpc;
  auto* schema = rpc.MutableRequest()->mutable_schema_id();
  schema->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_SCHEMA);
  schema->set_entity_id(schema_id);
  rpc.MutableRequest()->set_index_name(index_name);

  DINGO_RETURN_NOT_OK(stub_.GetCoordinatorRpcController()->SyncCall(rpc));

  if (CheckIndexResponse(*rpc.Response())) {
    return ProcessIndexDefinitionWithId(rpc.Response()->index_definition_with_id(), out_doc_index);
  } else {
    return Status::NotFound("response check invalid");
  }
}

Status DocumentIndexCache::SlowGetDocumentIndexById(int64_t index_id, std::shared_ptr<DocumentIndex>& out_doc_index) {
  GetIndexRpc rpc;
  auto* index_id_pb = rpc.MutableRequest()->mutable_index_id();
  index_id_pb->set_entity_type(::dingodb::pb::meta::EntityType::ENTITY_TYPE_INDEX);
  index_id_pb->set_parent_entity_id(::dingodb::pb::meta::ReservedSchemaIds::DINGO_SCHEMA);
  index_id_pb->set_entity_id(index_id);

  DINGO_RETURN_NOT_OK(stub_.GetCoordinatorRpcController()->SyncCall(rpc));

  if (CheckIndexResponse(*rpc.Response())) {
    return ProcessIndexDefinitionWithId(rpc.Response()->index_definition_with_id(), out_doc_index);
  } else {
    return Status::NotFound("response check invalid");
  }
}

Status DocumentIndexCache::ProcessIndexDefinitionWithId(const pb::meta::IndexDefinitionWithId& index_def_with_id,
                                                        std::shared_ptr<DocumentIndex>& out_doc_index) {
  int64_t index_id = index_def_with_id.index_id().entity_id();

  WriteLockGuard guard(rw_lock_);
  auto iter = id_to_index_.find(index_id);
  if (iter != id_to_index_.end()) {
    CHECK_EQ(iter->second->GetName(), index_def_with_id.index_definition().name());
    out_doc_index = iter->second;
    return Status::OK();
  } else {
    // becareful change CHECK to DCHECK
    auto doc_index = std::make_shared<DocumentIndex>(index_def_with_id);
    CHECK(index_key_to_id_.insert({GetDocumentIndexCacheKey(*doc_index), index_id}).second);
    CHECK(id_to_index_.insert({index_id, doc_index}).second);
    doc_index->UnMarkStale();
    out_doc_index = doc_index;
    return Status::OK();
  }
}

bool DocumentIndexCache::CheckIndexDefinitionWithId(const pb::meta::IndexDefinitionWithId& index_def_with_id) {
  if (!index_def_with_id.has_index_id()) {
    return false;
  }
  if (!(index_def_with_id.index_id().entity_id() > 0)) {
    return false;
  }
  if (!(index_def_with_id.index_id().parent_entity_id() > 0)) {
    return false;
  }
  if (!index_def_with_id.has_index_definition()) {
    return false;
  }
  if (index_def_with_id.index_definition().name().empty()) {
    return false;
  }
  if (!(index_def_with_id.index_definition().index_partition().partitions_size() > 0)) {
    return false;
  }
  return true;
}

}  // namespace sdk
}  // namespace dingodb