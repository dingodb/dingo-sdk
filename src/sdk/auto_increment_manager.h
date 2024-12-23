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

#ifndef DINGODB_SDK_AUTO_INCREMENT_MANAGER_H_
#define DINGODB_SDK_AUTO_INCREMENT_MANAGER_H_

#include <cstdint>
#include <deque>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <vector>

#include "dingosdk/status.h"
#include "proto/meta.pb.h"
#include "sdk/document/document_index.h"
#include "sdk/vector/vector_index.h"

namespace dingodb {
namespace sdk {

class ClientStub;

class AutoIncrementer {
 public:
  AutoIncrementer(const ClientStub& stub) : stub_(stub) {}

  virtual ~AutoIncrementer() = default;

  Status GetNextId(int64_t& next);

  Status GetNextIds(std::vector<int64_t>& to_fill, int64_t count);

  Status GetAutoIncrementId(int64_t& start_id);

  Status UpdateAutoIncrementId(int64_t start_id);

 protected:
  virtual void PrepareGenerateAutoIncrementRequest(pb::meta::GenerateAutoIncrementRequest& request) = 0;
  virtual void PrepareUpdateAutoIncrementRequest(pb::meta::UpdateAutoIncrementRequest& request, int64_t start_id) = 0;

 private:
  friend class AutoIncrementerManager;
  Status RefillCache();
  Status RunOperation(std::function<Status()> operation);

  const ClientStub& stub_;

  std::mutex mutex_;
  struct Req;
  std::deque<Req*> queue_;
  std::vector<int64_t> id_cache_;
};

class VectorIndexAutoInrementer : public AutoIncrementer {
 public:
  VectorIndexAutoInrementer(const ClientStub& stub, std::shared_ptr<VectorIndex> vector_index)
      : AutoIncrementer(stub), vector_index_(std::move(vector_index)) {}

  ~VectorIndexAutoInrementer() override = default;

 private:
  friend class AutoIncrementerManager;
  void PrepareGenerateAutoIncrementRequest(pb::meta::GenerateAutoIncrementRequest& request) override;
  void PrepareUpdateAutoIncrementRequest(pb::meta::UpdateAutoIncrementRequest& request, int64_t start_id) override;
  // NOTE: when delete index,  we should delete the auto incrementer
  // TODO: use index_id instead of vector_index
  const std::shared_ptr<VectorIndex> vector_index_;
};

class DocumentIndexAutoInrementer : public AutoIncrementer {
 public:
  DocumentIndexAutoInrementer(const ClientStub& stub, std::shared_ptr<DocumentIndex> doc_index)
      : AutoIncrementer(stub), doc_index_(std::move(doc_index)) {}

  ~DocumentIndexAutoInrementer() override = default;

 private:
  friend class AutoIncrementerManager;
  void PrepareGenerateAutoIncrementRequest(pb::meta::GenerateAutoIncrementRequest& request) override;
  void PrepareUpdateAutoIncrementRequest(pb::meta::UpdateAutoIncrementRequest& request, int64_t start_id) override;
  const std::shared_ptr<DocumentIndex> doc_index_;
};

class AutoIncrementerManager {
 public:
  AutoIncrementerManager(const ClientStub& stub) : stub_(stub) {}

  ~AutoIncrementerManager() = default;

  std::shared_ptr<AutoIncrementer> GetOrCreateVectorIndexIncrementer(std::shared_ptr<VectorIndex>& index);

  std::shared_ptr<AutoIncrementer> GetOrCreateDocumentIndexIncrementer(std::shared_ptr<DocumentIndex>& index);

  void RemoveIndexIncrementerById(int64_t index_id);

 private:
  const ClientStub& stub_;
  std::mutex mutex_;
  std::unordered_map<int64_t, std::shared_ptr<AutoIncrementer>> auto_incrementer_map_;
};

}  // namespace sdk
}  // namespace dingodb

#endif  // DINGODB_SDK_AUTO_INCREMENT_MANAGER_H_