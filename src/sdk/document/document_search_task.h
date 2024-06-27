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

#ifndef DINGODB_SDK_VECTOR_SEARCH_TATSK_H_
#define DINGODB_SDK_VECTOR_SEARCH_TATSK_H_

#include <cstdint>
#include <memory>
#include <unordered_map>

#include "fmt/core.h"
#include "sdk/client_stub.h"
#include "sdk/document.h"
#include "sdk/document/document_index.h"
#include "sdk/document/document_task.h"
#include "sdk/rpc/document_service_rpc.h"
#include "sdk/rpc/store_rpc_controller.h"

namespace dingodb {
namespace sdk {

class DocumentSearchPartTask;
class DocumentSearchTask : public DocumentTask {
 public:
  DocumentSearchTask(const ClientStub& stub, int64_t index_id, const DocSearchParam& search_param,
                     DocSearchResult& out_result)
      : DocumentTask(stub), index_id_(index_id), search_param_(search_param), out_result_(out_result) {}

  ~DocumentSearchTask() override = default;

 private:
  Status Init() override;
  void DoAsync() override;

  std::string Name() const override { return fmt::format("DocumentSearchTask-{}", index_id_); }

  void SubTaskCallback(Status status, DocumentSearchPartTask* sub_task);

  const int64_t index_id_;
  const DocSearchParam& search_param_;
  pb::common::DocumentSearchParameter search_parameter_;

  DocSearchResult& out_result_;

  std::shared_ptr<DocumentIndex> doc_index_;

  std::shared_mutex rw_lock_;
  std::set<int64_t> next_part_ids_;
  Status status_;

  std::atomic<int> sub_tasks_count_{0};
};

class DocumentSearchPartTask : public DocumentTask {
 public:
  DocumentSearchPartTask(const ClientStub& stub, int64_t index_id, int64_t part_id,
                         const pb::common::DocumentSearchParameter& search_param)
      : DocumentTask(stub), index_id_(index_id), part_id_(part_id), search_parameter_(search_param) {}

  ~DocumentSearchPartTask() override = default;

  std::vector<DocWithStore> GetDocSearchResult() {
    std::shared_lock<std::shared_mutex> r(rw_lock_);
    return std::move(search_result_);
  }

 private:
  friend class DocumentSearchTask;

  Status Init() override;
  void DoAsync() override;

  std::string Name() const override { return fmt::format("DocumentSearchPartTask-{}-{}", index_id_, part_id_); }

  void FillDocumentSearchRpcRequest(pb::document::DocumentSearchRequest* request,
                                    const std::shared_ptr<Region>& region);

  void DocumentSearchRpcCallback(const Status& status, DocumentSearchRpc* rpc);

  const int64_t index_id_;
  const int64_t part_id_;
  const pb::common::DocumentSearchParameter& search_parameter_;

  std::shared_ptr<DocumentIndex> doc_index_;

  std::unordered_map<int64_t, std::shared_ptr<Region>> next_batch_region_;

  std::vector<StoreRpcController> controllers_;
  std::vector<std::unique_ptr<DocumentSearchRpc>> rpcs_;

  std::shared_mutex rw_lock_;
  Status status_;
  std::vector<DocWithStore> search_result_;

  std::atomic<int> sub_tasks_count_{0};
};

}  // namespace sdk

}  // namespace dingodb
#endif  // DINGODB_SDK_VECTOR_SEARCH_TATSK_H_