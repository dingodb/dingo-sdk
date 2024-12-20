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

#ifndef DINGODB_SDK_DOCUMENT_SCAN_QUERY_TATSK_H_
#define DINGODB_SDK_DOCUMENT_SCAN_QUERY_TATSK_H_

#include <cstdint>

#include "sdk/client_stub.h"
#include "dingosdk/document.h"
#include "sdk/document/document_task.h"
#include "sdk/rpc/document_service_rpc.h"
#include "sdk/rpc/store_rpc_controller.h"

namespace dingodb {

namespace sdk {

class DocumentScanQueryPartTask;

class DocumentScanQueryTask : public DocumentTask {
 public:
  DocumentScanQueryTask(const ClientStub& stub, int64_t index_id, const DocScanQueryParam& query_param,
                        DocScanQueryResult& out_result)
      : DocumentTask(stub), index_id_(index_id), scan_query_param_(query_param), out_result_(out_result) {}

  ~DocumentScanQueryTask() override = default;

 private:
  Status Init() override;
  void DoAsync() override;

  std::string Name() const override { return fmt::format("DocumentScanQueryTask-{}", index_id_); }

  void SubTaskCallback(Status status, DocumentScanQueryPartTask* sub_task);

  void ConstructResultUnlocked();

  const int64_t index_id_;
  const DocScanQueryParam& scan_query_param_;

  DocScanQueryResult& out_result_;

  std::shared_ptr<DocumentIndex> doc_index_;

  std::shared_mutex rw_lock_;
  std::vector<DocWithId> result_docs_;
  std::set<int64_t> vector_ids_;  // for unique check
  std::set<int64_t> next_part_ids_;
  Status status_;

  std::atomic<int> sub_tasks_count_{0};
};

class DocumentScanQueryPartTask : public DocumentTask {
 public:
  DocumentScanQueryPartTask(const ClientStub& stub, std::shared_ptr<DocumentIndex> vector_index, int64_t part_id,
                            const DocScanQueryParam& query_param)
      : DocumentTask(stub), doc_index_(vector_index), part_id_(part_id), scan_query_param_(query_param) {}

  ~DocumentScanQueryPartTask() override = default;

  std::vector<DocWithId> GetResult() {
    std::shared_lock<std::shared_mutex> r(rw_lock_);
    return std::move(result_docs_);
  }

 private:
  friend class DocumentScanQueryTask;

  void DoAsync() override;

  std::string Name() const override {
    return fmt::format("DocumentScanQueryPartTask-{}-{}", doc_index_->GetId(), part_id_);
  }

  void FillDocumentScanQueryRpcRequest(pb::document::DocumentScanQueryRequest* request,
                                       const std::shared_ptr<Region>& region);

  void DocumentScanQueryRpcCallback(Status status, DocumentScanQueryRpc* rpc);

  const std::shared_ptr<DocumentIndex> doc_index_;
  const int64_t part_id_;
  const DocScanQueryParam& scan_query_param_;

  std::vector<StoreRpcController> controllers_;
  std::vector<std::unique_ptr<DocumentScanQueryRpc>> rpcs_;

  std::shared_mutex rw_lock_;
  std::vector<DocWithId> result_docs_;
  Status status_;

  std::atomic<int> sub_tasks_count_{0};
};

}  // namespace sdk
}  // namespace dingodb
#endif  // DINGODB_SDK_DOCUMENT_SCAN_QUERY_TATSK_H_
