#ifndef DINGODB_SDK_VECTOR_DISKANN_IMPORT_TASK_H_
#define DINGODB_SDK_VECTOR_DISKANN_IMPORT_TASK_H_

#include <cstdint>
#include <memory>
#include <unordered_map>
#include <vector>

#include "sdk/client_stub.h"
#include "sdk/rpc/index_service_rpc.h"
#include "sdk/rpc/store_rpc_controller.h"
#include "sdk/vector/vector_index.h"
#include "sdk/vector/vector_task.h"

namespace dingodb {
namespace sdk {

class VectorImportAddTask : public VectorTask {
 public:
  VectorImportAddTask(const ClientStub& stub, int64_t index_id, std::vector<VectorWithId>& vectors)
      : VectorTask(stub), index_id_(index_id), vectors_(vectors) {}

  ~VectorImportAddTask() override = default;

 private:
  Status Init() override;
  void DoAsync() override;

  void VectorImportAddRpcCallback(const Status& status, VectorImportRpc* rpc);
  std::string Name() const override { return fmt::format("VectorImportAddTask-{}", index_id_); }

  const int64_t index_id_;
  std::vector<VectorWithId>& vectors_;

  std::shared_ptr<VectorIndex> vector_index_;

  std::vector<StoreRpcController> controllers_;
  std::vector<std::unique_ptr<VectorImportRpc>> rpcs_;

  std::shared_mutex rw_lock_;
  std::unordered_map<int64_t, int64_t> vector_id_to_idx_;
  Status status_;

  std::atomic<int> sub_tasks_count_{0};
};

class VectorImportDeleteTask : public VectorTask {
 public:
  VectorImportDeleteTask(const ClientStub& stub, int64_t index_id, const std::vector<int64_t>& vector_ids,
                         std::vector<DeleteResult>& out_result)
      : VectorTask(stub), index_id_(index_id), vector_ids_(vector_ids), out_result_(out_result) {}

  ~VectorImportDeleteTask() override = default;

 private:
  Status Init() override;
  void DoAsync() override;

  std::string Name() const override { return fmt::format("VectorImportDeleteTask-{}", index_id_); }

  void VectorImportDeleteRpcCallback(const Status& status, VectorImportRpc* rpc);

  const int64_t index_id_;
  const std::vector<int64_t>& vector_ids_;
  std::vector<DeleteResult>& out_result_;

  std::shared_ptr<VectorIndex> vector_index_;

  std::vector<StoreRpcController> controllers_;
  std::vector<std::unique_ptr<VectorImportRpc>> rpcs_;

  std::shared_mutex rw_lock_;
  std::set<int64_t> next_vector_ids_;
  Status status_;

  std::atomic<int> sub_tasks_count_{0};
};

}  // namespace sdk

}  // namespace dingodb
#endif