#ifndef DINGODB_SDK_VECTOR_DISKANN_COUNT_MEMORY_TASK_H_
#define DINGODB_SDK_VECTOR_DISKANN_COUNT_MEMORY_TASK_H_

#include <cstdint>
#include <memory>
#include <unordered_map>

#include "dingosdk/vector.h"
#include "sdk/client_stub.h"
#include "sdk/rpc/index_service_rpc.h"
#include "sdk/rpc/store_rpc_controller.h"
#include "sdk/utils/rw_lock.h"
#include "sdk/vector/vector_index.h"
#include "sdk/vector/vector_task.h"

namespace dingodb {
namespace sdk {

class VectorCountMemoryPartTask;
class VectorCountMemoryByIndexTask : public VectorTask {
 public:
  VectorCountMemoryByIndexTask(const ClientStub& stub, int64_t index_id, int64_t& count)
      : VectorTask(stub), index_id_(index_id), count_(count) {}

  ~VectorCountMemoryByIndexTask() override = default;

 private:
  Status Init() override;
  void DoAsync() override;

  std::string Name() const override { return fmt::format("VectorCountMemoryByIndexTask-{}", index_id_); }

  void SubTaskCallback(Status status, VectorCountMemoryPartTask* sub_task);

  const int64_t index_id_;
  int64_t& count_;

  std::shared_ptr<VectorIndex> vector_index_;

  RWLock rw_lock_;
  std::set<int64_t> next_part_ids_;
  Status status_;
  std::atomic<int> tmp_count_{0};

  std::atomic<int> sub_tasks_count_{0};
};

class VectorCountMemoryPartTask : public VectorTask {
 public:
  VectorCountMemoryPartTask(const ClientStub& stub, std::shared_ptr<VectorIndex> vector_index, int64_t part_id)
      : VectorTask(stub), vector_index_(std::move(vector_index)), part_id_(part_id) {}

  ~VectorCountMemoryPartTask() override = default;

  int64_t GetCount() const { return count_.load(); }

 private:
  friend class VectorCountMemoryByIndexTask;

  void DoAsync() override;

  std::string Name() const override {
    return fmt::format("VectorCountMemoryPartTask-{}-{}", vector_index_->GetId(), part_id_);
  }

  void VectorCountMemoryRpcCallback(const Status& status, VectorCountMemoryRpc* rpc);

  const int64_t part_id_;
  const std::shared_ptr<VectorIndex> vector_index_;

  std::unordered_map<int64_t, std::shared_ptr<Region>> next_batch_region_;

  std::vector<StoreRpcController> controllers_;
  std::vector<std::unique_ptr<VectorCountMemoryRpc>> rpcs_;

  RWLock rw_lock_;
  Status status_;
  std::atomic<int64_t> count_{0};
  std::atomic<int> sub_tasks_count_{0};
};

}  // namespace sdk
}  // namespace dingodb

#endif  // DINGODB_SDK_VECTOR_DISKANN_COUNT_MEMORY_TASK_H_