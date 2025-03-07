#ifndef DINGODB_SDK_VECTOR_DOSKANN_STATUS_BY_INDEX_TASK_H_
#define DINGODB_SDK_VECTOR_DOSKANN_STATUS_BY_INDEX_TASK_H_

#include <cstdint>
#include <memory>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

#include "dingosdk/vector.h"
#include "sdk/client_stub.h"
#include "sdk/rpc/index_service_rpc.h"
#include "sdk/rpc/store_rpc_controller.h"
#include "sdk/vector/vector_index.h"
#include "sdk/vector/vector_task.h"

namespace dingodb {
namespace sdk {

class VectorStatusPartTask;
class VectorStatusByIndexTask : public VectorTask {
 public:
  VectorStatusByIndexTask(const ClientStub& stub, int64_t index_id, StateResult& result)
      : VectorTask(stub), index_id_(index_id), result_(result) {}

  ~VectorStatusByIndexTask() override = default;

 private:
  Status Init() override;
  void DoAsync() override;
  bool NeedRetry() override { return false; };

  std::string Name() const override { return fmt::format("VectorStatusByIndexTask-{}", index_id_); }
  void SubTaskCallback(const Status& status, VectorStatusPartTask* sub_task);

  const int64_t index_id_;
  StateResult& result_;

  std::shared_ptr<VectorIndex> vector_index_;

  std::shared_mutex rw_lock_;
  std::set<int64_t> next_part_ids_;
  Status status_;

  std::atomic<int> sub_tasks_count_{0};
};

class VectorStatusPartTask : public VectorTask {
 public:
  VectorStatusPartTask(const ClientStub& stub, std::shared_ptr<VectorIndex> vector_index, int64_t part_id)
      : VectorTask(stub), vector_index_(std::move(vector_index)), part_id_(part_id) {}

  ~VectorStatusPartTask() override = default;

  StateResult GetResult() const { return result_; }

 private:
  friend class VectorStatusByIndexTask;

  void DoAsync() override;
  int retry_count_{0};

  std::string Name() const override {
    return fmt::format("VectorStatusPartTask-{}-{}", vector_index_->GetId(), part_id_);
  }

  void VectorStatusRpcCallback(const Status& status, VectorStatusRpc* rpc);
  const int64_t part_id_;
  const std::shared_ptr<VectorIndex> vector_index_;

  std::unordered_map<int64_t, std::shared_ptr<Region>> next_batch_region_;

  std::vector<StoreRpcController> controllers_;
  std::vector<std::unique_ptr<VectorStatusRpc>> rpcs_;
  StateResult result_;

  std::shared_mutex rw_lock_;
  Status status_;

  std::atomic<int> sub_tasks_count_{0};
};

}  // namespace sdk
}  // namespace dingodb

#endif  // DINGODB_SDK_VECTOR_DISKANN_STATUS_TASK_H_