#ifndef DINGODB_SDK_VECTOR_DISKANN_BUILD_BY_INDEX_TASK_H_
#define DINGODB_SDK_VECTOR_DISKANN_BUILD_BY_INDEX_TASK_H_

#include <cstdint>
#include <memory>
#include <unordered_map>


#include "sdk/client_stub.h"
#include "sdk/rpc/index_service_rpc.h"
#include "sdk/rpc/store_rpc_controller.h"
#include "sdk/status.h"
#include "sdk/vector.h"
#include "sdk/vector/vector_index.h"
#include "sdk/vector/vector_task.h"

namespace dingodb {
namespace sdk {

class VectorBuildPartTask;
class VectorBuildByIndexTask : public VectorTask {
 public:
  VectorBuildByIndexTask(const ClientStub& stub, int64_t index_id, ErrStatusResult& result)
      : VectorTask(stub), index_id_(index_id), result_(result) {}

  ~VectorBuildByIndexTask() override = default;

 private:
  Status Init() override;
  void DoAsync() override;
  bool NeedRetry() override { return false; };

  std::string Name() const override { return fmt::format("VectorBuildByIndexTask-{}", index_id_); }
  void SubTaskCallback(const Status& status, VectorBuildPartTask* sub_task);

  const int64_t index_id_;
  ErrStatusResult& result_;

  std::shared_ptr<VectorIndex> vector_index_;

  std::shared_mutex rw_lock_;
  std::set<int64_t> next_part_ids_;
  Status status_;

  std::atomic<int> sub_tasks_count_{0};
};

class VectorBuildPartTask : public VectorTask {
 public:
  VectorBuildPartTask(const ClientStub& stub, std::shared_ptr<VectorIndex> vector_index, int64_t part_id)
      : VectorTask(stub), vector_index_(std::move(vector_index)), part_id_(part_id) {}

  ~VectorBuildPartTask() override = default;

  ErrStatusResult GetResult() const { return result_; }

 private:
  friend class VectorBuildByIndexTask;

  void DoAsync() override;
  bool NeedRetry() override;
  int retry_count_{0};

  std::string Name() const override {
    return fmt::format("VectorBuildPartTask-{}-{}", vector_index_->GetId(), part_id_);
  }

  void VectorBuildRpcCallback(const Status& status, VectorBuildRpc* rpc);

  const int64_t part_id_;
  const std::shared_ptr<VectorIndex> vector_index_;

  std::unordered_map<int64_t, std::shared_ptr<Region>> next_batch_region_;

  std::vector<StoreRpcController> controllers_;
  std::vector<std::unique_ptr<VectorBuildRpc>> rpcs_;
  ErrStatusResult result_;

  std::shared_mutex rw_lock_;
  Status status_;

  std::atomic<int> sub_tasks_count_{0};
};


}  // namespace sdk

}  // namespace dingodb
#endif