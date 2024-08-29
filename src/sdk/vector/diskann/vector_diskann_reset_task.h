#ifndef DINGODB_SDK_VECTOR_DISKANN_RESET_TASK_H_
#define DINGODB_SDK_VECTOR_DISKANN_RESET_TASK_H_

#include <cstdint>
#include <memory>
#include <unordered_map>

#include "proto/common.pb.h"
#include "sdk/client_stub.h"
#include "sdk/rpc/index_service_rpc.h"
#include "sdk/rpc/store_rpc_controller.h"
#include "sdk/vector.h"
#include "sdk/vector/vector_index.h"
#include "sdk/vector/vector_task.h"

namespace dingodb {
namespace sdk {

class VectorResetPartTask;
class VectorResetByIndexTask : public VectorTask {
 public:
  VectorResetByIndexTask(const ClientStub& stub, int64_t index_id, ErrStatusResult& result)
      : VectorTask(stub), index_id_(index_id), result_(result) {}

  ~VectorResetByIndexTask() override = default;

 private:
  Status Init() override;
  void DoAsync() override;
  bool NeedRetry() override { return false; };

  std::string Name() const override { return fmt::format("VectorResetByIndexTask-{}", index_id_); }
  void SubTaskCallback(const Status& status, VectorResetPartTask* sub_task);

  const int64_t index_id_;
  ErrStatusResult& result_;

  std::shared_ptr<VectorIndex> vector_index_;

  std::shared_mutex rw_lock_;
  std::set<int64_t> next_part_ids_;
  Status status_;

  std::atomic<int> sub_tasks_count_{0};
};

class VectorResetPartTask : public VectorTask {
 public:
  VectorResetPartTask(const ClientStub& stub, std::shared_ptr<VectorIndex> vector_index, int64_t part_id)
      : VectorTask(stub), vector_index_(std::move(vector_index)), part_id_(part_id) {}

  ~VectorResetPartTask() override = default;

  ErrStatusResult GetResult() const { return result_; }

 private:
  friend class VectorResetByIndexTask;

  void DoAsync() override;
  bool NeedRetry() override;
  int retry_count_{0};

  std::string Name() const override {
    return fmt::format("VectorResetPartTask-{}-{}", vector_index_->GetId(), part_id_);
  }

  void VectorResetRpcCallback(const Status& status, VectorResetRpc* rpc);

  const int64_t part_id_;
  const std::shared_ptr<VectorIndex> vector_index_;

  std::unordered_map<int64_t, std::shared_ptr<Region>> next_batch_region_;

  std::vector<StoreRpcController> controllers_;
  std::vector<std::unique_ptr<VectorResetRpc>> rpcs_;
  ErrStatusResult result_;

  std::shared_mutex rw_lock_;
  Status status_;

  std::atomic<int> sub_tasks_count_{0};
};

class VectorResetByRegionTask : public VectorTask {
 public:
  VectorResetByRegionTask(const ClientStub& stub, int64_t index_id, const std::vector<int64_t>& region_ids,
                          ErrStatusResult& result)
      : VectorTask(stub), index_id_(index_id), region_ids_(region_ids), result_(result) {}

  ~VectorResetByRegionTask() override = default;

 private:
  Status Init() override;
  void DoAsync() override;
  bool NeedRetry() override;
  int retry_count_{0};

  std::string Name() const override { return fmt::format("VectorResetByRegionTask-{}", index_id_); }

  void VectorResetByRegionRpcCallback(const Status& status, VectorResetRpc* rpc);

  const int64_t index_id_;
  const std::vector<int64_t>& region_ids_;
  ErrStatusResult& result_;

  std::shared_ptr<VectorIndex> vector_index_;

  std::unordered_map<int64_t, std::shared_ptr<Region>> regions_;
  std::vector<StoreRpcController> controllers_;
  std::vector<std::unique_ptr<VectorResetRpc>> rpcs_;
  std::atomic<int> sub_tasks_count_{0};

  std::shared_mutex rw_lock_;
  Status status_;
};

}  // namespace sdk

}  // namespace dingodb
#endif