#ifndef DINGODB_SDK_VECTOR_DISKANN_LOAD_TASK_H_
#define DINGODB_SDK_VECTOR_DISKANN_LOAD_TASK_H_

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

class VectorLoadPartTask;
class VectorLoadByIndexTask : public VectorTask {
 public:
  VectorLoadByIndexTask(const ClientStub& stub, int64_t index_id, ErrStatusResult& result)
      : VectorTask(stub), index_id_(index_id), result_(result) {}

  ~VectorLoadByIndexTask() override = default;

 private:
  Status Init() override;
  void DoAsync() override;
  bool NeedRetry() override { return false; };

  std::string Name() const override { return fmt::format("VectorLoadByIndexTask-{}", index_id_); }
  void SubTaskCallback(const Status& status, VectorLoadPartTask* sub_task);

  const int64_t index_id_;
  ErrStatusResult& result_;
  pb::common::VectorLoadParameter paramer_;

  std::shared_ptr<VectorIndex> vector_index_;

  std::shared_mutex rw_lock_;
  std::set<int64_t> next_part_ids_;
  Status status_;

  std::atomic<int> sub_tasks_count_{0};
};

class VectorLoadPartTask : public VectorTask {
 public:
  VectorLoadPartTask(const ClientStub& stub, std::shared_ptr<VectorIndex> vector_index, int64_t part_id,
                     const pb::common::VectorLoadParameter& param)
      : VectorTask(stub), vector_index_(std::move(vector_index)), part_id_(part_id), param_(param) {}

  ~VectorLoadPartTask() override = default;

  ErrStatusResult GetResult() const { return result_; }

 private:
  friend class VectorLoadByIndexTask;

  void DoAsync() override;
  bool NeedRetry() override;
  int retry_count_{0};

  std::string Name() const override {
    return fmt::format("VectorLoadPartTask-{}-{}", vector_index_->GetId(), part_id_);
  }

  void VectorLoadRpcCallback(const Status& status, VectorLoadRpc* rpc);

  const int64_t part_id_;
  const std::shared_ptr<VectorIndex> vector_index_;
  const pb::common::VectorLoadParameter& param_;

  std::unordered_map<int64_t, std::shared_ptr<Region>> next_batch_region_;

  std::vector<StoreRpcController> controllers_;
  std::vector<std::unique_ptr<VectorLoadRpc>> rpcs_;
  ErrStatusResult result_;

  std::shared_mutex rw_lock_;
  Status status_;

  std::atomic<int> sub_tasks_count_{0};
};

class VectorLoadByRegionTask : public VectorTask {
 public:
  VectorLoadByRegionTask(const ClientStub& stub, int64_t index_id, const std::vector<int64_t>& region_ids,
                         ErrStatusResult& result)
      : VectorTask(stub), index_id_(index_id), region_ids_(region_ids), result_(result) {}

  ~VectorLoadByRegionTask() override = default;

 private:
  Status Init() override;
  void DoAsync() override;
  bool NeedRetry() override;
  int retry_count_{0};

  std::string Name() const override { return fmt::format("VectorLoadByRegionTask-{}", index_id_); }

  void VectorLoadByRegionRpcCallback(const Status& status, VectorLoadRpc* rpc);

  const int64_t index_id_;
  const std::vector<int64_t>& region_ids_;
  ErrStatusResult& result_;
  ErrStatusResult tmp_result_;

  std::shared_ptr<VectorIndex> vector_index_;
  pb::common::VectorLoadParameter param_;

  std::unordered_map<int64_t, std::shared_ptr<Region>> regions_;
  std::vector<StoreRpcController> controllers_;
  std::vector<std::unique_ptr<VectorLoadRpc>> rpcs_;
  std::atomic<int> sub_tasks_count_{0};

  std::shared_mutex rw_lock_;
  Status status_;
};

}  // namespace sdk

}  // namespace dingodb
#endif