#ifndef DINGODB_SDK_VECTOR_DISKANN_DUMP_H_
#define DINGODB_SDK_VECTOR_DISKANN_DUMP_H_

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

class VectorDumpPartTask;
class VectorDumpTask : public VectorTask {
 public:
  VectorDumpTask(const ClientStub& stub, int64_t index_id) : VectorTask(stub), index_id_(index_id) {}

  ~VectorDumpTask() override = default;

 private:
  Status Init() override;
  void DoAsync() override;

  std::string Name() const override { return fmt::format("VectorDumpTask-{}", index_id_); }

  void SubTaskCallback(Status status, VectorDumpPartTask* sub_task);

  const int64_t index_id_;

  std::shared_ptr<VectorIndex> vector_index_;

  std::shared_mutex rw_lock_;
  std::set<int64_t> next_part_ids_;
  Status status_;

  std::atomic<int> sub_tasks_count_{0};
};

class VectorDumpPartTask : public VectorTask {
 public:
  VectorDumpPartTask(const ClientStub& stub, std::shared_ptr<VectorIndex> vector_index, int64_t part_id)
      : VectorTask(stub), vector_index_(std::move(vector_index)), part_id_(part_id) {}

  ~VectorDumpPartTask() override = default;

 private:
  friend class VectorDumpTask;

  void DoAsync() override;

  std::string Name() const override {
    return fmt::format("VectorDumpPartTask-{}-{}", vector_index_->GetId(), part_id_);
  }

  void VectorDumpRpcCallback(const Status& status, VectorDumpRpc* rpc);

  const int64_t part_id_;
  const std::shared_ptr<VectorIndex> vector_index_;

  std::unordered_map<int64_t, std::shared_ptr<Region>> next_batch_region_;

  std::vector<StoreRpcController> controllers_;
  std::vector<std::unique_ptr<VectorDumpRpc>> rpcs_;

  std::shared_mutex rw_lock_;
  Status status_;

  std::atomic<int> sub_tasks_count_{0};
};

}  // namespace sdk

}  // namespace dingodb
#endif