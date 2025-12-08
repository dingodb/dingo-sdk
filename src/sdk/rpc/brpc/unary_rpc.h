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

#ifndef DINGODB_SDK_BRPC_UNARY_RPC_H_
#define DINGODB_SDK_BRPC_UNARY_RPC_H_

#include <fmt/format.h>
#include <sys/stat.h>

#include <cstdint>
#include <string>

#include "brpc/callback.h"
#include "brpc/channel.h"
#include "brpc/controller.h"
#include "butil/fast_rand.h"
#include "common/logging.h"
#include "dingosdk/status.h"
#include "fmt/core.h"
#include "glog/logging.h"
#include "google/protobuf/message.h"
#include "sdk/common/common.h"
#include "sdk/common/param_config.h"
#include "sdk/common/rand.h"
#include "sdk/rpc/rpc.h"
#include "sdk/utils/callback.h"

namespace dingodb {
namespace sdk {

struct BrpcContext : public RpcContext {
  BrpcContext() = default;
  ~BrpcContext() override = default;

  std::shared_ptr<brpc::Channel> channel;
};

template <class RequestType, class ResponseType, class ServiceType, class StubType>
class UnaryRpc : public Rpc {
 public:
  UnaryRpc(const std::string& cmd) : Rpc(cmd) {
    request = new RequestType;
    response = new ResponseType;
    log_id = RandHelper::RandUInt64();
    controller.set_log_id(log_id);
    brpc_ctx = nullptr;
  }

  ~UnaryRpc() override {
    delete request;
    delete response;
    delete brpc_ctx;
  }

  RequestType* MutableRequest() { return request; }

  const RequestType* Request() const { return request; }

  ResponseType* MutableResponse() { return response; }

  const ResponseType* Response() const { return response; }

  google::protobuf::Message* RawMutableRequest() override { return request; }

  const google::protobuf::Message* RawRequest() const override { return request; }

  google::protobuf::Message* RawMutableResponse() override { return response; }

  const google::protobuf::Message* RawResponse() const override { return response; }

  std::string ServiceName() override { return ServiceType::descriptor()->name(); }

  std::string ServiceFullName() override { return ServiceType::descriptor()->full_name(); }

  brpc::Controller* MutableController() { return &controller; }

  const brpc::Controller* Controller() const { return &controller; }

  uint64_t LogId() const override { return controller.log_id(); }

  void OnRpcDone() override {
    if (controller.Failed()) {
      DINGO_LOG(WARNING) << fmt::format("[sdk.rpc.{}] Fail send rpc: {}, endpoint: {}, error_code: {}, error_text: {}",
                                        controller.log_id(), Method(), endpoint2str(controller.remote_side()).c_str(),
                                        controller.ErrorCode(), controller.ErrorText());

      Status err = Status::NetworkError(controller.ErrorCode(), controller.ErrorText());
      SetStatus(err);
    } else {
      DINGO_LOG(DEBUG) << fmt::format("[sdk.rpc.{}] Success send rpc: {}, endpoint: {}, request: {}, response: {}",
                                      controller.log_id(), Method(), endpoint2str(controller.remote_side()).c_str(),
                                      request->ShortDebugString(), response->ShortDebugString());
    }

    int64_t end_time =
        std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch())
            .count();
    std::string str = fmt::format("request_id: {}, status: {}", controller.log_id(), status.ToString());
    if ((end_time - start_time) > FLAGS_rpc_trace_full_info_threshold_us) {
      // Default log all rpc info if elapse time greater than 1 second
      str += fmt::format(",request : {}, response : {}", request->ShortDebugString(), response->ShortDebugString());
      DINGO_LOG(INFO) << fmt::format("[sdk.trace.rpc][{}][{:.6f}s][endpoint({})] Full rpc info {}", Method(),
                                     (end_time - start_time) / 1e6, endpoint2str(controller.remote_side()).c_str(),
                                     str);
    }
    TraceRpcPerformance(end_time - start_time, Method(), endpoint2str(controller.remote_side()).c_str(), str);

    brpc_ctx->cb();
  }

  void Reset() override {
    response->Clear();
    controller.Reset();
    controller.set_timeout_ms(FLAGS_rpc_time_out_ms);
    controller.set_max_retry(FLAGS_rpc_max_retry);
    controller.set_log_id(log_id);
    status = Status::OK();
  }

  // virtual void Call(RpcContext* ctx) = 0;
  // void Call(void* channel, RpcCallback cb, void* cq) override {
  void Call(RpcContext* ctx) override {
    brpc_ctx = dynamic_cast<BrpcContext*>(ctx);
    CHECK_NOTNULL(brpc_ctx);
    CHECK_NOTNULL(brpc_ctx->channel);
    StubType stub(brpc_ctx->channel.get());

    // Record the start time for performance tracing
    start_time =
        std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch())
            .count();

    Send(stub, brpc::NewCallback(this, &UnaryRpc::OnRpcDone));
  }

  virtual void Send(StubType& stub, google::protobuf::Closure* done) = 0;

 protected:
  RequestType* request;
  ResponseType* response;
  brpc::Controller controller;
  BrpcContext* brpc_ctx;
  int64_t start_time{0};  // record the start time of the RPC call , use for trace
  int64_t log_id{0};
};

#define DECLARE_UNARY_RPC_INNER(NS, SERVICE, METHOD, REQ_RSP_PREFIX)                                                  \
  class METHOD##Rpc final                                                                                             \
      : public UnaryRpc<NS::REQ_RSP_PREFIX##Request, NS::REQ_RSP_PREFIX##Response, NS::SERVICE, NS::SERVICE##_Stub> { \
   public:                                                                                                            \
    METHOD##Rpc(const METHOD##Rpc&) = delete;                                                                         \
    METHOD##Rpc& operator=(const METHOD##Rpc&) = delete;                                                              \
    explicit METHOD##Rpc();                                                                                           \
    explicit METHOD##Rpc(const std::string& cmd);                                                                     \
    ~METHOD##Rpc() override;                                                                                          \
    std::string Method() const override { return ConstMethod(); }                                                     \
    void Send(NS::SERVICE##_Stub& stub, google::protobuf::Closure* done) override;                                    \
    static std::string ConstMethod();                                                                                 \
  };

#define DECLARE_UNARY_RPC(NS, SERVICE, METHOD)                                                        \
  class METHOD##Rpc final                                                                             \
      : public UnaryRpc<NS::METHOD##Request, NS::METHOD##Response, NS::SERVICE, NS::SERVICE##_Stub> { \
   public:                                                                                            \
    METHOD##Rpc(const METHOD##Rpc&) = delete;                                                         \
    METHOD##Rpc& operator=(const METHOD##Rpc&) = delete;                                              \
    explicit METHOD##Rpc();                                                                           \
    explicit METHOD##Rpc(const std::string& cmd);                                                     \
    ~METHOD##Rpc() override;                                                                          \
    std::string Method() const override { return ConstMethod(); }                                     \
    void Send(NS::SERVICE##_Stub& stub, google::protobuf::Closure* done) override;                    \
    static std::string ConstMethod();                                                                 \
  };

#define DEFINE_UNAEY_RPC(NS, SERVICE, METHOD)                                         \
  METHOD##Rpc::METHOD##Rpc() : METHOD##Rpc("") {}                                     \
  METHOD##Rpc::METHOD##Rpc(const std::string& cmd) : UnaryRpc(cmd) {}                 \
  METHOD##Rpc::~METHOD##Rpc() = default;                                              \
  void METHOD##Rpc::Send(NS::SERVICE##_Stub& stub, google::protobuf::Closure* done) { \
    stub.METHOD(MutableController(), request, response, done);                        \
  }                                                                                   \
  std::string METHOD##Rpc::ConstMethod() { return fmt::format("{}.{}Rpc", NS::SERVICE::descriptor()->name(), #METHOD); }

}  // namespace sdk
}  // namespace dingodb

#endif  // DINGODB_SDK_BRPC_UNARY_RPC_H_