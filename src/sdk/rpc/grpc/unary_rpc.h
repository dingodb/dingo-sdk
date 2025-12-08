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

#ifndef DINGODB_SDK_GRPC_UNARY_RPC_H_
#define DINGODB_SDK_GRPC_UNARY_RPC_H_

#include <fmt/format.h>
#include <sys/stat.h>

#include <cstdint>
#include <memory>
#include <string>

#include "common/logging.h"
#include "dingosdk/status.h"
#include "fmt/core.h"
#include "glog/logging.h"
#include "google/protobuf/message.h"
#include "grpcpp/client_context.h"
#include "grpcpp/grpcpp.h"
#include "grpcpp/support/async_unary_call.h"
#include "grpcpp/support/status.h"
#include "grpcpp/support/stub_options.h"
#include "sdk/common/common.h"
#include "sdk/common/param_config.h"
#include "sdk/common/rand.h"
#include "sdk/rpc/rpc.h"
#include "sdk/utils/mutex_lock.h"
#include "sdk/utils/net_util.h"

namespace dingodb {
namespace sdk {

struct GrpcContext : public RpcContext {
  GrpcContext() = default;
  ~GrpcContext() override = default;

  std::shared_ptr<grpc::Channel> channel;
  grpc::CompletionQueue* cq;
  EndPoint endpoint;
};

template <class RequestType, class ResponseType, class ServiceType, class StubType>
class UnaryRpc : public Rpc {
 public:
  UnaryRpc(const std::string& cmd) : Rpc(cmd) {
    context = std::make_unique<grpc::ClientContext>();
    log_id = RandHelper::RandUInt64();
  }

  ~UnaryRpc() override = default;

  RequestType* MutableRequest() { return &request; }

  const RequestType* Request() const { return &request; }

  ResponseType* MutableResponse() { return &response; }

  const ResponseType* Response() const { return &response; }

  google::protobuf::Message* RawMutableRequest() override { return &request; }

  const google::protobuf::Message* RawRequest() const override { return &request; }

  google::protobuf::Message* RawMutableResponse() override { return &response; }

  const google::protobuf::Message* RawResponse() const override { return &response; }

  std::string ServiceName() override { return ServiceType::service_full_name(); }

  std::string ServiceFullName() override { return ServiceType::service_full_name(); }

  grpc::ClientContext* MutableContext() { return context.get(); }

  const grpc::ClientContext* Context() const { return context.get(); }

  uint64_t LogId() const override { return log_id; }

  void OnRpcDone() override {
    if (!grpc_status.ok()) {
      DINGO_LOG(WARNING) << fmt::format(
          "[sdk.rpc.{}] Fail send rpc: {}, endpoint(peer): {}, grpc error_code: {}, error_text: {}", log_id, Method(),
          context->peer(), static_cast<int>(grpc_status.error_code()), grpc_status.error_message());
      Status err = Status::NetworkError(grpc_status.error_code(), grpc_status.error_message());
      SetStatus(err);
    } else {
      DINGO_LOG(DEBUG) << fmt::format(
          "[sdk.rpc.{}] Success send rpc: {}, endpoint(peer): {}, request: {}, response: {}", log_id, Method(),
          context->peer(), request.ShortDebugString(), response.ShortDebugString());
    }

    int64_t end_time =
        std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch())
            .count();
    std::string str = fmt::format("request_id: {}, status: {}", log_id, status.ToString());
    if ((end_time - start_time) > FLAGS_rpc_trace_full_info_threshold_us) {
      // Default log all rpc info if elapse time greater than 1 second
      str += fmt::format(", request: {}, response: {}", request.ShortDebugString(), response.ShortDebugString());
      DINGO_LOG(INFO) << fmt::format("[sdk.trace.rpc][{}][{:.6f}s][endpoint({})] Full rpc info {}", Method(),
                                     (end_time - start_time) / 1e6, context->peer(), str);
    }
    TraceRpcPerformance(end_time - start_time, Method(), context->peer(), str);

    grpc_ctx->cb();
  }

  void Reset() override {
    response.Clear();
    grpc_status = grpc::Status();
    status = Status::OK();
    context->TryCancel();
    context = std::make_unique<grpc::ClientContext>();
  }

  virtual std::unique_ptr<grpc::ClientAsyncResponseReader<ResponseType>> Prepare(StubType* stub,
                                                                                 grpc::CompletionQueue* cq) = 0;

  void Call(RpcContext* ctx) override {
    grpc_ctx.reset(CHECK_NOTNULL(dynamic_cast<GrpcContext*>(ctx)));
    CHECK_NOTNULL(grpc_ctx->channel);
    CHECK_NOTNULL(grpc_ctx->cq);
    grpc::StubOptions options;

    StubType* p_stub = nullptr;
    {
      LockGuard lg(&lk);
      auto iter = stubs.find(grpc_ctx->endpoint);
      if (iter == stubs.end()) {
        auto stub = ServiceType::NewStub(grpc_ctx->channel);
        p_stub = stub.get();
        UnaryRpc::stubs.insert(std::make_pair(grpc_ctx->endpoint, std::move(stub)));
      } else {
        p_stub = iter->second.get();
      }
    }
    CHECK_NOTNULL(p_stub);

    // Record the start time for performance tracing
    start_time =
        std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch())
            .count();

    auto reader = Prepare(p_stub, grpc_ctx->cq);
    reader->Finish(&response, &grpc_status, (void*)this);
  }

 protected:
  RequestType request;
  ResponseType response;
  std::unique_ptr<grpc::ClientContext> context;
  grpc::Status grpc_status;
  std::unique_ptr<StubType> stub;
  std::unique_ptr<GrpcContext> grpc_ctx;
  uint64_t log_id{0};

  int64_t start_time{0};  // record the start time of the RPC call , use for trace

  static std::map<EndPoint, std::unique_ptr<StubType>> stubs;
  static Mutex lk;
};

template <class RequestType, class ResponseType, class ServiceType, class StubType>
std::map<EndPoint, std::unique_ptr<StubType>> UnaryRpc<RequestType, ResponseType, ServiceType, StubType>::stubs;
template <class RequestType, class ResponseType, class ServiceType, class StubType>
Mutex UnaryRpc<RequestType, ResponseType, ServiceType, StubType>::lk;

#define DECLARE_UNARY_RPC_INNER(NS, SERVICE, METHOD, REQ_RSP_PREFIX)                                                 \
  class METHOD##Rpc final                                                                                            \
      : public UnaryRpc<NS::REQ_RSP_PREFIX##Request, NS::REQ_RSP_PREFIX##Response, NS::SERVICE, NS::SERVICE::Stub> { \
   public:                                                                                                           \
    METHOD##Rpc(const METHOD##Rpc&) = delete;                                                                        \
    METHOD##Rpc& operator=(const METHOD##Rpc&) = delete;                                                             \
    explicit METHOD##Rpc();                                                                                          \
    explicit METHOD##Rpc(const std::string& cmd);                                                                    \
    ~METHOD##Rpc() override;                                                                                         \
    std::string Method() const override { return ConstMethod(); }                                                    \
    std::unique_ptr<grpc::ClientAsyncResponseReader<NS::REQ_RSP_PREFIX##Response>> Prepare(                          \
        NS::SERVICE::Stub* stub, grpc::CompletionQueue* cq) override;                                                \
    static std::string ConstMethod();                                                                                \
  };

#define DECLARE_UNARY_RPC(NS, SERVICE, METHOD)                                                       \
  class METHOD##Rpc final                                                                            \
      : public UnaryRpc<NS::METHOD##Request, NS::METHOD##Response, NS::SERVICE, NS::SERVICE::Stub> { \
   public:                                                                                           \
    METHOD##Rpc(const METHOD##Rpc&) = delete;                                                        \
    METHOD##Rpc& operator=(const METHOD##Rpc&) = delete;                                             \
    explicit METHOD##Rpc();                                                                          \
    explicit METHOD##Rpc(const std::string& cmd);                                                    \
    ~METHOD##Rpc() override;                                                                         \
    std::string Method() const override { return ConstMethod(); }                                    \
    std::unique_ptr<grpc::ClientAsyncResponseReader<NS::METHOD##Response>> Prepare(                  \
        NS::SERVICE::Stub* stub, grpc::CompletionQueue* cq) override;                                \
    static std::string ConstMethod();                                                                \
  };

#define DEFINE_UNAEY_RPC_INNER(NS, SERVICE, METHOD, REQ_RSP_PREFIX)                                    \
  METHOD##Rpc::METHOD##Rpc() : METHOD##Rpc("") {}                                                      \
  METHOD##Rpc::METHOD##Rpc(const std::string& cmd) : UnaryRpc(cmd) {}                                  \
  METHOD##Rpc::~METHOD##Rpc() = default;                                                               \
  std::unique_ptr<grpc::ClientAsyncResponseReader<NS::REQ_RSP_PREFIX##Response>> METHOD##Rpc::Prepare( \
      NS::SERVICE::Stub* stub, grpc::CompletionQueue* cq) {                                            \
    return stub->Async##METHOD(MutableContext(), request, cq);                                         \
  }                                                                                                    \
  std::string METHOD##Rpc::ConstMethod() { return fmt::format("{}.{}Rpc", NS::SERVICE::service_full_name(), #METHOD); }

#define DEFINE_UNAEY_RPC(NS, SERVICE, METHOD)                                                  \
  METHOD##Rpc::METHOD##Rpc() : METHOD##Rpc("") {}                                              \
  METHOD##Rpc::METHOD##Rpc(const std::string& cmd) : UnaryRpc(cmd) {}                          \
  METHOD##Rpc::~METHOD##Rpc() = default;                                                       \
  std::unique_ptr<grpc::ClientAsyncResponseReader<NS::METHOD##Response>> METHOD##Rpc::Prepare( \
      NS::SERVICE::Stub* stub, grpc::CompletionQueue* cq) {                                    \
    return stub->Async##METHOD(MutableContext(), request, cq);                                 \
  }                                                                                            \
  std::string METHOD##Rpc::ConstMethod() { return fmt::format("{}.{}Rpc", NS::SERVICE::service_full_name(), #METHOD); }

}  // namespace sdk
}  // namespace dingodb

#endif  // DINGODB_SDK_GRPC_UNARY_RPC_H_