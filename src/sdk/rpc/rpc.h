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

#ifndef DINGODB_SDK_RPC_H_
#define DINGODB_SDK_RPC_H_

#include <cstdint>
#include <string>

#include "dingosdk/status.h"
#include "google/protobuf/message.h"
#include "sdk/utils/callback.h"
#include "sdk/utils/net_util.h"

namespace dingodb {
namespace sdk {

struct RpcContext {
  virtual ~RpcContext() = default;

  RpcCallback cb;
};

class Rpc {
 public:
  Rpc(const std::string& p_cmd) : cmd(p_cmd) {}

  virtual ~Rpc() = default;

  const EndPoint& GetEndPoint() const { return end_point; }

  void SetEndPoint(const EndPoint& p_end_point) { end_point = p_end_point; }

  Status GetStatus() { return status; }

  void SetStatus(const Status& s) { status = s; }

  void IncRetryTimes() { retry_times++; }

  int GetRetryTimes() const { return retry_times; }

  void IncSleepTimesUs(uint64_t elapsed_time) { sleep_time_us += elapsed_time; }

  int GetSleepTimesUs() const { return sleep_time_us; }

  void IncSleepCount() { sleep_count++; }

  int GetSleepCount() const { return sleep_count; }

  virtual google::protobuf::Message* RawMutableRequest() = 0;

  virtual const google::protobuf::Message* RawRequest() const = 0;

  virtual google::protobuf::Message* RawMutableResponse() = 0;

  virtual const google::protobuf::Message* RawResponse() const = 0;

  virtual std::string ServiceName() = 0;

  virtual std::string ServiceFullName() = 0;

  virtual std::string Method() const = 0;

  virtual void Reset() = 0;

  virtual void Call(RpcContext* ctx) = 0;

  virtual void OnRpcDone() = 0;

  virtual uint64_t LogId() const = 0;

  virtual uint64_t ElapsedTimeUs() const = 0;

  StatusCallback call_back;

 protected:
  std::string cmd;
  EndPoint end_point;
  Status status;
  int retry_times{0};
  uint64_t sleep_time_us{0};
  uint64_t sleep_count{0};
};

}  // namespace sdk
}  // namespace dingodb

#endif  // DINGODB_SDK_RPC_H_