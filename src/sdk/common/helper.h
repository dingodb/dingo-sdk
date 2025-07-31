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

#ifndef DINGODB_SDK_HELPER_H_
#define DINGODB_SDK_HELPER_H_

#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iostream>

#include "common/logging.h"
#include "fmt/core.h"
#include "glog/logging.h"
#include "proto/meta.pb.h"
#include "sdk/client_stub.h"
#include "sdk/rpc/store_rpc_controller.h"
#include "sdk/utils/net_util.h"

namespace dingodb {
namespace sdk {

inline uint64_t TimestampMs() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
      .count();
}

inline uint64_t TimestampUs() {
  return std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch())
      .count();
}

inline int64_t Timestamp() {
  return std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch()).count();
}

inline std::string FormatTime(int64_t timestamp, const std::string& format = "%Y-%m-%d %H:%M:%S") {
  std::chrono::time_point<std::chrono::system_clock, std::chrono::seconds> tp((std::chrono::seconds(timestamp)));

  auto in_time_t = std::chrono::system_clock::to_time_t(tp);
  std::stringstream ss;
  ss << std::put_time(std::localtime(&in_time_t), format.c_str());
  return ss.str();
}

inline std::string NowTime() { return FormatTime(Timestamp()); }

// TODO: log in rpc when we support async
template <class StoreClientRpc>
static Status LogAndSendRpc(const ClientStub& stub, StoreClientRpc& rpc, std::shared_ptr<Region> region) {
  auto start_time_ms = TimestampMs();

  StoreRpcController controller(stub, rpc, region);
  Status s = controller.Call();

  DINGO_LOG_IF(INFO, fLB::FLAGS_log_rpc_time) << fmt::format("[rpc.{}][{}ms][region.{}] rpc finish", rpc.Method(),
                                                             TimestampMs() - start_time_ms, region->RegionId());
  return s;
}

static std::vector<std::string> Split(const std::string& s, const std::string& delimiters) {
  std::vector<std::string> parts;
  size_t start = 0;
  size_t end = s.find_first_of(delimiters);

  while (end != std::string::npos) {
    if (end != start) {
      parts.push_back(s.substr(start, end - start));
    }
    start = end + 1;
    end = s.find_first_of(delimiters, start);
  }

  if (start != s.length()) {
    parts.push_back(s.substr(start));
  }

  return parts;
}

static EndPoint StringToEndPoint(const std::string& addr) {
  EndPoint endpoint;

  size_t pos = addr.find(':');
  if (pos != std::string::npos) {
    std::string host = addr.substr(0, pos);
    uint16_t port = std::stoi(addr.substr(pos + 1));
    endpoint = EndPoint(host, port);
  }

  return endpoint;
}

// addrs: 127.0.0.1:8201,127.0.0.1:8202,127.0.0.1:8203
static std::vector<EndPoint> StringToEndpoints(const std::string& addrs) {
  std::vector<std::string> parts = Split(addrs, ", ");

  std::vector<EndPoint> endpoints;
  endpoints.reserve(parts.size());
  for (const auto& part : parts) {
    if (!part.empty()) {
      auto end_point = StringToEndPoint(part);
      CHECK(end_point.IsValid()) << "Invalid addrs: " << part;
      endpoints.push_back(end_point);
    }
  }

  CHECK(!endpoints.empty()) << "Invalid addrs: " << addrs;

  return endpoints;
}

static std::string EndPointToString(const std::vector<EndPoint>& endpoints) {
  std::string addrs;
  for (const auto& endpoint : endpoints) {
    addrs.append(endpoint.ToString()).append(",");
  }

  addrs.pop_back();

  return addrs;
}

static bool IsExistPath(const std::string& path) { return std::filesystem::exists(path); }

static bool IsServiceUrlValid(const std::string& service_url) { return service_url.substr(0, 7) == "file://"; }

static std::vector<EndPoint> FileNamingServiceUrlEndpoints(const std::string& naming_service_url) {
  CHECK(naming_service_url.substr(0, 7) == "file://") << "Invalid naming_service_url: " << naming_service_url;

  std::string file_path = naming_service_url.substr(7);

  std::ifstream file(file_path);
  if (!file.is_open()) {
    DINGO_LOG(ERROR) << fmt::format("Open file({}) failed, maybe not exist!", file_path);
    return {};
  }

  std::vector<EndPoint> endpoints;
  std::string line;
  while (std::getline(file, line)) {
    if (line.empty()) {
      continue;
    }
    if (line.find('#') == 0) {
      continue;
    }

    endpoints.push_back(StringToEndPoint(line));
  }

  return endpoints;
}

static std::string StringToHex(const std::string& str) {
  std::stringstream ss;
  for (const auto& ch : str) {
    ss << std::setw(2) << std::setfill('0') << std::hex << static_cast<int>(static_cast<unsigned char>(ch));
  }
  return ss.str();
}

static std::string StringToHex(const std::string_view& str) {
  std::stringstream ss;
  for (const auto& ch : str) {
    ss << std::setw(2) << std::setfill('0') << std::hex << static_cast<int>(static_cast<unsigned char>(ch));
  }
  return ss.str();
}

}  // namespace sdk
}  // namespace dingodb

#endif  // DINGODB_SDK_HELPER_H_