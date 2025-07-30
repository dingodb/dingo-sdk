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

#ifndef DINGODB_SDK_TRANSACTION_TSO_H_
#define DINGODB_SDK_TRANSACTION_TSO_H_

#include <cstdint>
#include <memory>

#include "dingosdk/status.h"
#include "proto/meta.pb.h"
#include "sdk/utils/rw_lock.h"

namespace dingodb {
namespace sdk {

class ClientStub;

class TsoProvider {
 public:
  TsoProvider(const ClientStub& stub);
  ~TsoProvider() = default;

  using TsoTimestamp = pb::meta::TsoTimestamp;

  Status GenTs(uint32_t count, int64_t& ts);

  void Refresh();

 private:
  // when period is beyond 1ms is considered stale
  bool IsStale();
  Status FetchTso(uint32_t count);

  const ClientStub& stub_;

  // fetch batch size
  const uint32_t batch_size_;

  RWLock rwlock_;

  int64_t physical_{0};
  int64_t next_logical_{0};
  int64_t max_logical_{0};

  uint64_t last_time_us_{0};
};

using TsoProviderSPtr = std::shared_ptr<TsoProvider>;

}  // namespace sdk
}  // namespace dingodb

#endif  // DINGODB_SDK_TRANSACTION_TSO_H_
