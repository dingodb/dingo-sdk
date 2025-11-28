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

#ifndef DINGODB_SDK_MUTEX_LOCK_H_
#define DINGODB_SDK_MUTEX_LOCK_H_

#ifdef USE_GRPC
#include "sdk/utils/thread/mutex_lock.h"
#else
#include "sdk/utils/bthread/mutex_lock.h"
#endif  // USE_GRPC

namespace dingodb {
namespace sdk {

class LockGuard {
 public:
  explicit LockGuard(Mutex* mutex) : mutex_(mutex) { mutex_->Lock(); }
  LockGuard(const LockGuard&) = delete;
  LockGuard& operator=(const LockGuard&) = delete;
  ~LockGuard() { mutex_->Unlock(); }

 private:
  Mutex* const mutex_;
};

}  // namespace sdk
}  // namespace dingodb

#endif  // DINGODB_SDK_MUTEX_LOCK_H_