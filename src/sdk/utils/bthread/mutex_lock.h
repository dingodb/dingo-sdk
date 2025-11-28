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

#ifndef DINGODB_SDK_BTHREAD_MUTEX_LOCK_H_
#define DINGODB_SDK_BTHREAD_MUTEX_LOCK_H_

#include <butil/time.h>

#include "bthread/bthread.h"

namespace dingodb {
namespace sdk {

class CondVar;

class Mutex {
 public:
  Mutex() { CHECK(bthread_mutex_init(&mutex_, nullptr) == 0) << "bthread_mutex_init fail."; }
  ~Mutex() { bthread_mutex_destroy(&mutex_); }

  void Lock() { bthread_mutex_lock(&mutex_); }
  void Unlock() { bthread_mutex_unlock(&mutex_); }

 private:
  friend class CondVar;
  bthread_mutex_t mutex_;
};

class CondVar {
 public:
  explicit CondVar(Mutex* mutex) : mutex_(mutex) {
    CHECK(bthread_cond_init(&cond_, nullptr) == 0) << "bthread_cond_init fail.";
  }
  ~CondVar() { bthread_cond_destroy(&cond_); }

  CondVar(const CondVar&) = delete;
  CondVar& operator=(const CondVar&) = delete;

  void Wait() { bthread_cond_wait(&cond_, &mutex_->mutex_); }
  void WaitFor(uint64_t us) {
    struct timespec abstime = butil::microseconds_from_now(us);
    bthread_cond_timedwait(&cond_, &mutex_->mutex_, &abstime);
  }

  void NotifyOne() { bthread_cond_signal(&cond_); }
  void NotifyAll() { bthread_cond_broadcast(&cond_); }

 private:
  Mutex* const mutex_;
  bthread_cond_t cond_;
};

}  // namespace sdk
}  // namespace dingodb

#endif  // DINGODB_SDK_BTHREAD_MUTEX_LOCK_H_
