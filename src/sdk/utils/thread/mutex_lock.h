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

#ifndef DINGODB_SDK_THREAD_MUTEX_LOCK_H_
#define DINGODB_SDK_THREAD_MUTEX_LOCK_H_

#include <glog/logging.h>

#include <condition_variable>
#include <shared_mutex>

namespace dingodb {
namespace sdk {

class CondVar;

class Mutex {
 public:
  Mutex() = default;
  ~Mutex() = default;

  Mutex(const Mutex&) = delete;
  Mutex& operator=(const Mutex&) = delete;

  void Lock() { mutex_.lock(); }
  void Unlock() { mutex_.unlock(); }

 private:
  friend class CondVar;
  std::mutex mutex_;
};

class CondVar {
 public:
  explicit CondVar(Mutex* mutex) : mutex_(mutex) { CHECK(mutex != nullptr); }
  ~CondVar() = default;

  CondVar(const CondVar&) = delete;
  CondVar& operator=(const CondVar&) = delete;

  void Wait() {
    std::unique_lock<std::mutex> lock(mutex_->mutex_, std::adopt_lock);
    condition_.wait(lock);
    lock.release();
  }
  void WaitFor(uint64_t us) {
    std::unique_lock<std::mutex> lock(mutex_->mutex_, std::adopt_lock);
    condition_.wait_for(lock, std::chrono::microseconds(us));
    lock.release();
  }

  void NotifyOne() { condition_.notify_one(); }
  void NotifyAll() { condition_.notify_all(); }

 private:
  Mutex* const mutex_;
  std::condition_variable condition_;
};

}  // namespace sdk
}  // namespace dingodb

#endif  // DINGODB_SDK_THREAD_MUTEX_LOCK_H_
