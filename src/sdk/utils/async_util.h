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

#ifndef DINGODB_SDK_ASYNC_UTIL_H_
#define DINGODB_SDK_ASYNC_UTIL_H_

#include <cstdint>
#include <vector>

#include "glog/logging.h"

#ifdef USE_GRPC

#include <condition_variable>
#include <mutex>
#include <thread>

#else

#include "bthread/bthread.h"

#endif  // USE_GRPC

#include "dingosdk/status.h"
#include "sdk/utils/callback.h"

namespace dingodb {
namespace sdk {

#ifdef USE_GRPC

class Synchronizer {
 public:
  Synchronizer() {}
  ~Synchronizer() {}

  void Wait() {
    std::unique_lock<std::mutex> lk(mutex_);
    while (!fire_) {
      cond_.wait(lk);
    }
  }

  RpcCallback AsRpcCallBack() {
    return [&]() { Fire(); };
  }

  StatusCallback AsStatusCallBack(Status& in_staus) {
    return [&](Status s) {
      in_staus = s;
      Fire();
    };
  }

  void Fire() {
    std::unique_lock<std::mutex> lk(mutex_);
    fire_ = true;
    cond_.notify_one();
  }

 private:
  std::mutex mutex_;
  std::condition_variable cond_;

  bool fire_{false};
};

class ParallelExecutor {
 public:
  static void Execute(uint32_t parallel_num, std::function<void(uint32_t)> func) {
    std::vector<std::thread> thread_pool;
    thread_pool.reserve(parallel_num);
    for (uint32_t i = 0; i < parallel_num; i++) {
      thread_pool.emplace_back(func, i);
    }

    for (auto& thread : thread_pool) {
      thread.join();
    }
  }

  template <typename T>
  static void AsyncExecute(std::vector<T>& tasks, std::function<void(T)> func) {
    std::vector<std::thread> thread_pool;
    thread_pool.reserve(tasks.size());
    for (auto& task : tasks) {
      thread_pool.emplace_back(func, task);
    }

    for (auto& thread : thread_pool) {
      thread.detach();
    }
  }
};

inline void SleepUs(int64_t us) { std::this_thread::sleep_for(std::chrono::microseconds(us)); }

#else

class Synchronizer {
 public:
  Synchronizer() {
    CHECK(bthread_mutex_init(&mutex_, nullptr) == 0) << "bthread_mutex_init fail.";
    CHECK(bthread_cond_init(&cond_, nullptr) == 0) << "bthread_cond_init fail.";
  }

  ~Synchronizer() {
    bthread_cond_destroy(&cond_);
    bthread_mutex_destroy(&mutex_);
  }

  void Wait() {
    bthread_mutex_lock(&mutex_);
    while (!fire_) {
      bthread_cond_wait(&cond_, &mutex_);
    }
    bthread_mutex_unlock(&mutex_);
  }

  RpcCallback AsRpcCallBack() {
    return [&]() { Fire(); };
  }

  StatusCallback AsStatusCallBack(Status& in_staus) {
    return [&](Status s) {
      in_staus = s;
      Fire();
    };
  }

  void Fire() {
    bthread_mutex_lock(&mutex_);
    fire_ = true;
    bthread_cond_signal(&cond_);
    bthread_mutex_unlock(&mutex_);
  }

 private:
  bthread_mutex_t mutex_;
  bthread_cond_t cond_;

  bool fire_{false};
};

class ParallelExecutor {
 public:
  static void Execute(uint32_t parallel_num, std::function<void(uint32_t)> func) {
    struct Param {
      uint32_t index;
      std::function<void(uint32_t)>* func;
    };

    std::vector<bthread_t> tids;
    tids.reserve(parallel_num);
    for (uint32_t i = 0; i < parallel_num; i++) {
      Param* param = new Param{.index = i, .func = new std::function<void(uint32_t)>(func)};
      bthread_t tid;
      CHECK(bthread_start_background(
                &tid, &BTHREAD_ATTR_SMALL,
                [](void* arg) -> void* {
                  Param* param = reinterpret_cast<Param*>(arg);

                  (*param->func)(param->index);

                  delete param->func;
                  delete param;

                  return nullptr;
                },
                param) == 0)
          << "bthread_start_background fail";

      tids.push_back(tid);
    }

    for (auto tid : tids) {
      bthread_join(tid, nullptr);
    }
  }

  template <typename T>
  static void AsyncExecute(std::vector<T>& tasks, std::function<void(T)> func) {
    struct Param {
      T task;
      std::function<void(T)> func;
    };

    std::vector<bthread_t> tids;
    tids.reserve(tasks.size());
    for (auto& task : tasks) {
      Param* param = new Param{.task = task, .func = func};
      bthread_t tid;
      CHECK(bthread_start_background(
                &tid, &BTHREAD_ATTR_SMALL,
                [](void* arg) -> void* {
                  Param* param = reinterpret_cast<Param*>(arg);

                  param->func(param->task);

                  delete param;

                  return nullptr;
                },
                param) == 0)
          << "bthread_start_background fail";

      tids.push_back(tid);
    }
  }
};

inline void SleepUs(int64_t us) { bthread_usleep(us); }

#endif  // USE_GRPC

}  // namespace sdk
}  // namespace dingodb

#endif  // DINGODB_SDK_ASYNC_UTIL_H_