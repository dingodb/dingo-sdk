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

#include <glog/logging.h>

#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <thread>
#include <vector>

#ifndef USE_GRPC
#include "bthread/bthread.h"
#endif  // USE_GRPC

#include "dingosdk/status.h"
#include "sdk/utils/callback.h"

namespace dingodb {
namespace sdk {

class Synchronizer {
 public:
  Synchronizer() {
#ifndef USE_GRPC
    CHECK(bthread_mutex_init(&mutex_, nullptr) == 0) << "bthread_mutex_init fail.";
    CHECK(bthread_cond_init(&cond_, nullptr) == 0) << "bthread_cond_init fail.";
#endif  // USE_GRPC
  }

  ~Synchronizer() {
#ifndef USE_GRPC
    bthread_cond_destroy(&cond_);
    bthread_mutex_destroy(&mutex_);
#endif  // USE_GRPC
  }

  void Wait() {
#ifdef USE_GRPC
    std::unique_lock<std::mutex> lk(mutex_);
    while (!fire_) {
      cond_.wait(lk);
    }

#else
    bthread_mutex_lock(&mutex_);
    while (!fire_) {
      bthread_cond_wait(&cond_, &mutex_);
    }
    bthread_mutex_unlock(&mutex_);
#endif  // USE_GRPC
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
#ifdef USE_GRPC
    std::unique_lock<std::mutex> lk(mutex_);
    fire_ = true;
    cond_.notify_one();

#else
    bthread_mutex_lock(&mutex_);
    fire_ = true;
    bthread_cond_signal(&cond_);
    bthread_mutex_unlock(&mutex_);

#endif  // USE_GRPC
  }

 private:
#ifdef USE_GRPC
  std::mutex mutex_;
  std::condition_variable cond_;
#else
  bthread_mutex_t mutex_;
  bthread_cond_t cond_;

#endif  // USE_GRPC

  bool fire_{false};
};

class ParallelExecutor {
 public:
  static void Execute(uint32_t parallel_num, std::function<void(uint32_t)> func) {
#ifdef USE_GRPC

    std::vector<std::thread> thread_pool;
    thread_pool.reserve(parallel_num);
    for (uint32_t i = 0; i < parallel_num; i++) {
      thread_pool.emplace_back(func, i);
    }

    for (auto& thread : thread_pool) {
      thread.join();
    }

#else

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
#endif  // USE_GRPC
  }
};

inline void Sleep(int64_t us) {
#ifdef USE_GRPC
  std::this_thread::sleep_for(std::chrono::microseconds(us));
#else
  bthread_usleep(us);
#endif  // USE_GRPC
}

}  // namespace sdk
}  // namespace dingodb

#endif  // DINGODB_SDK_ASYNC_UTIL_H_