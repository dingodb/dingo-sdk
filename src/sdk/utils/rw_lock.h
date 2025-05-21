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

#ifndef DINGODB_SDK_RW_LOCK_H_
#define DINGODB_SDK_RW_LOCK_H_

#ifdef USE_GRPC

#include <shared_mutex>

#else

#include "bthread/bthread.h"

#endif  // USE_GRPC

namespace dingodb {
namespace sdk {

class IRWLock {
 public:
  IRWLock(const IRWLock&) = delete;
  IRWLock& operator=(const IRWLock&) = delete;

  virtual void WRLock() = 0;
  virtual int TryWRLock() = 0;
  virtual void UnWRLock() = 0;

  virtual void RDLock() = 0;
  virtual int TryRDLock() = 0;
  virtual void UnRDLock() = 0;

 protected:
  IRWLock() = default;
  virtual ~IRWLock() = default;
};

#ifdef USE_GRPC

class PthreadRWLock : public IRWLock {
 public:
  PthreadRWLock() = default;
  ~PthreadRWLock() override = default;

  void WRLock() override { mutex_.lock(); }

  int TryWRLock() override { return mutex_.try_lock() ? 0 : 1; }

  void UnWRLock() override { mutex_.unlock(); }

  void RDLock() override { mutex_.lock_shared(); }

  int TryRDLock() override { return mutex_.try_lock_shared() ? 0 : 1; }

  void UnRDLock() override { mutex_.unlock_shared(); }

 private:
  std::shared_mutex mutex_;
};

using RWLock = PthreadRWLock;

#else

class BthreadRWLock : public IRWLock {
 public:
  BthreadRWLock() { bthread_rwlock_init(&rwlock_, nullptr); }
  ~BthreadRWLock() override { bthread_rwlock_destroy(&rwlock_); }

  void WRLock() override {
    int ret = bthread_rwlock_wrlock(&rwlock_);
    CHECK(0 == ret) << "wlock failed: " << ret << ", " << strerror(ret);
  }

  int TryWRLock() override { return bthread_rwlock_trywrlock(&rwlock_); }

  void UnWRLock() override { bthread_rwlock_unlock(&rwlock_); }

  void RDLock() override {
    int ret = bthread_rwlock_rdlock(&rwlock_);
    CHECK(0 == ret) << "rlock failed: " << ret << ", " << strerror(ret);
  }

  int TryRDLock() override { return bthread_rwlock_tryrdlock(&rwlock_); }

  void UnRDLock() override { bthread_rwlock_unlock(&rwlock_); }

 private:
  bthread_rwlock_t rwlock_;
};

using RWLock = BthreadRWLock;

#endif  // USE_GRPC

class ReadLockGuard {
 public:
  explicit ReadLockGuard(IRWLock& rwlock) : rwlock_(rwlock) { rwlock_.RDLock(); }
  ReadLockGuard(const ReadLockGuard&) = delete;
  ReadLockGuard& operator=(const ReadLockGuard&) = delete;

  ~ReadLockGuard() { rwlock_.UnRDLock(); }

 private:
  IRWLock& rwlock_;
};

class WriteLockGuard {
 public:
  explicit WriteLockGuard(IRWLock& rwlock) : rwlock_(rwlock) { rwlock_.WRLock(); }
  WriteLockGuard(const WriteLockGuard&) = delete;
  WriteLockGuard& operator=(const WriteLockGuard&) = delete;

  ~WriteLockGuard() { rwlock_.UnWRLock(); }

 private:
  IRWLock& rwlock_;
};

}  // namespace sdk
}  // namespace dingodb

#endif  // DINGODB_SDK_RW_LOCK_H_