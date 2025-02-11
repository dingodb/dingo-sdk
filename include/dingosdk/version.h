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

#ifndef DINGODB_SDK_VERSION_H_
#define DINGODB_SDK_VERSION_H_

#include <cstdint>
#include <string>
#include <vector>

#include "dingosdk/status.h"

namespace dingodb {
namespace sdk {

class ClientStub;

class Version {
 public:
  Version(const ClientStub& stub) : stub_(stub) {}

  struct Range {
    std::string start_key;
    std::string end_key;
  };

  struct KVPair {
    std::string key;
    std::string value;
  };

  struct KVWithExt {
    KVPair kv;
    int64_t create_revision;
    int64_t mod_revision;
    int64_t version;
    int64_t lease;
  };

  struct Options {
    bool need_prev_kv{false};
    bool ignore_value{false};
    bool ignore_lease{false};
    bool keys_only{false};
    bool count_only{false};
    int64_t lease_id{0};
  };

  Status KvRange(const Options& options, const Range& range, int64_t limit, std::vector<KVWithExt>& out_kvs,
                 bool& out_more, int64_t& out_count);
  Status KvPut(const Options& options, const KVPair& kv, KVWithExt& out_prev_kv);
  Status KvDeleteRange(const Options& options, const Range& range, int64_t& out_deleted,
                       std::vector<KVWithExt>& out_prev_kvs);

  Status KvCompaction(const Range& range, int64_t revision, int64_t& out_count);

  Status LeaseGrant(int64_t id, int64_t ttl, int64_t& out_id, int64_t& out_ttl);
  Status LeaseRevoke(int64_t id);
  Status LeaseRenew(int64_t id, int64_t& out_ttl);
  Status LeaseQuery(int64_t id, bool is_get_key, int64_t& out_ttl, int64_t& out_granted_ttl,
                    std::vector<std::string>& out_keys);
  Status ListLeases(std::vector<int64_t>& out_ids);

  enum EventType {
    kNone = 0,
    kPut = 1,
    kDelete = 2,
    kNotExists = 3,
  };

  struct Event {
    EventType type;
    KVWithExt kv;
    KVWithExt prev_kv;
  };

  enum class WatchType {
    kOneTime = 0,
  };

  enum class FilterType {
    kNoput = 0,
    kNodelete = 1,
  };

  struct OneTimeWatch {
    std::string key;
    int64_t start_revision;

    std::vector<FilterType> filter_types;

    bool need_prev_kv;
    bool wait_on_not_exist_key;
  };

  struct WatchParams {
    WatchType type;
    OneTimeWatch one_time_watch;
  };

  struct WatchOut {
    std::vector<Event> events;
  };

  Status Watch(const WatchParams& param, WatchOut& out);

 private:
  const ClientStub& stub_;
};

}  // namespace sdk
}  // namespace dingodb

#endif  // DINGODB_SDK_VERSION_H_