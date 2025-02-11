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

#include "sdk/rpc/brpc/version_rpc.h"

namespace dingodb {
namespace sdk {
namespace version {

#define DEFINE_VERSION_RPC(METHOD) DEFINE_UNAEY_RPC(pb::version, VersionService, METHOD)

DEFINE_VERSION_RPC(KvRange);
DEFINE_VERSION_RPC(KvPut);
DEFINE_VERSION_RPC(KvDeleteRange);
DEFINE_VERSION_RPC(KvCompaction);

DEFINE_VERSION_RPC(LeaseGrant);
DEFINE_VERSION_RPC(LeaseRevoke);
DEFINE_VERSION_RPC(LeaseRenew);
DEFINE_VERSION_RPC(LeaseQuery);
DEFINE_VERSION_RPC(ListLeases);

DEFINE_VERSION_RPC(Watch);

}  // namespace version
}  // namespace sdk
}  // namespace dingodb