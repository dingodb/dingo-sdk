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

#ifndef DINGODB_SDK_GRPC_VERSION_RPC_H_
#define DINGODB_SDK_GRPC_VERSION_RPC_H_

#include "proto/version.grpc.pb.h"
#include "sdk/rpc/grpc/unary_rpc.h"

namespace dingodb {
namespace sdk {
namespace version {

#define DECLARE_VERSION_RPC(METHOD) DECLARE_UNARY_RPC(pb::version, VersionService, METHOD)

DECLARE_UNARY_RPC_INNER(pb::version, VersionService, KvRange, Range);
DECLARE_UNARY_RPC_INNER(pb::version, VersionService, KvPut, Put);
DECLARE_UNARY_RPC_INNER(pb::version, VersionService, KvDeleteRange, DeleteRange);
DECLARE_UNARY_RPC_INNER(pb::version, VersionService, KvCompaction, Compaction);

DECLARE_VERSION_RPC(LeaseGrant);
DECLARE_VERSION_RPC(LeaseRevoke);
DECLARE_VERSION_RPC(LeaseRenew);
DECLARE_VERSION_RPC(LeaseQuery);
DECLARE_VERSION_RPC(ListLeases);

DECLARE_VERSION_RPC(Watch);

}  // namespace version
}  // namespace sdk
}  // namespace dingodb

#endif  // DINGODB_SDK_GRPC_VERSION_RPC_H_