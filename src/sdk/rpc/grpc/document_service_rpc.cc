
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

#include "sdk/rpc/grpc/document_service_rpc.h"

namespace dingodb {
namespace sdk {

#define DEFINE_DOCUMENT_SERVICE_RPC(METHOD) DEFINE_UNAEY_RPC(pb::document, DocumentService, METHOD)

namespace document {
DEFINE_DOCUMENT_SERVICE_RPC(Hello);
}

DEFINE_DOCUMENT_SERVICE_RPC(DocumentAdd);
DEFINE_DOCUMENT_SERVICE_RPC(DocumentBatchQuery);
DEFINE_DOCUMENT_SERVICE_RPC(DocumentSearch);
DEFINE_DOCUMENT_SERVICE_RPC(DocumentDelete);
DEFINE_DOCUMENT_SERVICE_RPC(DocumentGetBorderId);
DEFINE_DOCUMENT_SERVICE_RPC(DocumentScanQuery);
DEFINE_DOCUMENT_SERVICE_RPC(DocumentGetRegionMetrics);
DEFINE_DOCUMENT_SERVICE_RPC(DocumentCount);

}  // namespace sdk
}  // namespace dingodb