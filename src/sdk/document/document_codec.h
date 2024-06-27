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

#ifndef DINGODB_SDK_DOCUMENT_CODEC_H_
#define DINGODB_SDK_DOCUMENT_CODEC_H_

#include <optional>

#include "common/logging.h"
#include "glog/logging.h"
#include "sdk/utils/codec.h"
#include "serial/buf.h"
#include "serial/schema/long_schema.h"

namespace dingodb {
namespace sdk {

static const char kDocumentPrefix = 'r';
static const uint32_t kDocumentKeyMinLenWithPrefix = 9;
static const uint32_t kDocumentKeyMaxLenWithPrefix = 17;

namespace document_codec {

static void EncodeDocumentKey(char prefix, int64_t partition_id, std::string& result) {
  CHECK(prefix != 0) << "Encode Document key failed, prefix is 0, partition_id:[" << partition_id << "]";

  // Buf buf(17);
  Buf buf(kDocumentKeyMinLenWithPrefix);
  buf.Write(prefix);
  buf.WriteLong(partition_id);
  buf.GetBytes(result);
}

static void EncodeDocumentKey(char prefix, int64_t partition_id, int64_t doc_id, std::string& result) {
  CHECK(prefix != 0) << "Encode Document key failed, prefix is 0, partition_id:[" << partition_id << "], doc_id:["
                     << doc_id << "]";

  // Buf buf(17);
  Buf buf(kDocumentKeyMaxLenWithPrefix);
  buf.Write(prefix);
  buf.WriteLong(partition_id);
  DingoSchema<std::optional<int64_t>>::InternalEncodeKey(&buf, doc_id);
  buf.GetBytes(result);
}

static int64_t DecodeDocumentId(const std::string& value) {
  Buf buf(value);
  if (value.size() >= kDocumentKeyMaxLenWithPrefix) {
    buf.Skip(9);
  } else if (value.size() == kDocumentKeyMinLenWithPrefix) {
    return 0;
  } else {
    DINGO_LOG(FATAL) << "Decode Document id failed, value size is not 9 or >=17, value:["
                     << codec::BytesToHexString(value) << "]";
    return 0;
  }

  return DingoSchema<std::optional<int64_t>>::InternalDecodeKey(&buf);
}

static int64_t DecodePartitionId(const std::string& value) {
  Buf buf(value);

  // if (value.size() >= 17 || value.size() == 9) {
  if (value.size() >= kDocumentKeyMaxLenWithPrefix || value.size() == kDocumentKeyMinLenWithPrefix) {
    buf.Skip(1);
  }

  return buf.ReadLong();
}

}  // namespace document_codec
}  // namespace sdk
}  // namespace dingodb
#endif  // DINGODB_SDK_DOCUMENT_CODEC_H_