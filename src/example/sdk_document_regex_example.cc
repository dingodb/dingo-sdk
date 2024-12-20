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

#include <unistd.h>

#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/logging.h"
#include "gflags/gflags.h"
#include "glog/logging.h"
#include "dingosdk/client.h"
#include "dingosdk/document.h"
#include "dingosdk/status.h"
#include "dingosdk/types.h"
#include "sdk/utils/scoped_cleanup.h"

using namespace dingodb::sdk;

DEFINE_string(addrs, "", "coordinator addrs");
static std::shared_ptr<dingodb::sdk::Client> g_client;
static int64_t g_schema_id{2};
static int64_t g_index_id{0};
static std::string g_index_name = "example-doc-regex";
static std::vector<int64_t> g_range_partition_seperator_ids{5, 10, 20};
static std::vector<int64_t> g_doc_ids;
static dingodb::sdk::DocumentClient* g_doc_client;

static void PrepareDocumentIndex(int64_t start_id = 0) {
  dingodb::sdk::DocumentIndexCreator* creator;
  Status built = g_client->NewDocumentIndexCreator(&creator);
  CHECK(built.IsOK()) << "dingo creator build fail:" << built.ToString();
  CHECK_NOTNULL(creator);
  SCOPED_CLEANUP({ delete creator; });

  dingodb::sdk::DocumentSchema schema;
  schema.AddColumn({"title", Type::kSTRING});
  schema.AddColumn({"text", Type::kSTRING});

  creator->SetSchemaId(g_schema_id)
      .SetName(g_index_name)
      .SetReplicaNum(3)
      .SetRangePartitions(g_range_partition_seperator_ids)
      .SetSchema(schema);

  if (start_id > 0) {
    creator->SetAutoIncrementStart(start_id);
  }

  Status create = creator->Create(g_index_id);
  DINGO_LOG(INFO) << "Create doc index status: " << create.ToString() << ", doc_index_id:" << g_index_id;
  sleep(30);
}

void PostClean() {
  Status tmp = g_client->DropDocumentIndexById(g_index_id);
  DINGO_LOG(INFO) << "drop index status: " << tmp.ToString() << ", index_id:" << g_index_id;
  delete g_doc_client;
  g_doc_ids.clear();
}

static void PrepareDocumentClient() {
  dingodb::sdk::DocumentClient* client;
  Status built = g_client->NewDocumentClient(&client);
  CHECK(built.IsOK()) << "dingo vector client build fail:" << built.ToString();
  CHECK_NOTNULL(client);
  g_doc_client = client;
  CHECK_NOTNULL(g_doc_client);
}

static void DocumentAdd() {
  std::vector<dingodb::sdk::DocWithId> docs;

  {
    dingodb::sdk::Document tmp_doc;
    tmp_doc.AddField("title", dingodb::sdk::DocValue::FromString("a"));
    tmp_doc.AddField("text", dingodb::sdk::DocValue::FromString("The Diary of Muadib"));

    dingodb::sdk::DocWithId tmp(1, std::move(tmp_doc));

    docs.push_back(std::move(tmp));
  }

  {
    dingodb::sdk::Document tmp_doc;
    tmp_doc.AddField("title", dingodb::sdk::DocValue::FromString("bb"));
    tmp_doc.AddField("text", dingodb::sdk::DocValue::FromString("A Dairy Cow"));

    dingodb::sdk::DocWithId tmp(2, std::move(tmp_doc));

    docs.push_back(std::move(tmp));
  }

  {
    dingodb::sdk::Document tmp_doc;
    tmp_doc.AddField("title", dingodb::sdk::DocValue::FromString("ccc"));
    tmp_doc.AddField("text", dingodb::sdk::DocValue::FromString("The Diary of a Young Girl"));

    dingodb::sdk::DocWithId tmp(3, std::move(tmp_doc));

    docs.push_back(std::move(tmp));
  }

  Status add = g_doc_client->AddByIndexId(g_index_id, docs);

  for (auto& doc : docs) {
    g_doc_ids.push_back(doc.id);
  }

  std::stringstream ss;
  ss << '[';
  for (auto& id : g_doc_ids) {
    ss << std::to_string(id);
    if (id != g_doc_ids.back()) {
      ss << ", ";
    }
  }
  ss << ']';

  DINGO_LOG(INFO) << "Add doc status:" << add.ToString() << ", doc_ids:" << ss.str();
}

static void DocumentRegexSearch() {
  {
    dingodb::sdk::DocSearchParam param;
    param.top_n = 5;
    param.with_scalar_data = true;

    //  base64encode Dia.* to RGlhLioq
    // text contains "Dia"
    param.query_string = "text:RE [RGlhLio=]";
    param.use_id_filter = false;

    DocSearchResult result;
    Status tmp = g_doc_client->SearchByIndexId(g_index_id, param, result);

    DINGO_LOG(INFO) << "vector regex search length status: " << tmp.ToString();
    DINGO_LOG(INFO) << "vector regex search length result:" << result.ToString();
    if (tmp.ok()) {
      CHECK_EQ(result.doc_sores.size(), 2);
    }
  }
}

static void DocumentSearchLength() {
  {
    dingodb::sdk::DocSearchParam param;
    param.top_n = 5;
    param.with_scalar_data = true;

    //  base64encode (.{0,2})  to KC57MCwyfSk=
    // title length <= 2
    param.query_string = "title:RE [KC57MCwyfSk=]";
    param.use_id_filter = false;

    DocSearchResult result;
    Status tmp = g_doc_client->SearchByIndexId(g_index_id, param, result);

    DINGO_LOG(INFO) << "vector regex search length status: " << tmp.ToString();
    DINGO_LOG(INFO) << "vector regex search length result:" << result.ToString();
    if (tmp.ok()) {
      CHECK_EQ(result.doc_sores.size(), 2);
    }
  }
}

static void DocumentSearchAnd() {
  {
    dingodb::sdk::DocSearchParam param;
    param.top_n = 5;
    param.with_scalar_data = true;

    //  base64encode (.{0,2})  to KC57MCwyfSk=
    //  base64encode Dia.* to RGlhLioq
    // title length <= 2 and text contains "Dia"
    param.query_string = "title:RE [KC57MCwyfSk=] AND text:RE [RGlhLio=]";
    param.use_id_filter = false;

    DocSearchResult result;
    Status tmp = g_doc_client->SearchByIndexId(g_index_id, param, result);

    DINGO_LOG(INFO) << "vector regex search and status: " << tmp.ToString();
    DINGO_LOG(INFO) << "vector regex search and result:" << result.ToString();
    if (tmp.ok()) {
      CHECK_EQ(result.doc_sores.size(), 1);
    }
  }
}

int main(int argc, char* argv[]) {
  FLAGS_minloglevel = google::GLOG_INFO;
  FLAGS_logtostdout = true;
  FLAGS_colorlogtostdout = true;
  FLAGS_logbufsecs = 0;
  // FLAGS_v = dingodb::kGlobalValueOfDebug;

  google::InitGoogleLogging(argv[0]);
  google::ParseCommandLineFlags(&argc, &argv, true);

  if (FLAGS_addrs.empty()) {
    DINGO_LOG(WARNING) << "coordinator addrs is empty, try to use addr like: "
                          "127.0.0.1:22001,127.0.0.1:22002,127.0.0.1:22003";
    FLAGS_addrs = "127.0.0.1:22001,127.0.0.1:22002,127.0.0.1:22003";
  }

  CHECK(!FLAGS_addrs.empty());

  dingodb::sdk::Client* tmp;
  Status built = dingodb::sdk::Client::BuildFromAddrs(FLAGS_addrs, &tmp);
  if (!built.ok()) {
    DINGO_LOG(ERROR) << "Fail to build client, please check parameter --addrs=" << FLAGS_addrs
                     << " error: " << built.ToString();
    return -1;
  }
  CHECK_NOTNULL(tmp);
  g_client.reset(tmp);

  PrepareDocumentIndex();
  PrepareDocumentClient();

  DocumentAdd();
  DocumentRegexSearch();
  DocumentSearchLength();
  DocumentSearchAnd();

  PostClean();
}
