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

#include <algorithm>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/logging.h"
#include "gflags/gflags.h"
#include "glog/logging.h"
#include "sdk/client.h"
#include "sdk/document.h"
#include "sdk/status.h"
#include "sdk/types.h"
#include "sdk/utils/scoped_cleanup.h"

using namespace dingodb::sdk;

DEFINE_string(addrs, "", "coordinator addrs");
static std::shared_ptr<dingodb::sdk::Client> g_client;
static int64_t g_schema_id{2};
static int64_t g_index_id{0};
static std::string g_index_name = "example-doc-01";
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
  schema.AddColumn({"text", Type::kSTRING});
  schema.AddColumn({"i64", Type::kINT64});
  schema.AddColumn({"f64", Type::kDOUBLE});
  schema.AddColumn({"bytes", Type::kBYTES});
  schema.AddColumn({"bool", Type::kBOOL});

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

void PostClean(bool use_index_name = false) {
  Status tmp;
  if (use_index_name) {
    int64_t index_id;
    tmp = g_client->GetDocumentIndexId(g_schema_id, g_index_name, index_id);
    if (tmp.ok()) {
      CHECK_EQ(index_id, g_index_id);
      tmp = g_client->DropDocumentIndexByName(g_schema_id, g_index_name);
    }
  } else {
    tmp = g_client->DropDocumentIndexById(g_index_id);
  }
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

static void DocumentAdd(bool use_index_name = false) {
  // if PrepareDocumentIndex is sed auto start, then id is changed
  std::vector<std::string> texts_to_insert;
  texts_to_insert.push_back("Ancient empires rise and fall, shaping history's course.");                 // 3
  texts_to_insert.push_back("Artistic expressions reflect diverse cultural heritages.");                 // 5
  texts_to_insert.push_back("Social movements transform societies, forging new paths.");                 // 7
  texts_to_insert.push_back("Economies fluctuate, reflecting the complex interplay of global forces.");  // 9 of
  texts_to_insert.push_back("Strategic military campaigns alter the balance of power.");                 // 11 of
  texts_to_insert.push_back("Quantum leaps redefine understanding of physical laws.");                   // 13 of
  texts_to_insert.push_back("Chemical reactions unlock mysteries of nature.");                           // 15 of
  texts_to_insert.push_back("Philosophical debates ponder the essence of existence.");                   // 17 of
  texts_to_insert.push_back("Marriages blend traditions, celebrating love's union.");                    // 19
  texts_to_insert.push_back("Explorers discover uncharted territories, expanding world maps.");          // 21

  std::vector<int64_t> doc_ids{3, 5, 7, 9, 11, 13, 15, 17, 19, 21};

  CHECK_EQ(texts_to_insert.size(), doc_ids.size());

  std::vector<dingodb::sdk::DocWithId> docs;

  for (int i = 0; i < texts_to_insert.size(); i++) {
    auto id = doc_ids[i];

    dingodb::sdk::Document tmp_doc;
    tmp_doc.AddField("text", dingodb::sdk::DocValue::FromString(texts_to_insert[i]));
    tmp_doc.AddField("i64", dingodb::sdk::DocValue::FromInt(1000 + id));
    tmp_doc.AddField("f64", dingodb::sdk::DocValue::FromDouble(1000.0 + id));
    tmp_doc.AddField("bytes", dingodb::sdk::DocValue::FromBytes("bytes_data_" + std::to_string(id)));
    tmp_doc.AddField("bool", dingodb::sdk::DocValue::FromBool(id%3));

    dingodb::sdk::DocWithId tmp(id, std::move(tmp_doc));

    docs.push_back(std::move(tmp));
  }

  Status add;
  if (use_index_name) {
    add = g_doc_client->AddByIndexName(g_schema_id, g_index_name, docs);
  } else {
    add = g_doc_client->AddByIndexId(g_index_id, docs);
  }

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
  DINGO_LOG(INFO) << "doc add:" << add.ToString();
  DINGO_LOG(INFO) << "doc ids:" << ss.str();
}

static void DocumentSearch(bool use_index_name = false) {
  {
    dingodb::sdk::DocSearchParam param;
    param.top_n = 5;
    param.with_scalar_data = true;
    param.query_string = "discover";
    param.use_id_filter = false;

    Status tmp;
    DocSearchResult result;
    if (use_index_name) {
      tmp = g_doc_client->SearchByIndexName(g_schema_id, g_index_name, param, result);
    } else {
      tmp = g_doc_client->SearchByIndexId(g_index_id, param, result);
    }

    DINGO_LOG(INFO) << "vector search discover status: " << tmp.ToString();
    DINGO_LOG(INFO) << "vector search discover result:" << result.ToString();
    if (tmp.ok()) {
      CHECK_EQ(result.doc_sores.size(), 1);
    }
  }

  {
    dingodb::sdk::DocSearchParam param;
    param.top_n = 10;
    param.with_scalar_data = true;
    param.query_string = "of";
    param.use_id_filter = false;

    Status tmp;
    DocSearchResult result;
    if (use_index_name) {
      tmp = g_doc_client->SearchByIndexName(g_schema_id, g_index_name, param, result);
    } else {
      tmp = g_doc_client->SearchByIndexId(g_index_id, param, result);
    }

    DINGO_LOG(INFO) << "vector search of with limit 10 status: " << tmp.ToString();
    DINGO_LOG(INFO) << "vector search of with limit 10 result:" << result.ToString();
    if (tmp.ok()) {
      CHECK_EQ(result.doc_sores.size(), 5);
    }
  }

  {
    dingodb::sdk::DocSearchParam param;
    param.top_n = 3;
    param.with_scalar_data = true;
    param.query_string = "of";
    param.use_id_filter = false;

    Status tmp;
    DocSearchResult result;
    if (use_index_name) {
      tmp = g_doc_client->SearchByIndexName(g_schema_id, g_index_name, param, result);
    } else {
      tmp = g_doc_client->SearchByIndexId(g_index_id, param, result);
    }

    DINGO_LOG(INFO) << "vector search of with limit 3 status: " << tmp.ToString();
    DINGO_LOG(INFO) << "vector search of with limit 3 result:" << result.ToString();
    if (tmp.ok()) {
      CHECK_EQ(result.doc_sores.size(), 3);
    }
  }

  {
    dingodb::sdk::DocSearchParam param;
    param.top_n = 5;
    param.with_scalar_data = true;
    param.query_string = "of";
    param.use_id_filter = true;
    param.doc_ids = {
        13,
        15,
    };

    Status tmp;
    DocSearchResult result;
    if (use_index_name) {
      tmp = g_doc_client->SearchByIndexName(g_schema_id, g_index_name, param, result);
    } else {
      tmp = g_doc_client->SearchByIndexId(g_index_id, param, result);
    }

    DINGO_LOG(INFO) << "vector search of with id filter status: " << tmp.ToString();
    DINGO_LOG(INFO) << "vector search of with id filter result:" << result.ToString();
    if (tmp.ok()) {
      CHECK_EQ(result.doc_sores.size(), 2);
    }
  }

  {
    dingodb::sdk::DocSearchParam param;
    param.top_n = 5;
    param.with_scalar_data = true;
    param.query_string = R"(text:"of" AND i64: >= 1013)";
    param.use_id_filter = true;
    param.doc_ids = {
        9,
        11,
        13,
        15,
    };

    Status tmp;
    DocSearchResult result;
    if (use_index_name) {
      tmp = g_doc_client->SearchByIndexName(g_schema_id, g_index_name, param, result);
    } else {
      tmp = g_doc_client->SearchByIndexId(g_index_id, param, result);
    }

    DINGO_LOG(INFO) << "vector search of with id filter status: " << tmp.ToString();
    DINGO_LOG(INFO) << "vector search of with id filter result:" << result.ToString();
    if (tmp.ok()) {
      CHECK_EQ(result.doc_sores.size(), 2);
    }
  }

   {
    dingodb::sdk::DocSearchParam param;
    param.top_n = 5;
    param.with_scalar_data = true;
    param.query_string = R"( bool:true)";
    param.use_id_filter = true;
    param.doc_ids = {
        15,
        17,
        19,
        21
    };

    Status tmp;
    DocSearchResult result;
    if (use_index_name) {
      tmp = g_doc_client->SearchByIndexName(g_schema_id, g_index_name, param, result);
    } else {
      tmp = g_doc_client->SearchByIndexId(g_index_id, param, result);
    }

    DINGO_LOG(INFO) << "vector search of with id filter status: " << tmp.ToString();
    DINGO_LOG(INFO) << "vector search of with id filter result:" << result.ToString();
    if (tmp.ok()) {
      CHECK_EQ(result.doc_sores.size(), 2);
    }
  }

}

static void DocumentQuey(bool use_index_name = false) {
  dingodb::sdk::DocQueryParam param;
  param.doc_ids = g_doc_ids;
  param.with_scalar_data = true;
  param.selected_keys = {"text", "i64"};

  Status query;
  dingodb::sdk::DocQueryResult result;
  if (use_index_name) {
    query = g_doc_client->BatchQueryByIndexName(g_schema_id, g_index_name, param, result);
  } else {
    query = g_doc_client->BatchQueryByIndexId(g_index_id, param, result);
  }

  DINGO_LOG(INFO) << "document query: " << query.ToString();
  DINGO_LOG(INFO) << "document query result:" << result.ToString();
  CHECK_EQ(result.docs.size(), g_doc_ids.size());
}

static void DocumentGetBorder(bool use_index_name = false) {
  {
    // get max
    Status tmp;
    int64_t doc_id = 0;
    if (use_index_name) {
      tmp = g_doc_client->GetBorderByIndexName(g_schema_id, g_index_name, true, doc_id);
    } else {
      tmp = g_doc_client->GetBorderByIndexId(g_index_id, true, doc_id);
    }

    DINGO_LOG(INFO) << "document get border:" << tmp.ToString() << ", max document id:" << doc_id;
    if (tmp.ok()) {
      CHECK_EQ(doc_id, g_doc_ids[g_doc_ids.size() - 1]);
    }
  }

  {
    // get min
    Status tmp;
    int64_t doc_id = 0;
    if (use_index_name) {
      tmp = g_doc_client->GetBorderByIndexName(g_schema_id, g_index_name, false, doc_id);
    } else {
      tmp = g_doc_client->GetBorderByIndexId(g_index_id, false, doc_id);
    }

    DINGO_LOG(INFO) << "vector get border:" << tmp.ToString() << ", min vecotor id:" << doc_id;
    if (tmp.ok()) {
      CHECK_EQ(doc_id, g_doc_ids[0]);
    }
  }
}

static void DocumentScanQuery(bool use_index_name = false) {
  {
    // forward
    dingodb::sdk::DocScanQueryParam param;
    param.doc_id_start = g_doc_ids[0];
    param.doc_id_end = g_doc_ids[g_doc_ids.size() - 1];
    param.max_scan_count = 2;

    dingodb::sdk::DocScanQueryResult result;
    Status tmp;
    if (use_index_name) {
      tmp = g_doc_client->ScanQueryByIndexName(g_schema_id, g_index_name, param, result);
    } else {
      tmp = g_doc_client->ScanQueryByIndexId(g_index_id, param, result);
    }

    DINGO_LOG(INFO) << "doc forward scan query: " << tmp.ToString() << ", result:" << result.ToString();
    if (tmp.ok()) {
      CHECK_EQ(result.docs[0].id, g_doc_ids[0]);
      CHECK_EQ(result.docs[1].id, g_doc_ids[1]);
    }
  }

  {
    // backward
    dingodb::sdk::DocScanQueryParam param;
    param.doc_id_start = g_doc_ids[g_doc_ids.size() - 1];
    param.doc_id_end = g_doc_ids[0];
    param.max_scan_count = 2;
    param.is_reverse = true;

    dingodb::sdk::DocScanQueryResult result;
    Status tmp;
    if (use_index_name) {
      tmp = g_doc_client->ScanQueryByIndexName(g_schema_id, g_index_name, param, result);
    } else {
      tmp = g_doc_client->ScanQueryByIndexId(g_index_id, param, result);
    }

    DINGO_LOG(INFO) << "doc backward scan query: " << tmp.ToString() << ", result:" << result.ToString();
    if (tmp.ok()) {
      CHECK_EQ(result.docs[0].id, g_doc_ids[g_doc_ids.size() - 1]);
      CHECK_EQ(result.docs[1].id, g_doc_ids[g_doc_ids.size() - 2]);
    }
  }

  {
    // forward with selected keys
    dingodb::sdk::DocScanQueryParam param;
    param.doc_id_start = g_doc_ids[0];
    param.doc_id_end = g_doc_ids[g_doc_ids.size() - 1] + 10;
    param.with_scalar_data = true;
    param.selected_keys = {"text", "i64"};
    param.max_scan_count = 100;

    dingodb::sdk::DocScanQueryResult result;
    Status tmp;
    if (use_index_name) {
      tmp = g_doc_client->ScanQueryByIndexName(g_schema_id, g_index_name, param, result);
    } else {
      tmp = g_doc_client->ScanQueryByIndexId(g_index_id, param, result);
    }

    DINGO_LOG(INFO) << "doc forward scan query with filter:" << tmp.ToString() << ", result:" << result.ToString();
    if (tmp.ok()) {
      CHECK_EQ(result.docs.size(), 10);
    }
  }
}

static void DocumentGetIndexMetrics(bool use_index_name = false) {
  Status tmp;
  dingodb::sdk::DocIndexMetricsResult result;
  if (use_index_name) {
    tmp = g_doc_client->GetIndexMetricsByIndexName(g_schema_id, g_index_name, result);
  } else {
    tmp = g_doc_client->GetIndexMetricsByIndexId(g_index_id, result);
  }

  DINGO_LOG(INFO) << "doc get index metrics:" << tmp.ToString() << ", result :" << result.ToString();
  if (tmp.ok()) {
    CHECK_EQ(result.total_num_docs, g_doc_ids.size());
    CHECK_EQ(result.max_doc_id, g_doc_ids[g_doc_ids.size() - 1]);
    CHECK_EQ(result.min_doc_id, g_doc_ids[0]);
  }
}

static void DocumentCount(bool use_index_name = false) {
  {
    Status tmp;
    int64_t result{0};
    if (use_index_name) {
      tmp = g_doc_client->CountByIndexName(g_schema_id, g_index_name, 0, g_doc_ids[g_doc_ids.size() - 1] + 1, result);
    } else {
      tmp = g_doc_client->CountByIndexId(g_index_id, 0, g_doc_ids[g_doc_ids.size() - 1] + 1, result);
    }

    DINGO_LOG(INFO) << "doc count:" << tmp.ToString() << ", result :" << result;
    if (tmp.ok()) {
      CHECK_EQ(result, g_doc_ids.size());
    }
  }

  {
    Status tmp;
    int64_t result{0};
    int64_t start_doc_id = g_doc_ids[g_doc_ids.size() - 1] + 1;
    int64_t end_doc_id = start_doc_id + 1;
    if (use_index_name) {
      tmp = g_doc_client->CountByIndexName(g_schema_id, g_index_name, start_doc_id, end_doc_id, result);
    } else {
      tmp = g_doc_client->CountByIndexId(g_index_id, start_doc_id, end_doc_id, result);
    }

    DINGO_LOG(INFO) << "doc count:" << tmp.ToString() << ", result :" << result;
    if (tmp.ok()) {
      CHECK_EQ(result, 0);
    }
  }

  {
    Status tmp;
    int64_t result{0};
    if (use_index_name) {
      tmp = g_doc_client->CountByIndexName(g_schema_id, g_index_name, g_doc_ids[0], g_doc_ids[g_doc_ids.size() - 1],
                                           result);
    } else {
      tmp = g_doc_client->CountByIndexId(g_index_id, g_doc_ids[0], g_doc_ids[g_doc_ids.size() - 1], result);
    }

    DINGO_LOG(INFO) << "vector count:" << tmp.ToString() << ", result :" << result;
    if (tmp.ok()) {
      CHECK_EQ(result, g_doc_ids.size() - 1);
    }
  }
}

static void DocumentDelete(bool use_index_name = false) {
  Status tmp;
  std::vector<dingodb::sdk::DocDeleteResult> result;
  if (use_index_name) {
    tmp = g_doc_client->DeleteByIndexName(g_schema_id, g_index_name, g_doc_ids, result);
  } else {
    tmp = g_doc_client->DeleteByIndexId(g_index_id, g_doc_ids, result);
  }
  DINGO_LOG(INFO) << "doc delete status: " << tmp.ToString();
  for (const auto& r : result) {
    DINGO_LOG(INFO) << "doc delete result:" << r.ToString();
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

  {
    PrepareDocumentIndex();
    PrepareDocumentClient();

    DocumentAdd();
    DocumentSearch();
    DocumentQuey();
    DocumentGetBorder();
    DocumentScanQuery();
    DocumentGetIndexMetrics();
    DocumentCount();
    DocumentDelete();

    PostClean();
  }

  {
    PrepareDocumentIndex();
    PrepareDocumentClient();

    DocumentAdd(true);
    DocumentSearch(true);
    DocumentQuey(true);
    DocumentGetBorder(true);
    DocumentScanQuery(true);
    DocumentGetIndexMetrics(true);
    DocumentCount(true);
    DocumentDelete(true);

    PostClean(true);
  }

  {
    int64_t start_id = 1;

    PrepareDocumentIndex(start_id);

    PrepareDocumentClient();
    DocumentAdd();

    std::vector<int64_t> target_ids = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
    // sort vecotor_ids and target_ids, and check equal
    std::sort(g_doc_ids.begin(), g_doc_ids.end());
    CHECK(std::equal(g_doc_ids.begin(), g_doc_ids.end(), target_ids.begin(), target_ids.end()));

    PostClean(true);
  }
}
