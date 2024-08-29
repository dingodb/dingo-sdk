#include <unistd.h>

#include <cstdint>
#include <iostream>
#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "common/logging.h"
#include "gflags/gflags.h"
#include "glog/logging.h"
#include "sdk/client.h"
#include "sdk/status.h"
#include "sdk/types.h"
#include "sdk/utils/scoped_cleanup.h"
#include "sdk/vector.h"

using namespace dingodb::sdk;

DEFINE_string(addrs, "", "coordinator addrs");
static std::shared_ptr<dingodb::sdk::Client> g_client;
static int64_t g_schema_id{2};
static int64_t g_index_id{0};
static std::string g_index_name = "example01";
static std::vector<int64_t> g_range_partition_seperator_ids{5, 15, 30};
static int32_t g_dimension = 2;
static dingodb::sdk::DiskAnnParam g_diskann_param(g_dimension, dingodb::sdk::MetricType::kL2,
                                                  dingodb::sdk::ValueType::kFloat);
static int64_t g_start_id = 1;
static std::vector<int64_t> g_vector_ids;
static dingodb::sdk::VectorClient* g_vector_client;

static const dingodb::sdk::Type kDefaultType = dingodb::sdk::Type::kINT64;
static std::vector<std::string> g_scalar_col{"id", "fake_id"};
static std::vector<dingodb::sdk::Type> g_scalar_col_typ{kDefaultType, dingodb::sdk::Type::kDOUBLE};
static std::set<int64_t> g_region_ids;

static int add_times = 0;
static int vector_count = 300;

void PostClean(bool use_index_name = false) {
  Status tmp;
  if (use_index_name) {
    int64_t index_id;
    tmp = g_client->GetVectorIndexId(g_schema_id, g_index_name, index_id);
    if (tmp.ok()) {
      CHECK_EQ(index_id, g_index_id);
      tmp = g_client->DropVectorIndexByName(g_schema_id, g_index_name);
    }
  } else {
    tmp = g_client->DropVectorIndexById(g_index_id);
  }
  DINGO_LOG(INFO) << "drop index status: " << tmp.ToString() << ", index_id:" << g_index_id;
  delete g_vector_client;
  g_vector_ids.clear();
  g_region_ids.clear();
}

static void PrepareVectorIndex() {
  dingodb::sdk::VectorIndexCreator* creator;
  Status built = g_client->NewVectorIndexCreator(&creator);
  CHECK(built.IsOK()) << "dingo creator build fail:" << built.ToString();
  CHECK_NOTNULL(creator);
  SCOPED_CLEANUP({ delete creator; });

  dingodb::sdk::VectorScalarSchema schema;
  // NOTE: may be add more
  schema.cols.push_back({g_scalar_col[0], g_scalar_col_typ[0], true});
  schema.cols.push_back({g_scalar_col[1], g_scalar_col_typ[1], true});
  Status create = creator->SetSchemaId(g_schema_id)
                      .SetName(g_index_name)
                      .SetReplicaNum(3)
                      .SetRangePartitions(g_range_partition_seperator_ids)
                      .SetDiskAnnParam(g_diskann_param)
                      .SetScalarSchema(schema)
                      .Create(g_index_id);
  DINGO_LOG(INFO) << "Create index status: " << create.ToString() << ", index_id:" << g_index_id;
  sleep(3);
}

static void PrepareVectorClient() {
  dingodb::sdk::VectorClient* client;
  Status built = g_client->NewVectorClient(&client);
  CHECK(built.IsOK()) << "dingo vector client build fail:" << built.ToString();
  CHECK_NOTNULL(client);
  g_vector_client = client;
  CHECK_NOTNULL(g_vector_client);
}

static void VectorAdd(bool use_index_name = false) {
  std::vector<dingodb::sdk::VectorWithId> vectors;

  float delta = 0.1;
  for (int i = 1; i < vector_count; i++) {
    dingodb::sdk::Vector tmp_vector{dingodb::sdk::ValueType::kFloat, g_dimension};
    tmp_vector.float_values.push_back(1.0 + delta);
    tmp_vector.float_values.push_back(2.0 + delta);
    dingodb::sdk::VectorWithId tmp(i, std::move(tmp_vector));
    {
      {
        dingodb::sdk::ScalarValue scalar_value;
        scalar_value.type = kDefaultType;

        dingodb::sdk::ScalarField field;
        field.long_data = i;
        scalar_value.fields.push_back(field);

        tmp.scalar_data.insert(std::make_pair(g_scalar_col[0], scalar_value));
      }
      {
        dingodb::sdk::ScalarValue scalar_value;
        scalar_value.type = dingodb::sdk::kDOUBLE;

        dingodb::sdk::ScalarField field;
        field.double_data = i;
        scalar_value.fields.push_back(field);

        tmp.scalar_data.insert(std::make_pair(g_scalar_col[1], scalar_value));
      }
    }

    vectors.push_back(std::move(tmp));

    g_vector_ids.push_back(i);
    delta++;
  }
  Status add;
  if (use_index_name) {
    add = g_vector_client->AddByIndexName(g_schema_id, g_index_name, vectors, false, false);
  } else {
    add = g_vector_client->AddByIndexId(g_index_id, vectors, false, false);
  }
  add_times++;
}

static void VectorSearch(bool use_index_name = false) {
  std::vector<dingodb::sdk::VectorWithId> target_vectors;
  float init = 0.1f;
  for (int i = 0; i < 10; i++) {
    dingodb::sdk::Vector tmp_vector{dingodb::sdk::ValueType::kFloat, g_dimension};
    tmp_vector.float_values.clear();
    tmp_vector.float_values.push_back(init);
    tmp_vector.float_values.push_back(init);

    dingodb::sdk::VectorWithId tmp;
    tmp.vector = std::move(tmp_vector);
    target_vectors.push_back(std::move(tmp));

    init = init + 0.1;
  }

  dingodb::sdk::SearchParam param;
  param.topk = 1;
  param.with_scalar_data = true;
  // param.use_brute_force = true;
  param.extra_params.insert(std::make_pair(dingodb::sdk::kParallelOnQueries, 10));

  Status tmp;
  std::vector<dingodb::sdk::SearchResult> result;
  if (use_index_name) {
    tmp = g_vector_client->SearchByIndexName(g_schema_id, g_index_name, param, target_vectors, result);
  } else {
    tmp = g_vector_client->SearchByIndexId(g_index_id, param, target_vectors, result);
  }

  DINGO_LOG(INFO) << "vector search status: " << tmp.ToString();
  if (tmp.ok()) {
    for (const auto& r : result) {
      DINGO_LOG(INFO) << "vector search result: " << r.ToString();
    }

    CHECK_EQ(result.size(), target_vectors.size());
    for (auto i = 0; i < result.size(); i++) {
      auto& search_result = result[i];
      if (!search_result.vector_datas.empty()) {
        CHECK_EQ(search_result.vector_datas.size(), param.topk);
      }
      const auto& vector_id = search_result.id;
      CHECK_EQ(vector_id.id, target_vectors[i].id);
      CHECK_EQ(vector_id.vector.Size(), target_vectors[i].vector.Size());
    }
  }
}

static void VectorDelete(bool use_index_name = false) {
  Status tmp;
  std::vector<dingodb::sdk::DeleteResult> result;
  if (use_index_name) {
    tmp = g_vector_client->DeleteByIndexName(g_schema_id, g_index_name, g_vector_ids, result);
  } else {
    tmp = g_vector_client->DeleteByIndexId(g_index_id, g_vector_ids, result);
  }
  DINGO_LOG(INFO) << "vector delete status: " << tmp.ToString();
  for (const auto& r : result) {
    DINGO_LOG(INFO) << "vector delete result:" << r.ToString();
  }
}

static void VectorStatusByIndex(bool use_index_name = false) {
  StateResult result;
  Status statu;
  if (use_index_name) {
    statu = g_vector_client->StatusByIndexName(g_schema_id, g_index_name, result);
  } else {
    statu = g_vector_client->StatusByIndexId(g_index_id, result);
  }

  if (statu.ok()) {
    for (auto& result : result.region_states) {
      // 收集region id方便后续region操作
      g_region_ids.insert(result.region_id);
      if (result.status.ok()) {
        DINGO_LOG(INFO) << "region_id : " << result.region_id << " state : " << RegionStateToString(result.state);
      } else {
        DINGO_LOG(INFO) << "region_id : " << result.region_id << " status : " << result.status.ToString();
      }
    }
  }
  DINGO_LOG(INFO) << "diskann status:" << statu.ToString();
}

static void VectorStatusByRegion(bool use_index_name = false) {
  StateResult result;
  Status statu;
  std::vector<int64_t> region;
  if (!g_region_ids.empty()) {
    region.push_back(*g_region_ids.begin());
  }
  if (use_index_name) {
    statu = g_vector_client->StatusByRegionIdIndexName(g_schema_id, g_index_name, region, result);
  } else {
    statu = g_vector_client->StatusByRegionId(g_index_id, region, result);
  }

  if (statu.ok()) {
    DINGO_LOG(INFO) << "vector status " << result.ToString();
  }
  DINGO_LOG(INFO) << "id : " << region[0] << " diskann status:" << statu.ToString();
}

static void VectorResetByIndex(bool use_index_name = false) {
  ErrStatusResult result;
  Status reset;
  if (use_index_name) {
    reset = g_vector_client->ResetByIndexName(g_schema_id, g_index_name, result);
  } else {
    reset = g_vector_client->ResetByIndexId(g_index_id, result);
  }

  if (reset.IsResetFailed()) {
    DINGO_LOG(INFO) << "vector reset: " << result.ToString();
  }
  DINGO_LOG(INFO) << "vector reset: " << reset.ToString();
}

static void VectorResetByRegion(bool use_index_name = false) {
  ErrStatusResult result;
  Status reset;
  std::vector<int64_t> region;
  if (!g_region_ids.empty()) {
    region.push_back(*g_region_ids.begin());
  }
  if (use_index_name) {
    reset = g_vector_client->ResetByRegionIdIndexName(g_schema_id, g_index_name, region, result);
  } else {
    reset = g_vector_client->ResetByRegionId(g_index_id, region, result);
  }

  if (reset.IsResetFailed()) {
    DINGO_LOG(INFO) << "vector reset: " << result.ToString();
  }
  DINGO_LOG(INFO) << "id : " << region[0] << " vector reset:" << reset.ToString();
}

static void VectorBuildByIndex(bool use_index_name = false) {
  ErrStatusResult result;
  Status build;
  if (use_index_name) {
    build = g_vector_client->BuildByIndexName(g_schema_id, g_index_name, result);
  } else {
    build = g_vector_client->BuildByIndexId(g_index_id, result);
  }

  if (build.IsBuildFailed()) {
    DINGO_LOG(INFO) << "vector build " << result.ToString();
  }
  DINGO_LOG(INFO) << "vector build:" << build.ToString();
}

static void VectorBuildByRegion(bool use_index_name = false) {
  ErrStatusResult result;
  Status build;
  std::vector<int64_t> region;
  if (!g_region_ids.empty()) {
    region.push_back(*g_region_ids.begin());
  }
  CHECK(!region.empty()) << "empty";
  if (use_index_name) {
    build = g_vector_client->BuildByRegionIdIndexName(g_schema_id, g_index_name, region, result);
  } else {
    build = g_vector_client->BuildByRegionId(g_index_id, region, result);
  }

  if (build.IsBuildFailed()) {
    DINGO_LOG(INFO) << "vector build " << result.ToString();
  }
  DINGO_LOG(INFO) << "region id: " << region[0] << " vector build:" << build.ToString();
}

static void VectorLoadByIndex(bool use_index_name = false) {
  ErrStatusResult result;
  Status load;
  if (use_index_name) {
    load = g_vector_client->LoadByIndexName(g_schema_id, g_index_name, result);
  } else {
    load = g_vector_client->LoadByIndexId(g_index_id, result);
  }

  if (load.IsLoadFailed()) {
    DINGO_LOG(INFO) << "vector load failed: " << result.ToString();
  }
  DINGO_LOG(INFO) << "vector load:" << load.ToString();
}

static void VectorLoadByRegion(bool use_index_name = false) {
  ErrStatusResult result;
  Status load;
  std::vector<int64_t> region;
  if (!g_region_ids.empty()) {
    region.push_back(*g_region_ids.begin());
  }
  if (use_index_name) {
    load = g_vector_client->LoadByRegionIdIndexName(g_schema_id, g_index_name, region, result);
  } else {
    load = g_vector_client->LoadByRegionId(g_index_id, region, result);
  }

  if (load.IsLoadFailed()) {
    DINGO_LOG(INFO) << "vector load " << result.ToString();
  }
  DINGO_LOG(INFO) << "id : " << region[0] << " vector load:" << load.ToString();
}

static void VectorCountMemory(bool use_index_name = false) {
  {
    Status tmp;
    int64_t result{0};
    if (use_index_name) {
      tmp = g_vector_client->CountMemoryByIndexName(g_schema_id, g_index_name, result);
    } else {
      tmp = g_vector_client->CountMemoryByIndexId(g_index_id, result);
    }

    DINGO_LOG(INFO) << "vector count : " << tmp.ToString() << " , result :" << result;
  }
}

static void VectorDump(bool use_index_name = false) {
  Status dump;
  if (use_index_name) {
    dump = g_vector_client->DumpByIndexName(g_schema_id, g_index_name);
  } else {
    dump = g_vector_client->DumpByIndexId(g_index_id);
  }
  DINGO_LOG(INFO) << "vector dump:" << dump.ToString();
}

static void CheckBuildedByIndex(bool use_index_name = false) {
  bool tag = true;
  while (tag) {
    tag = false;
    StateResult result;
    Status statu;
    if (use_index_name) {
      statu = g_vector_client->StatusByIndexName(g_schema_id, g_index_name, result);
    } else {
      statu = g_vector_client->StatusByIndexId(g_index_id, result);
    }

    if (statu.ok()) {
      for (auto& result : result.region_states) {
        if (result.status.ok()) {
          DINGO_LOG(INFO) << "region_id : " << result.region_id << " state : " << RegionStateToString(result.state);
          if (result.state != kBuilded) {
            tag = true;
            sleep(1);
            break;
          }
        } else {
          DINGO_LOG(INFO) << "region_id : " << result.region_id << " status : " << result.status.ToString();
          sleep(1);
          tag = true;
          break;
        }
      }
    }
  }
}

static void CheckLoadedByIndex(bool use_index_name = false) {
  bool tag = true;
  while (tag) {
    tag = false;
    StateResult result;
    Status statu;
    if (use_index_name) {
      statu = g_vector_client->StatusByIndexName(g_schema_id, g_index_name, result);
    } else {
      statu = g_vector_client->StatusByIndexId(g_index_id, result);
    }

    if (statu.ok()) {
      for (auto& result : result.region_states) {
        if (result.status.ok()) {
          DINGO_LOG(INFO) << "region_id : " << result.region_id << " state : " << RegionStateToString(result.state);
          if (result.state != kLoaded) {
            tag = true;
            sleep(1);
            break;
          }
        } else {
          DINGO_LOG(INFO) << "region_id : " << result.region_id << " status : " << result.status.ToString();
          tag = true;
          sleep(1);
          break;
        }
      }
    }
  }
}

static void PreCheckResetByIndex(bool use_index_name = false) {
  bool tag = true;
  while (tag) {
    tag = false;
    StateResult result;
    Status statu;
    if (use_index_name) {
      statu = g_vector_client->StatusByIndexName(g_schema_id, g_index_name, result);
    } else {
      statu = g_vector_client->StatusByIndexId(g_index_id, result);
    }

    if (statu.ok()) {
      for (auto& result : result.region_states) {
        if (result.status.ok()) {
          DINGO_LOG(INFO) << "region_id : " << result.region_id << " state : " << RegionStateToString(result.state);
          if (result.state == kBuilding || result.state == kLoading) {
            tag = true;
            sleep(1);
            break;
          }
        } else {
          DINGO_LOG(INFO) << "region_id : " << result.region_id << " status : " << result.status.ToString();
          sleep(1);
          tag = true;
          break;
        }
      }
    }
  }
}

static void CheckBuildedByRegion(bool use_index_name = false) {
  bool tag = true;
  while (tag) {
    tag = false;
    StateResult result;
    Status statu;
    std::vector<int64_t> region;
    if (!g_region_ids.empty()) {
      region.push_back(*g_region_ids.begin());
    }
    if (use_index_name) {
      statu = g_vector_client->StatusByRegionIdIndexName(g_schema_id, g_index_name, region, result);
    } else {
      statu = g_vector_client->StatusByRegionId(g_index_id, region, result);
    }
    if (statu.ok()) {
      for (auto& result : result.region_states) {
        if (result.status.ok()) {
          DINGO_LOG(INFO) << "region_id : " << result.region_id << " state : " << RegionStateToString(result.state);
          if (result.state != kBuilded) {
            tag = true;
            sleep(1);
            break;
          }
        } else {
          DINGO_LOG(INFO) << "region_id : " << result.region_id << " status : " << result.status.ToString();
          sleep(1);
          tag = true;
          break;
        }
      }
    }
  }
}

static void CheckLoadedByRegion(bool use_index_name = false) {
  bool tag = true;
  while (tag) {
    tag = false;
    StateResult result;
    Status statu;
    std::vector<int64_t> region;
    if (!g_region_ids.empty()) {
      region.push_back(*g_region_ids.begin());
    }
    if (use_index_name) {
      statu = g_vector_client->StatusByRegionIdIndexName(g_schema_id, g_index_name, region, result);
    } else {
      statu = g_vector_client->StatusByRegionId(g_index_id, region, result);
    }

    if (statu.ok()) {
      for (auto& result : result.region_states) {
        if (result.status.ok()) {
          DINGO_LOG(INFO) << "region_id : " << result.region_id << " state : " << RegionStateToString(result.state);
          if (result.state != kLoaded) {
            tag = true;
            sleep(1);
            break;
          }
        } else {
          DINGO_LOG(INFO) << "region_id : " << result.region_id << " status : " << result.status.ToString();
          tag = true;
          sleep(1);
          break;
        }
      }
    }
  }
}

static void PreCheckResetByRegion(bool use_index_name = false) {
  bool tag = true;
  while (tag) {
    tag = false;
    StateResult result;
    Status statu;
    std::vector<int64_t> region;
    if (!g_region_ids.empty()) {
      region.push_back(*g_region_ids.begin());
    }
    if (use_index_name) {
      statu = g_vector_client->StatusByRegionIdIndexName(g_schema_id, g_index_name, region, result);
    } else {
      statu = g_vector_client->StatusByRegionId(g_index_id, region, result);
    }

    if (statu.ok()) {
      for (auto& result : result.region_states) {
        if (result.status.ok()) {
          DINGO_LOG(INFO) << "region_id : " << result.region_id << " state : " << RegionStateToString(result.state);
          if (result.state == kLoading || result.state == kBuilding) {
            tag = true;
            sleep(1);
            break;
          }
        } else {
          DINGO_LOG(INFO) << "region_id : " << result.region_id << " status : " << result.status.ToString();
          tag = true;
          sleep(1);
          break;
        }
      }
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

  PrepareVectorIndex();
  PrepareVectorClient();

  {
    // By index
    VectorAdd();
    VectorBuildByIndex();
    CheckBuildedByIndex();
    VectorStatusByIndex();
    VectorCountMemory();
    VectorLoadByIndex();
    VectorStatusByIndex();
    VectorDump();
    CheckLoadedByIndex();
    VectorStatusByIndex();
    VectorSearch();
  }

  {
    // By region
    PreCheckResetByRegion();
    VectorResetByRegion();
    VectorStatusByIndex();
    VectorBuildByRegion();
    CheckBuildedByRegion();
    VectorStatusByRegion();
    VectorLoadByRegion();
    CheckLoadedByRegion();
    VectorStatusByRegion();
    VectorDump();
    VectorCountMemory();
    VectorSearch();
    VectorStatusByRegion();
    PreCheckResetByIndex();
    VectorResetByIndex();
    VectorStatusByIndex();
  }

  {
    // reset  after build
    VectorBuildByIndex();
    CheckBuildedByIndex();
    PreCheckResetByRegion();
    VectorStatusByIndex();
    VectorResetByRegion();
    VectorStatusByRegion();
    VectorStatusByIndex();
    VectorBuildByRegion();
    CheckBuildedByRegion();
  }

  {
    // reset after load
    VectorLoadByIndex();
    CheckLoadedByIndex();
    PreCheckResetByRegion();
    VectorResetByRegion();
    VectorStatusByRegion();
    VectorStatusByIndex();
    VectorBuildByRegion();
    CheckBuildedByRegion();
    VectorLoadByRegion();
    CheckLoadedByRegion();
    VectorSearch();
  }

  VectorDelete();
  PostClean();
}
