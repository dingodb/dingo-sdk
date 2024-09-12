# /usr/bin/env python3

from os.path import dirname, abspath
import argparse
import time
from time import sleep

import dingosdk


parser = argparse.ArgumentParser(description="argparse")
parser.add_argument(
    "--coordinator_addrs",
    "-addrs",
    type=str,
    default="127.0.0.1:22001,127.0.0.1:22002,127.0.0.1:22003",
    help="coordinator addrs, try to use like 127.0.0.1:22001,127.0.0.1:22002,127.0.0.1:22003",
)
args = parser.parse_args()

g_schema_id = 2
g_index_id = 0
g_index_name = "example01"
g_range_partition_seperator_ids = [5, 10, 20]
g_dimension = 2

g_diskann_param = dingosdk.DiskAnnParam(
    g_dimension, dingosdk.MetricType.kL2, dingosdk.ValueType.kFloat
)
g_vector_ids = []
g_region_ids = set()
g_vector_count = 300

s, g_client = dingosdk.Client.BuildAndInitLog(args.coordinator_addrs)
assert s.ok(), f"client build fail, {s.ToString()}"

s, g_vector_client = g_client.NewVectorClient()
assert s.ok(), f"dingo vector client build fail, {s.ToString()}"


def prepare_vector_index():
    global g_index_id
    s, creator = g_client.NewVectorIndexCreator()
    assert s.ok(), f"dingo creator build fail: {s.ToString()}"

    creator.SetSchemaId(g_schema_id)
    creator.SetName(g_index_name)
    creator.SetReplicaNum(3)
    creator.SetRangePartitions(g_range_partition_seperator_ids)
    creator.SetDiskAnnParam(g_diskann_param)
    s, g_index_id = creator.Create()
    print(f"create index status: {s.ToString()}, index_id: {g_index_id}")
    assert s.ok(), f"create index fail: {s.ToString()}"

    time.sleep(3)


def post_clean(use_index_name=False):
    if use_index_name:
        tmp, index_id = g_client.GetVectorIndexId(g_schema_id, g_index_name)
        print(
            f"index_id: {index_id}, g_index_id: {g_index_id}, get indexid: {tmp.ToString()}"
        )
        tmp = g_client.DropVectorIndexByName(g_schema_id, g_index_name)
    else:
        tmp = g_client.DropVectorIndexById(g_index_id)

    print(f"drop index status: {tmp.ToString()}, index_id: {g_index_id}")
    g_vector_ids.clear()


def vector_add(use_index_name=False):
    vectors = []

    delta = 0.1
    for id in range(1, g_vector_count):
        tmp_vector = dingosdk.Vector(dingosdk.ValueType.kFloat, g_dimension)
        tmp_vector.float_values = [1.0 + delta, 2.0 + delta]
        tmp = dingosdk.VectorWithId(id, tmp_vector)
        vectors.append(tmp)

        g_vector_ids.append(id)
        delta += 1

    if use_index_name:
        add, vectors = g_vector_client.ImportAddByIndexName(
            g_schema_id, g_index_name, vectors, False, False
        )
    else:
        add, vectors = g_vector_client.ImportAddByIndexId(g_index_id, vectors)

    for v in vectors:
        print(f"add vector: {v.ToString()}")

    print(f"add vector status: {add.ToString()}")


def vector_search(use_index_name=False):
    target_vectors = []
    init = 0.1
    for i in range(5):
        tmp_vector = dingosdk.Vector(dingosdk.ValueType.kFloat, g_dimension)  ###
        tmp_vector.float_values = [init, init]

        tmp = dingosdk.VectorWithId()
        tmp.vector = tmp_vector
        target_vectors.append(tmp)

        init = init + 0.1

    param = dingosdk.SearchParam()
    param.topk = 2
    # param.use_brute_force = True
    param.extra_params[dingosdk.SearchExtraParamType.kParallelOnQueries] = 10

    if use_index_name:
        tmp, result = g_vector_client.SearchByIndexName(
            g_schema_id, g_index_name, param, target_vectors
        )
    else:
        tmp, result = g_vector_client.SearchByIndexId(g_index_id, param, target_vectors)

    print(f"vector search status: {tmp.ToString()}")
    for r in result:
        print(f"vector search result: {r.ToString()}")

    assert len(result) == len(target_vectors)

    for i in range(len(result)):
        search_result = result[i]
        if search_result.vector_datas:
            assert len(search_result.vector_datas) == param.topk
        vector_id = search_result.id
        assert vector_id.id == target_vectors[i].id
        assert vector_id.vector.Size() == target_vectors[i].vector.Size()


def vector_delete(use_index_name=False):
    if use_index_name:
        tmp = g_vector_client.ImportDeleteByIndexName(
            g_schema_id, g_index_name, g_vector_ids
        )
    else:
        tmp = g_vector_client.ImportDeleteByIndexId(g_index_id, g_vector_ids)

    if not tmp.ok():
        print(f"vector delete status: :{tmp.ToString()}")


def vector_status_by_index(use_index_name=False):
    if use_index_name:
        tmp, result = g_vector_client.StatusByIndexName(g_schema_id, g_index_name)
    else:
        tmp, result = g_vector_client.StatusByIndexId(g_index_id)

    if tmp.ok():
        print(f"vector status result :{ result.ToString()}")
    else:
        print(f"vector status status: {tmp.ToString()}")
    for i in range(len(result.region_states)):
        status_result = result.region_states[i]
        g_region_ids.add(status_result.region_id)


def vector_status_by_region(use_index_name=False):
    region_ids = []
    if len(g_region_ids) > 0:
        region_ids = [list(g_region_ids)[0]]
    if use_index_name:
        tmp, result = g_vector_client.StatusByRegionIdIndexName(
            g_schema_id, g_index_name, region_ids
        )
    else:
        tmp, result = g_vector_client.StatusByRegionId(g_index_id, region_ids)

    if tmp.ok():
        print(f"vector status result :{result.ToString()}")
    else:
        print(f"vector status status: {tmp.ToString()}")


def vector_reset_by_index(use_index_name=False):
    if use_index_name:
        tmp, result = g_vector_client.ResetByIndexName(g_schema_id, g_index_name)
    else:
        tmp, result = g_vector_client.ResetByIndexId(g_index_id)
    if tmp.IsResetFailed():
        print(f"vector Reset result :{ result.ToString()}")
    else:
        print(f"vector Reset status: {tmp.ToString()}")


def vector_reset_by_region(use_index_name=False):
    region_ids = []
    if len(g_region_ids) > 0:
        region_ids = [list(g_region_ids)[0]]
    if use_index_name:
        tmp, result = g_vector_client.ResetByRegionIdIndexName(
            g_schema_id, g_index_name, region_ids
        )
    else:
        tmp, result = g_vector_client.ResetByRegionId(g_index_id, region_ids)
    if tmp.IsResetFailed():
        print(f"vector Reset result :{result.ToString()}")
    else:
        print(f"vector Reset status: {tmp.ToString()}")


def vector_build_by_index(use_index_name=False):
    if use_index_name:
        tmp, result = g_vector_client.BuildByIndexName(g_schema_id, g_index_name)
    else:
        tmp, result = g_vector_client.BuildByIndexId(g_index_id)
    if tmp.IsBuildFailed():
        print(f"vector Build result :{result.ToString()}")
    else:
        print(f"vector Build status: {tmp.ToString()}")


def vector_build_by_region(use_index_name=False):
    region_ids = []
    if len(g_region_ids) > 0:
        region_ids = [list(g_region_ids)[0]]
    if use_index_name:
        tmp, result = g_vector_client.BuildByRegionIdIndexName(
            g_schema_id, g_index_name, region_ids
        )
    else:
        tmp, result = g_vector_client.BuildByRegionId(g_index_id, region_ids)
    if tmp.IsBuildFailed():
        print(f"vector Reset result :{result.ToString()}")
    else:
        print(f"vector Build status: {tmp.ToString()}")


def vector_load_by_index(use_index_name=False):
    if use_index_name:
        tmp, result = g_vector_client.LoadByIndexName(g_schema_id, g_index_name)
    else:
        tmp, result = g_vector_client.LoadByIndexId(g_index_id)
    if tmp.IsLoadFailed():
        print(f"vector Load result :{result.ToString()}")
    else:
        print(f"vector Load status: {tmp.ToString()}")


def vector_load_by_region(use_index_name=False):
    region_ids = []
    if len(g_region_ids) > 0:
        region_ids = [list(g_region_ids)[0]]
    if use_index_name:
        tmp, result = g_vector_client.LoadByRegionIdIndexName(
            g_schema_id, g_index_name, region_ids
        )
    else:
        tmp, result = g_vector_client.LoadByRegionId(g_index_id, region_ids)
    if tmp.IsLoadFailed():
        print(f"vector Load result :{result.ToString()}")
    else:
        print(f"vector Load status: {tmp.ToString()}")


def vector_count_memory(use_index_name=False):
    if use_index_name:
        tmp, result = g_vector_client.LoadByIndexName(g_schema_id, g_index_name)
    else:
        tmp, result = g_vector_client.CountMemoryByIndexId(g_index_id)
    if tmp.ok():
        print(f"vector CountMemory result :{result}")
    else:
        print(f"vector CountMemory status: {tmp.ToString()}")


def check_builded_by_index(use_index_name=False):
    tag = True
    while tag:
        tag = False
        if use_index_name:
            tmp, result = g_vector_client.StatusByIndexName(g_schema_id, g_index_name)
        else:
            tmp, result = g_vector_client.StatusByIndexId(g_index_id)
        for r in result.region_states:
            print(
                f"vector Check Build By Index region id : {r.region_id} , region state : {dingosdk.RegionStateToString(r.state)}"
            )
            if r.state != dingosdk.DiskANNRegionState.kBuilded:
                tag = True
                sleep(1)
                break


def check_loaded_by_index(use_index_name=False):
    tag = True
    while tag:
        tag = False
        if use_index_name:
            tmp, result = g_vector_client.StatusByIndexName(g_schema_id, g_index_name)
        else:
            tmp, result = g_vector_client.StatusByIndexId(g_index_id)
        for r in result.region_states:
            print(f"vector Check Load By Index status: {r.region_id}")
            if r.state != dingosdk.DiskANNRegionState.kLoaded:
                tag = True
                sleep(1)
                break


def check_reset_by_index(use_index_name=False):
    tag = True
    while tag:
        tag = False
        if use_index_name:
            tmp, result = g_vector_client.StatusByIndexName(g_schema_id, g_index_name)
        else:
            tmp, result = g_vector_client.StatusByIndexId(g_index_id)
        for r in result.region_states:
            print(
                f"vector Check Build By Index region id : {r.region_id} , region state : {dingosdk.RegionStateToString(r.state)}"
            )
            if (
                r.state == dingosdk.DiskANNRegionState.kBuilding
                or r.state == dingosdk.DiskANNRegionState.kLoading
            ):
                tag = True
                sleep(1)
                break


def check_builded_by_region(use_index_name=False):
    tag = True
    while tag:
        tag = False
        region_ids = []
        if len(g_region_ids) > 0:
            region_ids = [list(g_region_ids)[0]]
        if use_index_name:
            tmp, result = g_vector_client.StatusByRegionIdIndexName(
                g_schema_id, g_index_name, region_ids
            )
        else:
            tmp, result = g_vector_client.StatusByRegionId(g_index_id, region_ids)
        for r in result.region_states:
            print(
                f"vector Check Build By Index region id : {r.region_id} , region state : {dingosdk.RegionStateToString(r.state)}"
            )
            if r.state != dingosdk.DiskANNRegionState.kBuilded:
                tag = True
                sleep(1)
                break


def check_loaded_by_region(use_index_name=False):
    tag = True
    while tag:
        tag = False
        region_ids = []
        if len(g_region_ids) > 0:
            region_ids = [list(g_region_ids)[0]]
        if use_index_name:
            tmp, result = g_vector_client.StatusByRegionIdIndexName(
                g_schema_id, g_index_name, region_ids
            )
        else:
            tmp, result = g_vector_client.StatusByRegionId(g_index_id, region_ids)
        for r in result.region_states:
            print(
                f"vector Check Build By Index region id : {r.region_id} , region state : {dingosdk.RegionStateToString(r.state)}"
            )
            if r.state != dingosdk.DiskANNRegionState.kLoaded:
                tag = True
                sleep(1)
                break


def check_reset_by_region(use_index_name=False):
    tag = True
    while tag:
        tag = False
        region_ids = []
        if len(g_region_ids) > 0:
            region_ids = [list(g_region_ids)[0]]
        if use_index_name:
            tmp, result = g_vector_client.StatusByRegionIdIndexName(
                g_schema_id, g_index_name, region_ids
            )
        else:
            tmp, result = g_vector_client.StatusByRegionId(g_index_id, region_ids)
        for r in result.region_states:
            print(
                f"vector Check Build By Index region id : {r.region_id} , region state : {dingosdk.RegionStateToString(r.state)}"
            )
            if (
                r.state == dingosdk.DiskANNRegionState.kBuilding
                or r.state == dingosdk.DiskANNRegionState.kLoading
            ):
                tag = True
                sleep(1)
                break


if __name__ == "__main__":
    post_clean(True)
    prepare_vector_index()

    vector_add()

    vector_build_by_index()
    check_builded_by_index()
    vector_status_by_index()
    vector_count_memory()
    vector_load_by_index()
    vector_status_by_index()
    check_loaded_by_index()
    vector_status_by_index()
    vector_search()

    check_reset_by_region()
    vector_reset_by_region()
    vector_status_by_region()
    vector_build_by_region()
    check_builded_by_region()
    vector_status_by_region()
    vector_load_by_region()
    check_loaded_by_region()
    vector_status_by_region()
    vector_count_memory()
    vector_search()
    vector_status_by_region()
    check_reset_by_index()
    vector_reset_by_index()
    vector_status_by_index()

    vector_build_by_index()
    check_builded_by_index()
    check_reset_by_region()
    vector_status_by_index()
    vector_reset_by_region()
    vector_status_by_region()
    vector_status_by_index()
    vector_build_by_region()
    check_builded_by_region()

    vector_load_by_index()
    check_loaded_by_index()
    check_reset_by_region()
    vector_reset_by_region()
    vector_status_by_region()
    vector_status_by_index()
    vector_build_by_region()
    check_builded_by_region()
    vector_load_by_region()
    check_loaded_by_region()
    vector_search()

    vector_delete()
    post_clean()
