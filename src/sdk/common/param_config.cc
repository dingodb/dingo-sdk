
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

#include "sdk/common/param_config.h"

#include <gflags/gflags.h>

// sdk config
DEFINE_int64(actuator_thread_num, 8, "actuator thread num");
DEFINE_int64(txn_actuator_thread_num, 16, "txn actuator thread num");

// coordinator config
DEFINE_int64(coordinator_interaction_delay_ms, 500, "coordinator interaction delay ms");
DEFINE_int64(coordinator_interaction_max_retry, 600, "coordinator interaction max retry");
DEFINE_int64(auto_incre_req_count, 1000, "raw kv max retry times");

// ChannelOptions should set "timeout_ms > connect_timeout_ms" for circuit breaker
DEFINE_int64(rpc_channel_timeout_ms, 500000, "rpc channel timeout ms");
DEFINE_int64(rpc_channel_connect_timeout_ms, 3000, "rpc channel connect timeout ms");

// only used for grpc
DEFINE_int64(grpc_poll_thread_num, 32, "grpc poll cq thread num");

DEFINE_int64(rpc_max_retry, 3, "rpc call max retry times");
DEFINE_int64(rpc_time_out_ms, 500000, "rpc call timeout ms");

DEFINE_bool(enable_trace_rpc_performance, true, "enable trance rpc performance, use for debug");
DEFINE_int64(rpc_elapse_time_threshold_us, 1000, "rpc elapse time us threshold");
DEFINE_int64(rpc_trace_full_info_threshold_us, 1000000,
			 "log full rpc detail when elapsed time exceeds this threshold (us)");

DEFINE_int64(store_rpc_retry_delay_ms, 500, "store rpc retry delay ms");
DEFINE_int64(store_rpc_max_retry, 600, "store rpc max retry times, use case: wrong leader or request range invalid");

DEFINE_int64(scan_batch_size, 1000, "scan batch size, use for region scanner");

DEFINE_int64(txn_op_delay_ms, 300, "txn op delay ms");
DEFINE_int64(txn_op_max_retry, 20, "txn op max retry times");

DEFINE_int64(txn_prewrite_delay_ms, 500, "txn prewrite delay ms");
DEFINE_int64(txn_prewrite_max_retry, 300, "txn prewrite max retry");
DEFINE_bool(enable_txn_concurrent_prewrite, true, "enable txn concurrent prewrite");

DEFINE_int64(raw_kv_delay_ms, 500, "raw kv backoff delay ms");
DEFINE_int64(raw_kv_max_retry, 10, "raw kv max retry times");

DEFINE_int64(vector_op_delay_ms, 500, "vector task base backoff delay ms");
DEFINE_int64(vector_op_max_retry, 30, "vector task max retry times");

DEFINE_int64(txn_max_batch_count, 4096, "txn max batch count");
DEFINE_int64(txn_max_async_commit_count, 256, "txn max async commit count");
DEFINE_bool(enable_txn_async_commit, true, "enable txn async commit");

DEFINE_bool(log_rpc_time, false, "log rpc time");

DEFINE_int64(txn_heartbeat_interval_ms, 8000, "txn heartbeat interval time");
DEFINE_int64(txn_heartbeat_lock_delay_ms, 20000, "txn heartbeat lock delay time");

DEFINE_int64(txn_check_status_interval_ms, 100, "txn check status interval ms");

DEFINE_uint32(stale_period_us, 1000, "stale period us default 1000 us, used for tso provider");
DEFINE_uint32(tso_batch_size, 256, "tso batch size default 256, used for tso provider");
