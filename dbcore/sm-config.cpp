#include <unistd.h>
#include <numa.h>
#include "../macros.h"
#include "sm-config.h"
#include "sm-thread.h"
#include <iostream>

namespace ermia {
namespace config {

uint32_t arena_size_mb = 4;
bool threadpool = true;
bool tls_alloc = true;
bool verbose = true;
bool coro_tx = false;
uint32_t coro_batch_size = 1;
uint32_t coro_io_batch_size = 1;
uint32_t coro_remote_batch_size = 1;
bool coro_batch_schedule = false;
uint32_t coro_scheduler = 0;
uint32_t coro_io_scheduler = 0;
uint32_t coro_remote_scheduler = 0;
uint32_t coro_cold_queue_size = 1;
uint32_t coro_cold_tx_threshold = 0;
std::string coro_cold_tx_name = "";
uint32_t coro_check_cold_tx_interval = 1000;
bool scan_with_it = false;
std::string benchmark("");
uint32_t worker_threads = 0;
uint32_t io_threads = 0;
uint32_t remote_threads = 0;
uint32_t benchmark_seconds = 30;
uint64_t benchmark_transactions = 0;
bool parallel_loading = false;
bool retry_aborted_transactions = false;
bool quick_bench_start = false;
int backoff_aborted_transactions = 0;
int numa_nodes = 0;
int enable_gc = 0;
std::string tmpfs_dir("/dev/shm");
int enable_safesnap = 0;
int enable_ssi_read_only_opt = 0;
uint64_t ssn_read_opt_threshold = SSN_READ_OPT_DISABLED;
uint64_t log_buffer_mb = 64;
uint64_t log_segment_mb = 16384;
std::string log_dir("");
bool null_log_device = false;
bool truncate_at_bench_start = false;
bool htt_is_on = true;
bool physical_workers_only = true;
bool physical_io_workers_only = true;
bool print_cpu_util = false;
bool enable_perf = false;
std::string perf_record_event("");
uint64_t node_memory_gb = 12;
int recovery_warm_up_policy = WARM_UP_NONE;
bool pcommit = false;
uint32_t pcommit_queue_length = 50000;
uint32_t pcommit_timeout_ms = 1000;
uint64_t pcommit_size_kb = PAGE_SIZE;
uint64_t pcommit_bytes = PAGE_SIZE * 1024;
bool pcommit_thread = false;
bool log_key_for_update = false;
bool enable_chkpt = 0;
uint64_t chkpt_interval = 50;
bool phantom_prot = 0;
double cycles_per_byte = 0;
uint32_t state = kStateLoading;
bool full_replay = false;
uint32_t replay_threads = 0;
uint32_t threads = 0;
bool index_probe_only = false;
bool amac_version_chain = false;
bool numa_spread = false;
bool kStateRunning = false;
bool iouring_read_log = false;
bool log_direct_io = true;
std::string read_txn_type = "sequential";
uint64_t fetch_cold_tx_interval = 0;
bool test_spinlock = false;
uint64_t test_spinlock_cs = 0;
bool test_coro_spinlock = false;

void init() {
  ALWAYS_ASSERT(threads);
  // Here [threads] refers to worker threads, so use the number of physical cores
  // to calculate # of numa nodes
  if (numa_spread) {
    numa_nodes = threads > numa_max_node() + 1 ? numa_max_node() + 1 : threads;
  } else {
    uint32_t max = thread::cpu_cores.size() / (numa_max_node() + 1);
    numa_nodes = (threads + max - 1) / max;
    ALWAYS_ASSERT(numa_nodes);
  }

  LOG(INFO) << "Workloads may run on " << numa_nodes << " nodes";
}

void sanity_check() {
  LOG_IF(FATAL, tls_alloc && !threadpool) << "Cannot use TLS allocator without threadpool";
  //ALWAYS_ASSERT(recover_functor);
  ALWAYS_ASSERT(numa_nodes || !threadpool);
  ALWAYS_ASSERT(!pcommit || pcommit_queue_length);
}

}  // namespace config
}  // namespace ermia
