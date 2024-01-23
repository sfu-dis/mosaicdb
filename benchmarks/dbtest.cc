#include <gflags/gflags.h>

#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <utility>

#include "dbtest.h"
#include "../dbcore/rcu.h"

#if defined(SSI) && defined(SSN)
#error "SSI + SSN?"
#endif

DEFINE_bool(threadpool, true,
            "Whether to use ERMIA thread pool (no oversubscription)");
DEFINE_uint64(arena_size_mb, 8,
              "Size of transaction arena (private workspace) in MB");
DEFINE_bool(tls_alloc, true,
            "Whether to use the TLS allocator defined in sm-alloc.h");
DEFINE_bool(htt, true,
            "Whether the HW has hyper-threading enabled."
            "Ignored if auto-detection of physical cores succeeded.");
DEFINE_bool(
    physical_workers_only, true,
    "Whether to only use one thread per physical core as transaction workers.");
DEFINE_bool(
    physical_io_workers_only, true,
    "Whether to only use one thread per physical core as cold transaction workers.");
DEFINE_bool(amac_version_chain, false,
            "Whether to use AMAC for traversing version chain; applicable only "
            "for multi-get.");
DEFINE_bool(coro_tx, false,
            "Whether to turn each transaction into a coroutine");
DEFINE_uint64(coro_batch_size, 1, "Number of in-flight coroutines");
DEFINE_uint64(coro_io_batch_size, 1, "Number of in-flight coroutines");
DEFINE_uint64(coro_remote_batch_size, 1, "Number of in-flight coroutines");
DEFINE_bool(coro_batch_schedule, false,
            "Whether to run the same type of transactions per batch");
DEFINE_uint32(coro_scheduler, 0,
            "Different scheduling modes in the hybrid-coro/nested-coro YCSB."
            "0: batch scheduler"
            "1: pipeline scheduler"
            "2: pipeline with admission control"
            "3: dual pipeline with admission control");
DEFINE_uint32(coro_io_scheduler, 0,
            "Different I/O scheduling modes in the hybrid-coro YCSB."
            "0: batch scheduler"
            "1: pipeline scheduler"
            "2: pipeline with admission control"
            "3: dual pipeline with admission control");
DEFINE_uint32(coro_remote_scheduler, 0,
            "Different remote scheduling modes in the hybrid-coro YCSB."
            "0: batch scheduler"
            "1: pipeline scheduler"
            "2: pipeline with admission control"
            "3: dual pipeline with admission control");
DEFINE_uint32(coro_cold_queue_size, 0, "Dual-queue pipeline cold queue size");
DEFINE_uint32(coro_cold_tx_threshold, 0, "Max number of cold transactions in the scheduler");
DEFINE_string(coro_cold_tx_name, "", "Cold transaction name used for admission control in schedulers");
DEFINE_uint32(coro_check_cold_tx_interval, 1024, "The interval of checking cold transactions measured by # of committed hot transactions");
DEFINE_bool(scan_with_iterator, false,
            "Whether to run scan with iterator version or callback version");
DEFINE_bool(verbose, true, "Verbose mode.");
DEFINE_bool(index_probe_only, false,
            "Whether the read is only probing into index");
DEFINE_uint64(threads, 1, "Number of worker threads to run transactions.");
DEFINE_uint64(io_threads, 0, "Number of worker threads to run io operations.");
DEFINE_uint64(remote_threads, 0, "Number of worker threads to run remote operations.");
DEFINE_uint64(node_memory_gb, 16, "GBs of memory to allocate per node.");
DEFINE_bool(numa_spread, false,
            "Whether to pin threads in spread mode (compact if false)");
DEFINE_string(tmpfs_dir, "/dev/shm",
              "Path to a tmpfs location. Used by log buffer.");
DEFINE_string(log_data_dir, "/tmpfs/ermia-log", "Log directory.");
DEFINE_uint64(log_buffer_mb, 8, "Log buffer size in MB.");
DEFINE_uint64(log_segment_mb, 16384, "Log segment size in MB.");
DEFINE_bool(log_direct_io, true, "Whether to use O_DIRECT for dlog.");
DEFINE_bool(phantom_prot, false, "Whether to enable phantom protection.");
DEFINE_bool(print_cpu_util, false, "Whether to print CPU utilization.");
DEFINE_bool(enable_perf, false,
            "Whether to run Linux perf along with benchmark.");
DEFINE_string(perf_record_event, "", "Perf record event");
#if defined(SSN) || defined(SSI)
DEFINE_bool(safesnap, false,
            "Whether to use the safe snapshot (for SSI and SSN only).");
#endif
#ifdef SSN
DEFINE_string(ssn_read_opt_threshold, "0xFFFFFFFFFFFFFFFF",
              "Threshold for SSN's read optimization."
              "0 - don't track reads at all;"
              "0xFFFFFFFFFFFFFFFF - track all reads.");
#endif
#ifdef SSI
DEFINE_bool(ssi_read_only_opt, false,
            "Whether to enable SSI's read-only optimization."
            "Note: this is **not** safe snapshot.");
#endif

// Options specific to the primary
DEFINE_uint64(seconds, 10, "Duration to run benchmark in seconds.");
DEFINE_uint64(transactions, 0, "Number of transactions to finish in each run.");
DEFINE_bool(parallel_loading, true, "Load data in parallel.");
DEFINE_bool(retry_aborted_transactions, false,
            "Whether to retry aborted transactions.");
DEFINE_bool(backoff_aborted_transactions, false,
            "Whether backoff when retrying.");
DEFINE_string(
    recovery_warm_up, "none",
    "Method to load tuples during recovery:"
    "none - don't load anything; lazy - load tuples using a background thread; "
    "eager - load everything to memory during recovery.");
DEFINE_bool(enable_chkpt, false, "Whether to enable checkpointing.");
DEFINE_uint64(chkpt_interval, 10, "Checkpoint interval in seconds.");
DEFINE_bool(null_log_device, false, "Whether to skip writing log records.");
DEFINE_bool(truncate_at_bench_start, false,
            "Whether truncate the log/chkpt file written before starting "
            "benchmark (save tmpfs space).");
DEFINE_bool(log_key_for_update, false,
            "Whether to store the key in update log records.");
// Group (pipelined) commit related settings. The daemon will flush the log
// buffer
// when the following happens, whichever is earlier:
// 1. queue is full; 2. the log buffer is half full; 3. after [timeout] seconds.
DEFINE_bool(pcommit, true, "Whether to enable pipelined commit.");
DEFINE_uint64(pcommit_queue_length, 50000, "Pipelined commit queue length");
DEFINE_uint64(pcommit_timeout_ms, 1000,
              "Pipelined commit flush interval (in milliseconds).");
DEFINE_uint64(pcommit_size_kb, PAGE_SIZE,
              "Pipelined commit flush size interval in KB.");
DEFINE_bool(pcommit_thread, false,
            "Whether to use a dedicated pipelined committer thread.");
DEFINE_bool(enable_gc, false, "Whether to enable garbage collection.");
DEFINE_bool(iouring_read_log, true,
            "Whether to use iouring to load versions from logs.");
DEFINE_uint64(fetch_cold_tx_interval, 0, "The interval of fetching cold transactions measured by # of transactions");
DEFINE_bool(test_spinlock, false, "Acquire the spinlock before a transaction starts");
DEFINE_uint64(test_spinlock_cs, 0, "Simulated critical section");
DEFINE_bool(test_coro_spinlock, false, "Acquire the coroutine-optimized spinlock before a transaction starts");

static std::vector<std::string> split_ws(const std::string &s) {
  std::vector<std::string> r;
  std::istringstream iss(s);
  copy(std::istream_iterator<std::string>(iss),
       std::istream_iterator<std::string>(),
       std::back_inserter<std::vector<std::string>>(r));
  return r;
}

void bench_main(int argc, char **argv, std::function<void(ermia::Engine *)> test_fn) {
#ifndef NDEBUG
  std::cerr << "WARNING: benchmark built in DEBUG mode!!!" << std::endl;
#endif
  std::cerr << "PID: " << getpid() << std::endl;

  google::InitGoogleLogging(argv[0]);
  google::ParseCommandLineFlags(&argc, &argv, true);

  ermia::config::threadpool = FLAGS_threadpool;
  ermia::config::tls_alloc = FLAGS_tls_alloc;
  ermia::config::print_cpu_util = FLAGS_print_cpu_util;
  ermia::config::htt_is_on = FLAGS_htt;
  ermia::config::enable_perf = FLAGS_enable_perf;
  ermia::config::perf_record_event = FLAGS_perf_record_event;
  ermia::config::physical_workers_only = FLAGS_physical_workers_only;
  ermia::config::physical_io_workers_only = FLAGS_physical_io_workers_only;
  if (ermia::config::physical_workers_only)
    ermia::config::threads = FLAGS_threads;
  else
    ermia::config::threads = (FLAGS_threads + 1) / 2;
  ermia::config::index_probe_only = FLAGS_index_probe_only;
  ermia::config::verbose = FLAGS_verbose;
  ermia::config::node_memory_gb = FLAGS_node_memory_gb;
  ermia::config::numa_spread = FLAGS_numa_spread;
  ermia::config::tmpfs_dir = FLAGS_tmpfs_dir;
  ermia::config::log_dir = FLAGS_log_data_dir;
  ermia::config::log_buffer_mb = FLAGS_log_buffer_mb;
  ermia::config::log_segment_mb = FLAGS_log_segment_mb;
  ermia::config::log_direct_io = FLAGS_log_direct_io;

  if (FLAGS_log_direct_io) {
    // Log buffer must be 4KB aligned if enabled
    LOG_IF(FATAL, FLAGS_log_buffer_mb * 1024 % PAGE_SIZE) << "Log buffer must be aligned to enable O_DIRECT.";
  }

  ermia::config::phantom_prot = FLAGS_phantom_prot;
  // ermia::config::recover_functor = new
  // ermia::parallel_oid_replay(FLAGS_threads);

  ermia::config::amac_version_chain = FLAGS_amac_version_chain;

#if defined(SSI) || defined(SSN)
  ermia::config::enable_safesnap = FLAGS_safesnap;
#endif
#ifdef SSI
  ermia::config::enable_ssi_read_only_opt = FLAGS_ssi_read_only_opt;
#endif
#ifdef SSN
  ermia::config::ssn_read_opt_threshold =
      strtoul(FLAGS_ssn_read_opt_threshold.c_str(), nullptr, 16);
#endif

  ermia::config::arena_size_mb = FLAGS_arena_size_mb;

  ermia::config::coro_tx = FLAGS_coro_tx;

  LOG_IF(FATAL, FLAGS_coro_scheduler != 0 &&
                !(FLAGS_coro_batch_size > 0 && (FLAGS_coro_batch_size & (FLAGS_coro_batch_size - 1)) == 0))
          << "coro_batch_size needs to be the power of 2";
  ermia::config::coro_batch_size = FLAGS_coro_batch_size;
  ermia::config::coro_scheduler = FLAGS_coro_scheduler;
  ermia::config::coro_batch_schedule = FLAGS_coro_batch_schedule;

  LOG_IF(FATAL, !(FLAGS_coro_io_batch_size > 0 && (FLAGS_coro_io_batch_size & (FLAGS_coro_io_batch_size - 1)) == 0))
          << "coro_io_batch_size needs to be the power of 2";
  ermia::config::coro_io_scheduler = FLAGS_coro_io_scheduler;
  ermia::config::coro_io_batch_size = FLAGS_coro_io_batch_size;

  LOG_IF(FATAL, !(FLAGS_coro_remote_batch_size > 0 && (FLAGS_coro_remote_batch_size & (FLAGS_coro_remote_batch_size - 1)) == 0))
          << "coro_remote_batch_size needs to be the power of 2";
  ermia::config::coro_remote_scheduler = FLAGS_coro_remote_scheduler;
  ermia::config::coro_remote_batch_size = FLAGS_coro_remote_batch_size;

  LOG_IF(FATAL, !(FLAGS_coro_cold_queue_size == 0 || (FLAGS_coro_cold_queue_size > 0 && (FLAGS_coro_cold_queue_size & (FLAGS_coro_cold_queue_size - 1)) == 0)))
          << "coro_cold_queue_size needs to be the power of 2";
  ermia::config::coro_cold_queue_size = FLAGS_coro_cold_queue_size;
  ermia::config::coro_cold_tx_threshold = FLAGS_coro_cold_tx_threshold;
  ermia::config::coro_cold_tx_name = FLAGS_coro_cold_tx_name;
  ermia::config::coro_check_cold_tx_interval = FLAGS_coro_check_cold_tx_interval;

  ermia::config::scan_with_it = FLAGS_scan_with_iterator;

  ermia::config::benchmark_seconds = FLAGS_seconds;
  ermia::config::benchmark_transactions = FLAGS_transactions;
  ermia::config::retry_aborted_transactions = FLAGS_retry_aborted_transactions;
  ermia::config::backoff_aborted_transactions =
      FLAGS_backoff_aborted_transactions;
  ermia::config::null_log_device = FLAGS_null_log_device;
  ermia::config::truncate_at_bench_start = FLAGS_truncate_at_bench_start;

  ermia::config::replay_threads = 0;
  ermia::config::worker_threads = FLAGS_threads;
  ermia::config::io_threads = FLAGS_io_threads;
  ermia::config::remote_threads = FLAGS_remote_threads;

  ermia::config::pcommit = FLAGS_pcommit;
  ermia::config::pcommit_queue_length = FLAGS_pcommit_queue_length;
  ermia::config::pcommit_timeout_ms = FLAGS_pcommit_timeout_ms;
  ermia::config::pcommit_size_kb = FLAGS_pcommit_size_kb;
  ermia::config::pcommit_bytes = FLAGS_pcommit_size_kb * 1024;
  ermia::config::pcommit_thread = FLAGS_pcommit_thread;
  ermia::config::enable_chkpt = FLAGS_enable_chkpt;
  ermia::config::chkpt_interval = FLAGS_chkpt_interval;
  ermia::config::parallel_loading = FLAGS_parallel_loading;
  ermia::config::enable_gc = FLAGS_enable_gc;
  ermia::config::iouring_read_log = FLAGS_iouring_read_log;
  ermia::config::fetch_cold_tx_interval = FLAGS_fetch_cold_tx_interval;

  ermia::config::test_spinlock = FLAGS_test_spinlock;
  ermia::config::test_spinlock_cs = FLAGS_test_spinlock_cs;
  ermia::config::test_coro_spinlock = FLAGS_test_coro_spinlock;

  if (FLAGS_recovery_warm_up == "none") {
    ermia::config::recovery_warm_up_policy = ermia::config::WARM_UP_NONE;
  } else if (FLAGS_recovery_warm_up == "lazy") {
    ermia::config::recovery_warm_up_policy = ermia::config::WARM_UP_LAZY;
  } else if (FLAGS_recovery_warm_up == "eager") {
    ermia::config::recovery_warm_up_policy = ermia::config::WARM_UP_EAGER;
  } else {
    LOG(FATAL) << "Invalid recovery warm up policy: " << FLAGS_recovery_warm_up;
  }

  ermia::config::log_key_for_update = FLAGS_log_key_for_update;

  ermia::thread::Initialize();
  ermia::config::init();

  std::cerr << "CC: ";
#ifdef SSI
  std::cerr << "SSI";
  std::cerr << "  safe snapshot          : " << ermia::config::enable_safesnap
            << std::endl;
  std::cerr << "  read-only optimization : "
            << ermia::config::enable_ssi_read_only_opt << std::endl;
#elif defined(SSN)
#ifdef RC
  std::cerr << "RC+SSN";
  std::cerr << "  safe snapshot          : " << ermia::config::enable_safesnap
            << std::endl;
  std::cerr << "  read opt threshold     : 0x" << std::hex
            << ermia::config::ssn_read_opt_threshold << std::dec << std::endl;
#else
  std::cerr << "SI+SSN";
  std::cerr << "  safe snapshot          : " << ermia::config::enable_safesnap
            << std::endl;
  std::cerr << "  read opt threshold     : 0x" << std::hex
            << ermia::config::ssn_read_opt_threshold << std::dec << std::endl;
#endif
#elif defined(MVCC)
  std::cerr << "MVOCC";
#else
  std::cerr << "SI";
#endif
  std::cerr << std::endl;
  std::cerr << "  phantom-protection: " << ermia::config::phantom_prot
            << std::endl;

  std::cerr << "Settings and properties" << std::endl;
  std::cerr << "  amac-version-chain: " << FLAGS_amac_version_chain
            << std::endl;
  std::cerr << "  arena-size-mb     : " << FLAGS_arena_size_mb << std::endl;
  std::cerr << "  coro-tx           : " << FLAGS_coro_tx << std::endl;
  std::cerr << "  coro-batch-schedule: " << FLAGS_coro_batch_schedule
            << std::endl;
  std::cerr << "  coro-scheduler: " << FLAGS_coro_scheduler
            << std::endl;
  std::cerr << "  coro-batch-size   : " << FLAGS_coro_batch_size << std::endl;
  std::cerr << "  scan-use-iterator : " << FLAGS_scan_with_iterator
            << std::endl;
  std::cerr << "  enable-perf       : " << ermia::config::enable_perf
            << std::endl;
  std::cerr << "  pipelined commit  : " << ermia::config::pcommit << std::endl;
  std::cerr << "  dedicated pcommit thread: " << ermia::config::pcommit_thread
            << std::endl;
  std::cerr << "  index-probe-only  : " << FLAGS_index_probe_only << std::endl;
  std::cerr << "  iouring-read-log  : " << FLAGS_iouring_read_log << std::endl;
  std::cerr << "  log-buffer-mb     : " << ermia::config::log_buffer_mb
            << std::endl;
  std::cerr << "  log-dir           : " << ermia::config::log_dir << std::endl;
  std::cerr << "  log-segment-mb    : " << ermia::config::log_segment_mb
            << std::endl;
  std::cerr << "  log-direct-io     : " << ermia::config::log_direct_io << std::endl;
  std::cerr << "  masstree_internal_node_size: "
            << ermia::ConcurrentMasstree::InternalNodeSize() << std::endl;
  std::cerr << "  masstree_leaf_node_size    : "
            << ermia::ConcurrentMasstree::LeafNodeSize() << std::endl;
  std::cerr << "  node-memory       : " << ermia::config::node_memory_gb << "GB"
            << std::endl;
  std::cerr << "  null-log-device   : " << ermia::config::null_log_device
            << std::endl;
  std::cerr << "  num-threads       : " << ermia::config::threads << std::endl;
  std::cerr << "  numa-nodes        : " << ermia::config::numa_nodes
            << std::endl;
  std::cerr << "  numa-mode         : "
            << (ermia::config::numa_spread ? "spread" : "compact") << std::endl;
  std::cerr << "  perf-record-event : " << ermia::config::perf_record_event
            << std::endl;
  std::cerr << "  physical-workers-only: "
            << ermia::config::physical_workers_only << std::endl;
  std::cerr << "  physical-io-workers-only: "
            << ermia::config::physical_io_workers_only << std::endl;
  std::cerr << "  print-cpu-util    : " << ermia::config::print_cpu_util
            << std::endl;
  std::cerr << "  threadpool        : " << ermia::config::threadpool
            << std::endl;
  std::cerr << "  tmpfs-dir         : " << ermia::config::tmpfs_dir
            << std::endl;
  std::cerr << "  tls-alloc         : " << FLAGS_tls_alloc << std::endl;
  std::cerr << "  total-threads     : " << ermia::config::threads << std::endl;
#ifdef USE_VARINT_ENCODING
  std::cerr << "  var-encode        : yes" << std::endl;
#else
  std::cerr << "  var-encode        : no" << std::endl;
#endif
  std::cerr << "  worker-threads    : " << ermia::config::worker_threads << std::endl;
  if (ermia::config::io_threads) {
    std::cerr << "  io-threads    : " << ermia::config::io_threads << std::endl;
    std::cerr << "  io-scheduler    : " << ermia::config::coro_io_scheduler << std::endl;
    std::cerr << "  io-batch-size    : " << ermia::config::coro_io_batch_size << std::endl;
  }

  if (ermia::config::remote_threads) {
    std::cerr << "  remote-threads    : " << ermia::config::remote_threads << std::endl;
    std::cerr << "  remote-scheduler    : " << ermia::config::coro_remote_scheduler << std::endl;
    std::cerr << "  remote-batch-size    : " << ermia::config::coro_remote_batch_size << std::endl;
  }

  if (ermia::config::coro_scheduler == 3 || ermia::config::coro_scheduler == 4) {
      std::cerr << "  dual-queue pipeline cold queue size: " << ermia::config::coro_cold_queue_size << std::endl;
  }

  if (ermia::config::coro_cold_tx_threshold) {
    std::cerr << "  cold transaction name: " << ermia::config::coro_cold_tx_name << std::endl;
    std::cerr << "  max number of cold transactions in the scheduler: " << ermia::config::coro_cold_tx_threshold << std::endl;
  }

  if (ermia::config::coro_scheduler == 3 || ermia::config::coro_scheduler == 4) {
    std::cerr << "  interval of checking cold transactions measured by # of committed hot transactions: " << ermia::config::coro_check_cold_tx_interval << std::endl;
  }

  std::string myString = "rm -rf " + FLAGS_log_data_dir + "/*";
  const char* charPtr = myString.c_str();
  system(charPtr);
  ermia::MM::prepare_node_memory();

  // Must have everything in config ready by this point
  ermia::config::sanity_check();
  ermia::Engine *db = new ermia::Engine();

  // FIXME(tzwang): the current thread doesn't belong to the thread pool, and
  // it could be on any node. But not all nodes will be used by benchmark
  // (i.e., config::numa_nodes) and so not all nodes will have memory pool. So
  // here run on the first NUMA node to ensure we got a place to allocate memory
  numa_run_on_node(0);
  test_fn(db);
  delete db;
}

