#include <getopt.h>
#include <ostream>

#include <gflags/gflags.h>

#include "../../engine.h"
#include "../bench.h"
#include "ycsb.h"

DEFINE_uint64(ycsb_ops_per_tx, 10, "Number of operations to issue per transaction");
DEFINE_uint64(ycsb_ops_per_hot_tx, 10, "Number of operations to issue per hot transaction");
DEFINE_uint64(ycsb_ins_per_tx, 1, "Number of inserts per transaction");
DEFINE_uint64(ycsb_update_per_tx, 1, "Number of updates per transaction");
DEFINE_double(ycsb_hot_tx_percent, 0, "Percentage of hot transactions in the whole workload");
DEFINE_double(ycsb_remote_tx_percent, 0, "Percentage of remote transactions in the whole workload");
DEFINE_uint64(ycsb_cold_ops_per_tx, 0, "Cold operations to issue per transaction");
DEFINE_uint64(ycsb_rmw_additional_reads, 0, "Additional reads to issue in the RMW transaction");
DEFINE_uint64(ycsb_hot_table_size, 100000, "In-memory (hot) table size");
DEFINE_uint64(ycsb_cold_table_size, 0, "In-storage (cold) table size");
DEFINE_uint64(ycsb_max_scan_size, 10, "Maximum scan size");
DEFINE_uint64(ycsb_latency, 0, "Simulated data access latency");

DEFINE_double(ycsb_zipfian_theta, 0.99, "Zipfian theta (for hot table only)");

DEFINE_string(ycsb_workload, "C", "Workload: A - H");
DEFINE_string(ycsb_hot_table_rng, "uniform", "RNG to use to issue ops for the hot table; uniform or zipfian.");
DEFINE_string(ycsb_read_tx_type, "sequential", "Type of read transaction: sequential or multiget-amac.");

bool g_hot_table_zipfian = false;

// TODO: support scan_min length, current zipfain rng does not support min bound.
const int g_scan_min_length = 1;
int g_scan_length_zipfain_rng = 0;
double g_scan_length_zipfain_theta = 0.99;

// A spin barrier for all loaders to finish
std::atomic<uint32_t> loaders_barrier(0);
std::atomic<bool> all_flushed(false);

ReadTransactionType g_read_txn_type = ReadTransactionType::Sequential;

// { insert, read, update, scan, rmw }
YcsbWorkload YcsbWorkloadA('A', 0, 50U, 100U, 0, 0);  // Workload A - 50% read, 50% update
YcsbWorkload YcsbWorkloadB('B', 0, 95U, 100U, 0, 0);  // Workload B - 95% read, 5% update
YcsbWorkload YcsbWorkloadC('C', 0, 100U, 0, 0, 0);  // Workload C - 100% read
YcsbWorkload YcsbWorkloadD('D', 5U, 100U, 0, 0, 0);  // Workload D - 95% read, 5% insert
YcsbWorkload YcsbWorkloadE('E', 5U, 0, 0, 100U, 0);  // Workload E - 5% insert, 95% scan

// Combine reps_per_tx and rmw_additional_reads to have "10R+10RMW" style
// transactions.
YcsbWorkload YcsbWorkloadF('F', 0, 0, 0, 0, 100U);  // Workload F - 100% RMW

// Extra workloads (not in spec)
YcsbWorkload YcsbWorkloadG('G', 0, 0, 5U, 100U, 0);  // Workload G - 5% update, 95% scan
YcsbWorkload YcsbWorkloadH('H', 0, 0, 0, 100U, 0);  // Workload H - 100% scan
YcsbWorkload YcsbWorkloadI('I', 100U, 0, 0, 0, 0);  // Workload I - 100% insert

YcsbWorkload ycsb_workload = YcsbWorkloadC;

void ycsb_create_db(ermia::Engine *db) {
  ermia::thread::Thread *thread = ermia::thread::GetThread(true);
  ALWAYS_ASSERT(thread);

  auto create_table = [=](char *) {
    db->CreateTable("USERTABLE");
    db->CreateMasstreePrimaryIndex("USERTABLE", std::string("USERTABLE"));
  };

  thread->StartTask(create_table);
  thread->Join();
  ermia::thread::PutThread(thread);
}

void ycsb_table_loader::load() {
  LOG(INFO) << "Loading user table, " << FLAGS_ycsb_hot_table_size << " hot records, " << FLAGS_ycsb_cold_table_size << " cold records.";
  do_load(open_tables.at("USERTABLE"), "USERTABLE", FLAGS_ycsb_hot_table_size, FLAGS_ycsb_cold_table_size);
}

void ycsb_table_loader::do_load(ermia::OrderedIndex *tbl, std::string table_name, uint64_t hot_record_count, uint64_t cold_record_count) {
  uint32_t nloaders = std::thread::hardware_concurrency() / (numa_max_node() + 1) / 2 * ermia::config::numa_nodes;
  int64_t hot_to_insert = hot_record_count / nloaders;
  int64_t cold_to_insert = cold_record_count / nloaders;
  uint64_t hot_start_key = loader_id * hot_to_insert;
  uint64_t cold_start_key = hot_record_count + loader_id * cold_to_insert;
  int64_t to_insert = hot_to_insert + cold_to_insert;

  uint64_t kBatchSize = 256;

  // Load hot records.
  ermia::transaction *txn = db->NewTransaction(0, *arena, txn_buf());
  for (uint64_t i = 0; i < hot_to_insert; ++i) {
    ermia::varstr &k = str(sizeof(ycsb_kv::key));
    BuildKey(hot_start_key + i, k);

    ermia::varstr &v = str(sizeof(ycsb_kv::value));
    new (&v) ermia::varstr((char *)&v + sizeof(ermia::varstr), sizeof(ycsb_kv::value));
    *(char *)v.p = 'a';

#if defined(NESTED_COROUTINE)
      TryVerifyStrict(sync_wait_coro(tbl->InsertRecord(txn, k, v)));
#else
      TryVerifyStrict(tbl->InsertRecord(txn, k, v));
#endif

    if ((i + 1) % kBatchSize == 0 || i == hot_to_insert - 1) {
      TryVerifyStrict(db->Commit(txn));
      if (i != hot_to_insert - 1) {
        txn = db->NewTransaction(0, *arena, txn_buf());
      }
    }
  }

  // Load cold records.
  if (cold_to_insert) {
    txn = db->NewTransaction(0, *arena, txn_buf());
  }
  for (uint64_t i = 0; i < cold_to_insert; ++i) {
    ermia::varstr &k = str(sizeof(ycsb_kv::key));
    BuildKey(cold_start_key + i, k);

    ermia::varstr &v = str(sizeof(ycsb_kv::value));
    new (&v) ermia::varstr((char *)&v + sizeof(ermia::varstr), sizeof(ycsb_kv::value));
    *(char *)v.p = 'a';

#if defined(NESTED_COROUTINE)
      TryVerifyStrict(sync_wait_coro(tbl->InsertColdRecord(txn, k, v)));
#else
      TryVerifyStrict(tbl->InsertColdRecord(txn, k, v));
#endif

    if ((i + 1) % kBatchSize == 0 || i == cold_to_insert - 1) {
      TryVerifyStrict(db->Commit(txn));
      if (i != cold_to_insert - 1) {
        txn = db->NewTransaction(0, *arena, txn_buf());
      }
    }
  }

  uint32_t n = ++loaders_barrier;
  while (loaders_barrier != nloaders && !all_flushed);

  if (n == nloaders) {
    ermia::dlog::flush_all();
    loaders_barrier = 0;
    all_flushed = true;
  }
  while (loaders_barrier);

  // Verify inserted values
  txn = db->NewTransaction(0, *arena, txn_buf());
  for (uint64_t i = 0; i < hot_to_insert; ++i) {
    rc_t rc = rc_t{RC_INVALID};
    ermia::varstr &k = str(sizeof(ycsb_kv::key));
    BuildKey(hot_start_key + i, k);
    ermia::varstr &v = str(0);
#if defined(NESTED_COROUTINE)
    sync_wait_coro(tbl->GetRecord(txn, rc, k, v));
#else
    tbl->GetRecord(txn, rc, k, v);
#endif
    ALWAYS_ASSERT(*(char *)v.data() == 'a');
    TryVerifyStrict(rc);

    if ((i + 1) % kBatchSize == 0 || i == hot_to_insert - 1) {
      TryVerifyStrict(db->Commit(txn));
      if (i != hot_to_insert - 1) {
        txn = db->NewTransaction(0, *arena, txn_buf());
      }
    }
  }

  if (cold_to_insert) {
    txn = db->NewTransaction(0, *arena, txn_buf());
  }
  for (uint64_t i = 0; i < cold_to_insert; ++i) {
    rc_t rc = rc_t{RC_INVALID};
    ermia::varstr &k = str(sizeof(ycsb_kv::key));
    BuildKey(cold_start_key + i, k);
    ermia::varstr &v = str(0);
#if defined(NESTED_COROUTINE)
    sync_wait_coro(tbl->GetRecord(txn, rc, k, v));
#else
    tbl->GetRecord(txn, rc, k, v);
#endif
    ALWAYS_ASSERT(*(char *)v.data() == 'a');
    TryVerifyStrict(rc);

    if ((i + 1) % kBatchSize == 0 || i == cold_to_insert - 1) {
      TryVerifyStrict(db->Commit(txn));
      if (i != cold_to_insert - 1) {
        txn = db->NewTransaction(0, *arena, txn_buf());
      }
    }
  }

  if (ermia::config::verbose) {
    std::cerr << "[INFO] loader " << loader_id << " loaded " << to_insert
              << " keys in " << table_name << std::endl;
  }
}

void ycsb_parse_options() {
  ermia::config::read_txn_type = FLAGS_ycsb_read_tx_type;
  if (FLAGS_ycsb_read_tx_type == "sequential") {
    g_read_txn_type = ReadTransactionType::Sequential;
  } else if (FLAGS_ycsb_read_tx_type == "nested-coro") {
    g_read_txn_type = ReadTransactionType::NestedCoro;
  } else if (FLAGS_ycsb_read_tx_type == "simple-coro") {
    g_read_txn_type = ReadTransactionType::SimpleCoro;
    ermia::config::coro_tx = true;
  } else if (FLAGS_ycsb_read_tx_type == "hybrid-coro") {
    g_read_txn_type = ReadTransactionType::HybridCoro;
  } else if (FLAGS_ycsb_read_tx_type == "flat-coro") {
    g_read_txn_type = ReadTransactionType::FlatCoro;
  } else if (FLAGS_ycsb_read_tx_type == "multiget-simple-coro") {
    g_read_txn_type = ReadTransactionType::SimpleCoroMultiGet;
    ermia::config::coro_tx = true;
  } else if (FLAGS_ycsb_read_tx_type == "multiget-nested-coro") {
    g_read_txn_type = ReadTransactionType::NestedCoroMultiGet;
  } else if (FLAGS_ycsb_read_tx_type == "multiget-amac") {
    g_read_txn_type = ReadTransactionType::AMACMultiGet;
  } else {
    LOG(FATAL) << "Wrong read transaction type " << std::string(optarg);
  }

  if (FLAGS_ycsb_workload == "A") {
    ycsb_workload = YcsbWorkloadA;
  } else if (FLAGS_ycsb_workload == "B") {
    ycsb_workload = YcsbWorkloadB;
  } else if (FLAGS_ycsb_workload == "C") {
    ycsb_workload = YcsbWorkloadC;
    ermia::config::coro_cold_tx_name = "1-ColdRead";
  } else if (FLAGS_ycsb_workload == "D") {
    ycsb_workload = YcsbWorkloadD;
  } else if (FLAGS_ycsb_workload == "E") {
    ycsb_workload = YcsbWorkloadE;
  } else if (FLAGS_ycsb_workload == "F") {
    ycsb_workload = YcsbWorkloadF;
    ermia::config::coro_cold_tx_name = "1-ColdRMW";
  } else if (FLAGS_ycsb_workload == "G") {
    ycsb_workload = YcsbWorkloadG;
  } else if (FLAGS_ycsb_workload == "H") {
    ycsb_workload = YcsbWorkloadH;
  } else if (FLAGS_ycsb_workload == "I") {
    ycsb_workload = YcsbWorkloadI;
  } else {
    std::cerr << "Wrong workload type: " << FLAGS_ycsb_workload << std::endl;
    abort();
  }

  g_hot_table_zipfian = FLAGS_ycsb_hot_table_rng == "zipfian";

  ALWAYS_ASSERT(FLAGS_ycsb_hot_table_size);

  std::cerr << "ycsb settings:" << std::endl
            << "  workload:                   " << FLAGS_ycsb_workload << std::endl
            << "  hot table size:             " << FLAGS_ycsb_hot_table_size << std::endl
            << "  cold table size:            " << FLAGS_ycsb_cold_table_size << std::endl
            << "  operations per transaction: " << FLAGS_ycsb_ops_per_tx << std::endl
            << "  operations per hot transaction: " << FLAGS_ycsb_ops_per_hot_tx << std::endl
            << "  inserts per transaction: "    << FLAGS_ycsb_ins_per_tx << std::endl
            << "  updates per transaction: "    << FLAGS_ycsb_update_per_tx << std::endl
            << "  cold record reads per transaction: " << FLAGS_ycsb_cold_ops_per_tx << std::endl
            << "  additional reads after RMW: " << FLAGS_ycsb_rmw_additional_reads << std::endl
            << "  distribution:               " << FLAGS_ycsb_hot_table_rng << std::endl
            << "  read transaction type:      " << FLAGS_ycsb_read_tx_type << std::endl
            << "  simulated latency:          " << FLAGS_ycsb_latency << " microseconds" << std::endl;

  if (FLAGS_ycsb_hot_tx_percent) {
    std::cerr << "  hot read transaction percentage: " << FLAGS_ycsb_hot_tx_percent << std::endl;
  }
  if (FLAGS_ycsb_remote_tx_percent) {
    std::cerr << "  remote read transaction percentage: " << FLAGS_ycsb_remote_tx_percent << std::endl;
  }
  if (FLAGS_ycsb_hot_table_rng == "zipfian") {
    std::cerr << "  hot table zipfian theta:   " << FLAGS_ycsb_zipfian_theta << std::endl;
  }
  if (ycsb_workload.scan_percent() > 0) {
    if (FLAGS_ycsb_max_scan_size < g_scan_min_length || g_scan_min_length < 1) {
      std::cerr << "  invalid scan range:      " << std::endl;
      std::cerr << "  min :                    " << g_scan_min_length
                << std::endl;
      std::cerr << "  max :                    " << FLAGS_ycsb_max_scan_size
                << std::endl;
    }
    std::cerr << "  scan maximal range:         " << FLAGS_ycsb_max_scan_size
              << std::endl;
  }
}
