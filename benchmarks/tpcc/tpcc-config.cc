#include <getopt.h>

#include <gflags/gflags.h>

#include "../bench.h"
#include "tpcc-config.h"

// configuration flags
// XXX(shiges): For compatibility issues, we keep gflags configs and the old
// configs separately. Eventually we want to merge them together.
DEFINE_uint64(tpcc_scale_factor, 10, "Scale factor.");
DEFINE_bool(tpcc_disable_xpartition_txn, false, "Disable cross partition transactions.");
DEFINE_bool(tpcc_enable_separate_tree_per_partition, false, "Whether to use a separate index per partition.");
DEFINE_uint64(tpcc_new_order_remote_item_pct, 1, "");
DEFINE_bool(tpcc_new_order_fast_id_gen, false, "Whether to pre-generate new order IDs");
DEFINE_bool(tpcc_uniform_item_dist, false, "Whether to use uniform distribution for Item.");
DEFINE_bool(tpcc_order_status_scan_hack, true, "");  // TODO(shiges): reverse scan
DEFINE_uint64(tpcc_wh_temperature, 0, "");
DEFINE_uint64(tpcc_microbench_rows, 10, "");  // this many rows
// can't have both ratio and rows at the same time
DEFINE_uint64(tpcc_microbench_wr_rows, 0, "");  // this number of rows to write
DEFINE_uint64(tpcc_hybrid, 0, "");

DEFINE_string(tpcc_txn_workload_mix, "", "");
DEFINE_double(tpcc_wh_spread, 0, "How much percentage of time a worker should use a random WH.\
                                  0 - always use home WH; \
                                  0.5 - 50% of the time use a randome WH; \
                                  1 - always use a random WH.");
DEFINE_uint64(tpcc_nr_suppliers, 100, "Size of the Suppliers table.");

DEFINE_uint32(tpcc_cold_customer_pct, 0, "The percentage of cold customer records when ramping up the database");
DEFINE_uint32(tpcc_cold_item_pct, 0, "The percentage of cold item records when ramping up the database");

DEFINE_bool(tpcc_coro_local_wh, true, "Whether to use coro-local warehouse");

int g_wh_temperature = 0;
uint g_microbench_rows = 10;  // this many rows
// can't have both ratio and rows at the same time
int g_microbench_wr_rows = 0;  // this number of rows to write
int g_hybrid = 0;

// TPC-C workload mix
// 0: NewOrder
// 1: Payment
// 2: CreditCheck
// 3: Delivery
// 4: OrderStatus
// 5: StockLevel
// 6: TPC-CH query 2 variant - original query 2, but /w marginal stock table update
// 7: Microbenchmark-random - same as Microbenchmark, but uses random read-set range
unsigned g_txn_workload_mix[8] = {
    45, 43, 0, 4, 4, 4, 0, 0};  // default TPC-C workload mix

util::aligned_padded_elem<std::atomic<uint64_t>> *g_district_ids = nullptr;

SuppStockMap supp_stock_map(10000);  // value ranges 0 ~ 9999 ( modulo by 10k )

const Nation nations[] = {{48, "ALGERIA", 0},
                          {49, "ARGENTINA", 1},
                          {50, "BRAZIL", 1},
                          {51, "CANADA", 1},
                          {52, "EGYPT", 4},
                          {53, "ETHIOPIA", 0},
                          {54, "FRANCE", 3},
                          {55, "GERMANY", 3},
                          {56, "INDIA", 2},
                          {57, "INDONESIA", 2},
                          {65, "IRAN", 4},
                          {66, "IRAQ", 4},
                          {67, "JAPAN", 2},
                          {68, "JORDAN", 4},
                          {69, "KENYA", 0},
                          {70, "MOROCCO", 0},
                          {71, "MOZAMBIQUE", 0},
                          {72, "PERU", 1},
                          {73, "CHINA", 2},
                          {74, "ROMANIA", 3},
                          {75, "SAUDI ARABIA", 4},
                          {76, "VIETNAM", 2},
                          {77, "RUSSIA", 3},
                          {78, "UNITED KINGDOM", 3},
                          {79, "UNITED STATES", 1},
                          {80, "CHINA", 2},
                          {81, "PAKISTAN", 2},
                          {82, "BANGLADESH", 2},
                          {83, "MEXICO", 1},
                          {84, "PHILIPPINES", 2},
                          {85, "THAILAND", 2},
                          {86, "ITALY", 3},
                          {87, "SOUTH AFRICA", 0},
                          {88, "SOUTH KOREA", 2},
                          {89, "COLOMBIA", 1},
                          {90, "SPAIN", 3},
                          {97, "UKRAINE", 3},
                          {98, "POLAND", 3},
                          {99, "SUDAN", 0},
                          {100, "UZBEKISTAN", 2},
                          {101, "MALAYSIA", 2},
                          {102, "VENEZUELA", 1},
                          {103, "NEPAL", 2},
                          {104, "AFGHANISTAN", 2},
                          {105, "NORTH KOREA", 2},
                          {106, "TAIWAN", 2},
                          {107, "GHANA", 0},
                          {108, "IVORY COAST", 0},
                          {109, "SYRIA", 4},
                          {110, "MADAGASCAR", 0},
                          {111, "CAMEROON", 0},
                          {112, "SRI LANKA", 2},
                          {113, "ROMANIA", 3},
                          {114, "NETHERLANDS", 3},
                          {115, "CAMBODIA", 2},
                          {116, "BELGIUM", 3},
                          {117, "GREECE", 3},
                          {118, "PORTUGAL", 3},
                          {119, "ISRAEL", 4},
                          {120, "FINLAND", 3},
                          {121, "SINGAPORE", 2},
                          {122, "NORWAY", 3}};

const char *regions[] = {"AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE EAST"};

std::string tpcc_worker_mixin::NameTokens[] = {
    std::string("BAR"),   std::string("OUGHT"), std::string("ABLE"), std::string("PRI"),
    std::string("PRES"),  std::string("ESE"),   std::string("ANTI"), std::string("CALLY"),
    std::string("ATION"), std::string("EING"),
};

std::vector<uint> tpcc_worker_mixin::hot_whs;
std::vector<uint> tpcc_worker_mixin::cold_whs;

void tpcc_parse_options() {
  bool did_spec_remote_pct = false;
  g_wh_temperature = FLAGS_tpcc_wh_temperature;
  g_microbench_rows = FLAGS_tpcc_microbench_rows;
  g_microbench_wr_rows = FLAGS_tpcc_microbench_wr_rows;
  g_hybrid = FLAGS_tpcc_hybrid;

  ALWAYS_ASSERT(FLAGS_tpcc_microbench_rows > 0);
  ALWAYS_ASSERT(FLAGS_tpcc_new_order_remote_item_pct >= 0 &&
                FLAGS_tpcc_new_order_remote_item_pct <= 100);
  ALWAYS_ASSERT(FLAGS_tpcc_nr_suppliers > 0);
  if (!gflags::GetCommandLineFlagInfoOrDie("tpcc_new_order_remote_item_pct").is_default) {
    did_spec_remote_pct = true;
  }

  if (!gflags::GetCommandLineFlagInfoOrDie("tpcc_txn_workload_mix").is_default) {
    const std::vector<std::string> toks = util::split(FLAGS_tpcc_txn_workload_mix, ',');
    ALWAYS_ASSERT(toks.size() == ARRAY_NELEMS(g_txn_workload_mix));
    unsigned s = 0;
    for (size_t i = 0; i < toks.size(); i++) {
      unsigned p = strtoul(toks[i].c_str(), nullptr, 10);
      ALWAYS_ASSERT(p >= 0 && p <= 100);
      s += p;
      g_txn_workload_mix[i] = p;
    }
    ALWAYS_ASSERT(s == 100);
  }

  if (did_spec_remote_pct && FLAGS_tpcc_disable_xpartition_txn) {
    std::cerr << "WARNING: --new-order-remote-item-pct given with "
            "--disable-cross-partition-transactions" << std::endl;
    std::cerr << "  --new-order-remote-item-pct will have no effect" << std::endl;
  }

  if (g_wh_temperature) {
    // set up hot and cold WHs
    ALWAYS_ASSERT(NumWarehouses() * 0.2 >= 1);
    uint num_hot_whs = NumWarehouses() * 0.2;
    util::fast_random r(23984543);
    for (uint i = 1; i <= num_hot_whs; i++) {
    try_push:
      uint w = r.next() % NumWarehouses() + 1;
      if (find(tpcc_worker_mixin::hot_whs.begin(), tpcc_worker_mixin::hot_whs.end(), w) ==
          tpcc_worker_mixin::hot_whs.end())
        tpcc_worker_mixin::hot_whs.push_back(w);
      else
        goto try_push;
    }

    for (uint i = 1; i <= NumWarehouses(); i++) {
      if (find(tpcc_worker_mixin::hot_whs.begin(), tpcc_worker_mixin::hot_whs.end(), i) ==
          tpcc_worker_mixin::hot_whs.end())
        tpcc_worker_mixin::cold_whs.push_back(i);
    }
    ALWAYS_ASSERT(tpcc_worker_mixin::cold_whs.size() + tpcc_worker_mixin::hot_whs.size() ==
                  NumWarehouses());
  }

  if (ermia::config::verbose) {
    std::cerr << "tpcc settings:" << std::endl;
    std::cerr << "  scale_factor: " << FLAGS_tpcc_scale_factor << std::endl;
    if (g_wh_temperature) {
      std::cerr << "  hot whs for 80% accesses     :";
      for (uint i = 0; i < tpcc_worker_mixin::hot_whs.size(); i++)
        std::cerr << " " << tpcc_worker_mixin::hot_whs[i];
      std::cerr << std::endl;
    } else {
      std::cerr << "  random home warehouse (%)    : " << FLAGS_tpcc_wh_spread * 100 << std::endl;
    }
    std::cerr << "  cross_partition_transactions : " << !FLAGS_tpcc_disable_xpartition_txn
         << std::endl;
    std::cerr << "  separate_tree_per_partition  : "
         << FLAGS_tpcc_enable_separate_tree_per_partition << std::endl;
    std::cerr << "  new_order_remote_item_pct    : " << FLAGS_tpcc_new_order_remote_item_pct
         << std::endl;
    std::cerr << "  new_order_fast_id_gen        : " << FLAGS_tpcc_new_order_fast_id_gen
         << std::endl;
    std::cerr << "  uniform_item_dist            : " << FLAGS_tpcc_uniform_item_dist << std::endl;
    std::cerr << "  order_status_scan_hack       : " << FLAGS_tpcc_order_status_scan_hack
         << std::endl;
    std::cerr << "  microbench rows            : " << g_microbench_rows << std::endl;
    std::cerr << "  microbench wr ratio (%)    : "
         << g_microbench_wr_rows / g_microbench_rows << std::endl;
    std::cerr << "  microbench wr rows         : " << g_microbench_wr_rows << std::endl;
    std::cerr << "  number of suppliers : " << FLAGS_tpcc_nr_suppliers << std::endl;
    std::cerr << "  hybrid : " << g_hybrid << std::endl;
    std::cerr << "  workload_mix                 : "
         << util::format_list(g_txn_workload_mix,
                        g_txn_workload_mix + ARRAY_NELEMS(g_txn_workload_mix))
         << std::endl;
    std::cerr << "  coro-local warehouse: " << FLAGS_tpcc_coro_local_wh << std::endl;
  }
}
