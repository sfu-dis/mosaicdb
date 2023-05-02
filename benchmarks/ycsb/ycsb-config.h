#include <gflags/gflags.h>

DECLARE_uint64(ycsb_ops_per_tx);
DECLARE_uint64(ycsb_ops_per_hot_tx);
DECLARE_uint64(ycsb_ins_per_tx);
DECLARE_uint64(ycsb_update_per_tx);
DECLARE_double(ycsb_hot_tx_percent);
DECLARE_double(ycsb_remote_tx_percent);
DECLARE_uint64(ycsb_cold_ops_per_tx);
DECLARE_uint64(ycsb_rmw_additional_reads);
DECLARE_uint64(ycsb_hot_table_size);
DECLARE_uint64(ycsb_cold_table_size);
DECLARE_uint64(ycsb_max_scan_size);
DECLARE_uint64(ycsb_latency);
DECLARE_double(ycsb_zipfian_theta);
DECLARE_string(ycsb_workload);
DECLARE_string(ycsb_hot_table_rng);
DECLARE_string(ycsb_read_tx_type);
