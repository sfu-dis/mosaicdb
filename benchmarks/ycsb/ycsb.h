#pragma once

#include <string>
#include <utility>
#include <vector>

#include "ycsb-config.h"
#include "../../third-party/foedus/zipfian_random.hpp"
#include "../bench.h"
#include "../record/encoder.h"
#include "../record/inline_str.h"
#include "../../macros.h"

extern bool g_hot_table_zipfian;
extern const int g_scan_min_length;
extern int g_scan_length_zipfain_rng;
extern double g_scan_length_zipfain_theta;

enum class ReadTransactionType {
  Sequential,
  AMACMultiGet,
  SimpleCoroMultiGet,
  NestedCoroMultiGet,
  SimpleCoro,
  NestedCoro,
  HybridCoro
};

// TODO(tzwang); support other value lengths and columns specified by user
#define YCSB_KEY_FIELDS(x, y) x(inline_str_fixed<8>, y_key)
#define YCSB_VALUE_FIELDS(x, y) x(inline_str_fixed<8>, y_value)
DO_STRUCT(ycsb_kv, YCSB_KEY_FIELDS, YCSB_VALUE_FIELDS);

inline void BuildKey(uint64_t key, ermia::varstr &k) {
  ASSERT (sizeof(ycsb_kv::key) % sizeof(uint64_t) == 0);
  static const char *prefix = "corobase";
  ycsb_kv::key extended_key;
  for (uint offset = 0; offset < sizeof(ycsb_kv::key); offset = offset + sizeof(uint64_t)) {
    if (offset + sizeof(uint64_t) < sizeof(ycsb_kv::key))
      memcpy((void *)(extended_key.y_key.data() + offset), (void *)prefix, sizeof(uint64_t));
    else
      *(uint64_t *)(extended_key.y_key.data() + offset) = __builtin_bswap64(key);
  }
  Encode(k, extended_key);
}

struct YcsbWorkload {
  YcsbWorkload(char desc, int16_t insert_percent, int16_t read_percent,
               int16_t update_percent, int16_t scan_percent,
               int16_t rmw_percent)
      : desc(desc),
        insert_percent_(insert_percent),
        read_percent_(read_percent),
        update_percent_(update_percent),
        scan_percent_(scan_percent),
        rmw_percent_(rmw_percent),
        rmw_additional_reads_(0),
        reps_per_tx_(1),
        distinct_keys_(true) {}

  YcsbWorkload() {}
  int16_t insert_percent() const { return insert_percent_; }
  int16_t read_percent() const {
    return read_percent_ == 0 ? 0 : read_percent_ - insert_percent_;
  }
  int16_t update_percent() const {
    return update_percent_ == 0 ? 0 : update_percent_ - read_percent_;
  }
  int16_t scan_percent() const {
    return scan_percent_ == 0 ? 0 : scan_percent_ - update_percent_;
  }
  int16_t rmw_percent() const {
    return rmw_percent_ == 0 ? 0 : rmw_percent_ - scan_percent_;
  }

  char desc;
  // Cumulative percentage of i/r/u/s/rmw. From insert...rmw the percentages
  // accumulates, e.g., i=5, r=12 => we'll have 12-5=7% of reads in total.
  int16_t insert_percent_;
  int16_t read_percent_;
  int16_t update_percent_;
  int16_t scan_percent_;
  int16_t rmw_percent_;
  int32_t rmw_additional_reads_;
  int32_t reps_per_tx_;
  bool distinct_keys_;
};

class ycsb_table_loader : public bench_loader {
 public:
  ycsb_table_loader(unsigned long seed, ermia::Engine *db,
                        const std::map<std::string, ermia::OrderedIndex *> &open_tables,
                        uint32_t loader_id)
      : bench_loader(seed, db, open_tables), loader_id(loader_id) {}
 private:
  uint32_t loader_id;
  void do_load(ermia::OrderedIndex *tbl, std::string table_name, uint64_t hot_record_count, uint64_t cold_record_count);

 protected:
  void load();
};

void ycsb_create_db(ermia::Engine *db);
void ycsb_parse_options();

template<class WorkerType>
class ycsb_bench_runner : public bench_runner {
 public:
  ycsb_bench_runner(ermia::Engine *db) : bench_runner(db) {
    ycsb_create_db(db);
  }

  virtual void prepare(char *) {
    open_tables["USERTABLE"] = ermia::TableDescriptor::GetPrimaryIndex("USERTABLE");
  }

 protected:
  virtual std::vector<bench_loader *> make_loaders() {
    uint64_t requested = FLAGS_ycsb_hot_table_size;
    uint32_t nloaders =
	    std::thread::hardware_concurrency() / (numa_max_node() + 1) / 2 * ermia::config::numa_nodes;

    uint64_t records_per_thread = std::max<uint64_t>(1, FLAGS_ycsb_hot_table_size / nloaders);
    FLAGS_ycsb_hot_table_size = records_per_thread * nloaders;
    std::cerr << "[INFO] requested for " << requested << " hot table records, will load "
         << FLAGS_ycsb_hot_table_size << std::endl;

    if (FLAGS_ycsb_cold_table_size > 0) {
      requested = FLAGS_ycsb_cold_table_size;
      records_per_thread = std::max<uint64_t>(1, FLAGS_ycsb_cold_table_size / nloaders);
      FLAGS_ycsb_cold_table_size = records_per_thread * nloaders;
      std::cerr << "[INFO] requested for " << requested << " cold table records, will load "
           << FLAGS_ycsb_cold_table_size << std::endl;
    }

    std::vector<bench_loader *> ret;
    for (uint32_t i = 0; i < nloaders; ++i) {
      ret.push_back(new ycsb_table_loader(0, db, open_tables, i));
    }
    return ret;
  }

  virtual std::vector<bench_worker *> make_workers() {
    util::fast_random r(8544290);
    std::vector<bench_worker *> ret;
    for (size_t i = 0; i < ermia::config::worker_threads; i++) {
      std::cerr<<"[INFO] Preparing worker #"<<i<<std::endl;
      auto seed = r.next();
      ret.push_back(new WorkerType(i, seed, db, open_tables, &barrier_a, &barrier_b));
    }
    return ret;
  }
};

class ycsb_base_worker : public bench_worker {
 public:
  ycsb_base_worker(unsigned int worker_id, unsigned long seed, ermia::Engine *db,
                   const std::map<std::string, ermia::OrderedIndex *> &open_tables,
                   spin_barrier *barrier_a, spin_barrier *barrier_b)
      : bench_worker(worker_id, true, seed, db, open_tables, barrier_a, barrier_b),
        table_index((ermia::ConcurrentMasstreeIndex*)open_tables.at("USERTABLE")) {
      const unsigned int key_rng_seed = 1237 + worker_id;
      uniform_rng = foedus::assorted::UniformRandom(key_rng_seed);
      if (g_hot_table_zipfian) {
          hot_table_zipfian_rng.init(FLAGS_ycsb_hot_table_size, FLAGS_ycsb_zipfian_theta, key_rng_seed);
      }

      const unsigned int scan_length_rng_seed = 2358 + worker_id;
      scan_length_uniform_rng =
          foedus::assorted::UniformRandom(scan_length_rng_seed);
      if (g_scan_length_zipfain_rng) {
          scan_length_zipfian_rng.init(FLAGS_ycsb_max_scan_size,
                                       g_scan_length_zipfain_theta,
                                       scan_length_rng_seed);
      }
  }

 protected:
  struct KeyCompare {
    explicit KeyCompare(ermia::varstr &baseline) : baseline(baseline) {}
    bool operator() (const ermia::varstr &arg) {
      return *(uint64_t*)arg.p == *(uint64_t*)baseline.p;
    }
    ermia::varstr &baseline;
  };

  ALWAYS_INLINE ermia::varstr &str(uint64_t size) { return *arena->next(size); }
  ALWAYS_INLINE ermia::varstr &str(ermia::str_arena &a, uint64_t size) { return *a.next(size); }

  inline uint64_t rng_gen_key(bool hot) {
    uint64_t r = 0;
    if (hot) {
      if (g_hot_table_zipfian) {
        r = hot_table_zipfian_rng.next();
      } else {
        r = uniform_rng.uniform_within(0, FLAGS_ycsb_hot_table_size - 1);
      }
    } else {
      r = uniform_rng.uniform_within(FLAGS_ycsb_hot_table_size, FLAGS_ycsb_hot_table_size + FLAGS_ycsb_cold_table_size - 1);
    }
    return r;
  }

  inline uint64_t gen_new_key() {
    static std::atomic<uint64_t> x(FLAGS_ycsb_hot_table_size);
    uint64_t r = x.fetch_add(1);
    return r;
  }

  uint64_t rng_gen_scan_length() {
    uint64_t r = 0;
    if(g_scan_length_zipfain_rng) {
      while (!r)
        r = scan_length_zipfian_rng.next();
    } else {
      r = scan_length_uniform_rng.uniform_within(g_scan_min_length, FLAGS_ycsb_max_scan_size);
    }
    return r;
  }

  ermia::varstr &GenerateKey(ermia::transaction *t, bool hot = true) {
    ermia::varstr &k = t ? *t->string_allocator().next(sizeof(ycsb_kv::key)) : str(sizeof(ycsb_kv::key));
    new (&k) ermia::varstr((char *)&k + sizeof(ermia::varstr), sizeof(ycsb_kv::key));
    ::BuildKey(rng_gen_key(hot), k);
    return k;
  }

  ermia::varstr &GenerateNewKey(ermia::transaction *t) {
    ermia::varstr &k = t ? *t->string_allocator().next(sizeof(ycsb_kv::key)) : str(sizeof(ycsb_kv::key));
    new (&k) ermia::varstr((char *)&k + sizeof(ermia::varstr), sizeof(ycsb_kv::key));
    ::BuildKey(gen_new_key(), k);
    return k;
  }

  struct ScanRange {
    ermia::varstr &start_key;
    ermia::varstr &end_key;
  };

  ScanRange GenerateScanRange(ermia::transaction *t, bool hot = true) {
    ermia::varstr &start_key = t ? *t->string_allocator().next(sizeof(ycsb_kv::key)) : str(sizeof(ycsb_kv::key));
    ermia::varstr &end_key = t ? *t->string_allocator().next(sizeof(ycsb_kv::key)) : str(sizeof(ycsb_kv::key));

    new (&start_key) ermia::varstr((char *)&start_key + sizeof(ermia::varstr), sizeof(ycsb_kv::key));
    new (&end_key) ermia::varstr((char *)&end_key + sizeof(ermia::varstr), sizeof(ycsb_kv::key));
    uint64_t r_start_key = rng_gen_key(hot);
    uint64_t r_end_key = r_start_key + rng_gen_scan_length();
    ::BuildKey(r_start_key, start_key);
    ::BuildKey(r_end_key, end_key);
    return {start_key, end_key};
  }

  ermia::ConcurrentMasstreeIndex *table_index;
  foedus::assorted::UniformRandom uniform_rng;
  foedus::assorted::ZipfianRandom hot_table_zipfian_rng;
  foedus::assorted::UniformRandom scan_length_uniform_rng;
  foedus::assorted::ZipfianRandom scan_length_zipfian_rng;
};

class ycsb_scan_callback : public ermia::OrderedIndex::ScanCallback {
  public:
    ycsb_scan_callback() : n(0){}
    bool Invoke(const char *keyp, size_t keylen, const ermia::varstr &value) override {
      MARK_REFERENCED(keyp);
      MARK_REFERENCED(keylen);
#if defined(SI)
      ASSERT(*(char *)value.data() == 'a');
#endif
      memcpy(value_buf, value.data(), sizeof(ycsb_kv::value));
      memcpy(key_buf, keyp, keylen);
      n++;
      return true;
    }

  inline size_t size() const { return n; }

  private:
    int32_t n;
    unsigned char key_buf[sizeof(ycsb_kv::key)];
    unsigned char value_buf[sizeof(ycsb_kv::value)];
};
