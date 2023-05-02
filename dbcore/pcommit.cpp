#include <atomic>

#include "pcommit.h"
#include "sm-common.h"
#include "../engine.h"
#include "../macros.h"

namespace ermia {

namespace pcommit {

// tls_committer-local durable CSNs - belongs to tls_committer 
// but stored here together
uint64_t *_tls_durable_csn =
    (uint64_t *)malloc(sizeof(uint64_t) * config::MAX_THREADS);

// Up to which CSN are we sure all transcations are durable
std::atomic<uint64_t> global_durable_csn(0);

void commit_queue::push_back(uint64_t csn, uint64_t start_time, bool *flush, bool *insert) {
  CRITICAL_SECTION(cs, lock);
  // Signal a flush if the queue is over 80% full
  if (items >= length * 0.8) {
    *flush = true;
  }
  if (*insert && items < length) {
    uint32_t idx = (start + items) % length;
    volatile_write(queue[idx].csn, csn);
    volatile_write(queue[idx].start_time, start_time);
    volatile_write(items, items + 1);
    ASSERT(items == size());
    *insert = false;
  }
}

void commit_queue::extend() {
  Entry *new_queue = new Entry[length * 2];
  memcpy(new_queue, queue, sizeof(Entry) * length);
  length *= 2;
  delete[] queue;
  queue = new_queue;
}

void tls_committer::initialize(uint32_t id) {
  this->id = id;
  _commit_queue = new commit_queue();
}

uint64_t tls_committer::get_global_durable_csn() {
  bool found = false;
  uint64_t min_dirty = std::numeric_limits<uint64_t>::max();
  uint64_t max_clean = 0;
  for (uint32_t i = 0; i < ermia::dlog::tlogs.size(); i++) {
    uint64_t csn = volatile_read(_tls_durable_csn[i]);
    if (csn) {  
      if (csn & DIRTY_FLAG) {
        min_dirty = std::min(csn & ~DIRTY_FLAG, min_dirty);
        found = true;
      } else if (max_clean < csn) {
        max_clean = csn;
      }
    }
  }
  uint64_t ret = found ? min_dirty : max_clean;
  global_durable_csn.store(ret, std::memory_order_release);
  return ret;
}

void tls_committer::dequeue_committed_xcts() {
  uint64_t upto_csn = get_global_durable_csn();
  util::timer t;
  uint64_t end_time = t.get_start();
  CRITICAL_SECTION(cs, _commit_queue->lock);
  uint32_t n = volatile_read(_commit_queue->start);
  uint32_t size = _commit_queue->size();
  uint32_t dequeue = 0;
  for (uint32_t j = 0; j < size; ++j) {
    uint32_t idx = (n + j) % _commit_queue->length;
    auto &entry = _commit_queue->queue[idx];
    if (volatile_read(entry.csn) > upto_csn) {
      break;
    }
    _commit_queue->total_latency_us += end_time - entry.start_time;
    dequeue++;
  }
  _commit_queue->items -= dequeue;
  volatile_write(_commit_queue->start, (n + dequeue) % _commit_queue->length);
}

} // namespace pcommit

} // namespace ermia
