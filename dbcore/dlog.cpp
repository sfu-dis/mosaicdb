#include <atomic>
#include <dirent.h>

#include "dlog.h"
#include "sm-common.h"
#include "sm-config.h"
#include "../engine.h"
#include "../macros.h"

// io_uring code based off of examples from https://unixism.net/loti/tutorial/index.html

namespace ermia {

namespace dlog {

// Segment file name template: tlog-id-segnum
#define SEGMENT_FILE_NAME_FMT "tlog-%08x-%08x"
#define SEGMENT_FILE_NAME_BUFSZ sizeof("tlog-01234567-01234567")
char segment_name_buf[SEGMENT_FILE_NAME_BUFSZ];

std::vector<tls_log *> tlogs;

std::atomic<uint64_t> current_csn(0);

std::mutex tls_log_lock;

thread_local struct io_uring tls_read_ring;

std::thread *pcommit_thread = nullptr;
std::condition_variable pcommit_daemon_cond;
std::mutex pcommit_daemon_lock;
std::atomic<bool>pcommit_daemon_has_work(false);

void flush_all() {
  // Flush rest blocks
  for (auto &tlog : tlogs) {
    tlog->last_flush();
  }
}

void dequeue_committed_xcts() {
  std::lock_guard<std::mutex> guard(ermia::tlog_lock);
  for (auto &tlog : tlogs) {
    tlog->dequeue_committed_xcts();
  }
}

void wakeup_commit_daemon() {
  pcommit_daemon_lock.lock();
  pcommit_daemon_has_work = true;
  pcommit_daemon_lock.unlock();
  pcommit_daemon_cond.notify_all();
}

void commit_daemon() {
  auto timeout = std::chrono::milliseconds(ermia::config::pcommit_timeout_ms);
  while (!ermia::config::IsShutdown()) {
    if (pcommit_daemon_has_work) {
      dequeue_committed_xcts();
    } else {
      std::unique_lock<std::mutex> lock(pcommit_daemon_lock);
      pcommit_daemon_cond.wait_for(lock, timeout);
      pcommit_daemon_has_work = false;
    }
  }
}

void initialize() {
  if (ermia::config::pcommit_thread) {
    pcommit_thread = new std::thread(commit_daemon);
  }
}

void uninitialize() {
  if (ermia::config::pcommit_thread) {
    pcommit_thread->join();
    delete pcommit_thread;
  }
}

void tls_log::initialize(const char *log_dir, uint32_t log_id, uint32_t node,
                         uint64_t logbuf_mb, uint64_t max_segment_mb) {
  std::lock_guard<std::mutex> lock(tls_log_lock);
  dir = log_dir;
  id = log_id;
  numa_node = node;
  flushing = false;
  logbuf_size = logbuf_mb * uint32_t{1024 * 1024};
  /*
  logbuf[0] = (char *)numa_alloc_onnode(logbuf_size, numa_node);
  LOG_IF(FATAL, !logbuf[0]) << "Unable to allocate log buffer";
  logbuf[1] = (char *)numa_alloc_onnode(logbuf_size, numa_node);
  LOG_IF(FATAL, !logbuf[1]) << "Unable to allocate log buffer";
  */
  int pmret = posix_memalign((void **)&logbuf[0], PAGE_SIZE, logbuf_size);
  LOG_IF(FATAL, pmret) << "Unable to allocate log buffer";
  pmret = posix_memalign((void **)&logbuf[1], PAGE_SIZE, logbuf_size);
  LOG_IF(FATAL, pmret) << "Unable to allocate log buffer";

  segment_size = max_segment_mb * uint32_t{1024 * 1024};
  LOG_IF(FATAL, segment_size > SEGMENT_MAX_SIZE) << "Unable to allocate log buffer";

  logbuf_offset = 0;
  active_logbuf = logbuf[0];
  durable_lsn = 0;
  current_lsn = 0;

  // Create a new segment
  create_segment();
  current_segment()->start_offset = current_lsn;

  DLOG(INFO) << "Log " << id << ": new segment " << segments.size() - 1 << ", start lsn " << current_lsn;

  // Initialize io_uring
  int ret = io_uring_queue_init(2, &ring, 0);
  LOG_IF(FATAL, ret != 0) << "Error setting up io_uring: " << strerror(ret);

  // Initialize committer
  tcommitter.initialize(log_id);
}

void tls_log::uninitialize() {
  std::lock_guard<std::mutex> lg(lock);
  if (logbuf_offset) {
    uint64_t aligned_size = align_up_flush_size(logbuf_offset);
    current_lsn += (aligned_size - logbuf_offset);
    logbuf_offset = aligned_size;
    issue_flush(active_logbuf, logbuf_offset);
    poll_flush();
  }
  io_uring_queue_exit(&ring);
}

void tls_log::enqueue_flush() {
  std::lock_guard<std::mutex> lg(lock);
  if (flushing) {
    poll_flush();
    flushing = false;
  }

  if (logbuf_offset) {
    uint64_t aligned_size = align_up_flush_size(logbuf_offset);
    current_lsn += (aligned_size - logbuf_offset);
    logbuf_offset = aligned_size;
    issue_flush(active_logbuf, logbuf_offset);
    switch_log_buffers();
  }
}

void tls_log::last_flush() {
  std::lock_guard<std::mutex> lg(lock);
  if (flushing) {
    poll_flush();
    flushing = false;
  }

  if (logbuf_offset) {
    uint64_t aligned_size = align_up_flush_size(logbuf_offset);
    current_lsn += (aligned_size - logbuf_offset);
    logbuf_offset = aligned_size;
    issue_flush(active_logbuf, logbuf_offset);
    switch_log_buffers();
    poll_flush();
    flushing = false;
  }
}

// TODO(tzwang) - fd should correspond to the actual segment
void tls_log::issue_read(int fd, char *buf, uint64_t size, uint64_t offset, void *user_data) {
  thread_local bool initialized = false;
  if (unlikely(!initialized)) {
    // Initialize the tls io_uring
    int ret = io_uring_queue_init(1024, &tls_read_ring, 0);
    LOG_IF(FATAL, ret != 0) << "Error setting up io_uring: " << strerror(ret);
    initialized = true;
  }

  struct io_uring_sqe *sqe = io_uring_get_sqe(&tls_read_ring);
  LOG_IF(FATAL, !sqe);

  io_uring_prep_read(sqe, fd, buf, size, offset);
  io_uring_sqe_set_data(sqe, user_data);
  int nsubmitted = io_uring_submit(&tls_read_ring);
  LOG_IF(FATAL, nsubmitted != 1);
}

void tls_log::peek_tid(int &tid, int &ret_val) {
  struct io_uring_cqe* cqe;
  int ret = io_uring_peek_cqe(&tls_read_ring, &cqe);
  if (ret < 0) {
    if (ret == -EAGAIN) {
      // Nothing yet - caller should retry later
      return;
    } else {
      LOG(FATAL) << strerror(ret);
    }
  }

  tid = *(int *)cqe->user_data;
  ret_val = cqe->res;
  io_uring_cqe_seen(&tls_read_ring, cqe);
}

bool tls_log::peek_only(void *user_data, uint32_t read_size) {
  struct io_uring_cqe* cqe;
  int ret = io_uring_peek_cqe(&tls_read_ring, &cqe);

  if (ret < 0) {
    if (ret == -EAGAIN) {
      // Nothing yet - caller should retry later
      return false;
    } else {
      LOG(FATAL) << strerror(ret);
    }
  }

  if (*(int *)cqe->user_data != *(int *)user_data) {
    return false;
  }

  ALWAYS_ASSERT(cqe->res == read_size);
  io_uring_cqe_seen(&tls_read_ring, cqe);
  return true;
}

void tls_log::issue_flush(const char *buf, uint64_t size) {
  if (config::null_log_device) {
    durable_lsn += size;
    return;
  }

  if (flushing) {
    poll_flush();
    flushing = false;
  }

  // Issue an async I/O to flush the buffer into the current open segment
  flushing = true;

  struct io_uring_sqe *sqe = io_uring_get_sqe(&ring);
  LOG_IF(FATAL, !sqe);

  io_uring_prep_write(sqe, current_segment()->fd, buf, size, current_segment()->size);

  // Encode data size which is useful upon completion (to add to durable_lsn)
  // Must be set after io_uring_prep_write (which sets user_data to 0)
  sqe->user_data = size;
  current_segment()->expected_size += size;

  int nsubmitted = io_uring_submit(&ring);
  LOG_IF(FATAL, nsubmitted != 1);
}

void tls_log::poll_flush() {
  if(config::null_log_device) {
    return;
  }

  struct io_uring_cqe *cqe = nullptr;
  while (1) {
    int ret = io_uring_peek_cqe(&ring, &cqe);
    if (ret < 0) {
      if (ret == -EAGAIN) {
        // Nothing yet
        continue;
      } else {
        LOG(FATAL) << strerror(ret);
      }
    }
    break;
  }
  LOG_IF(FATAL, cqe->res < 0) << "Error in async operation: " << strerror(-cqe->res);
  uint64_t size = cqe->user_data;
  io_uring_cqe_seen(&ring, cqe);
  durable_lsn += size;
  current_segment()->size += size;

  // get last tls durable csn
  uint64_t last_tls_durable_csn =
      (active_logbuf == logbuf[0]) ? last_csns[1] : last_csns[0];

  // set tls durable csn
  tcommitter.set_tls_durable_csn(last_tls_durable_csn);
  ALWAYS_ASSERT(tcommitter.get_tls_durable_csn() == last_tls_durable_csn);

  if (ermia::config::pcommit_thread) {
    dlog::wakeup_commit_daemon();
  } else {
    dequeue_committed_xcts();
  }
}

void tls_log::create_segment() {
  size_t n = snprintf(segment_name_buf, sizeof(segment_name_buf), SEGMENT_FILE_NAME_FMT, id, (unsigned int)segments.size());
  DIR *logdir = opendir(dir);
  ALWAYS_ASSERT(logdir);
  segments.emplace_back(dirfd(logdir), segment_name_buf, ermia::config::log_direct_io);
}

/*
void tls_log::insert(log_block *block) {
  if (block->total_size() + logbuf_offset > logbuf_size) {
    issue_flush(active_logbuf, logbuf_offset);
    active_logbuf = (active_logbuf == logbuf[0]) ? logbuf[1] : logbuf[0];
    logbuf_offset = 0;
  }
  memcpy(active_logbuf + logbuf_offset, block, block->total_size());
  logbuf_offset += block->total_size();
  current_lsn += block->total_size();
}
*/

uint64_t tls_log::align_up_flush_size(uint64_t size) {
  // Pad to PAGE_SIZE boundary if dio is enabled
  if (ermia::config::log_direct_io) {
    size = align_up(size, PAGE_SIZE);
    LOG_IF(FATAL, size > logbuf_size) << "Aligned log buffer data size > log buffer size";
  }
  return size;
}

log_block *tls_log::allocate_log_block(uint32_t payload_size,
                                       uint64_t *out_cur_lsn,
                                       uint64_t *out_seg_num,
                                       uint64_t block_csn) {
  if (payload_size == 0) {
    return nullptr;
  }

  std::lock_guard<std::mutex> lg(lock);
  tcommitter.set_dirty_flag();

  uint32_t alloc_size = payload_size + sizeof(log_block);
  LOG_IF(FATAL, alloc_size > logbuf_size) << "Total size too big";


  // If this allocated log block would span across segments, we need a new segment.
  bool create_new_segment = false;
  if (alloc_size + logbuf_offset + current_segment()->expected_size > segment_size) {
    create_new_segment = true;
  }

  // If the allocated size exceeds the available space in the active logbuf,
  // or we need to create a new segment for this log block,
  // flush the active logbuf, and switch to the other logbuf.
  if (alloc_size + logbuf_offset > logbuf_size || create_new_segment) {
    if (logbuf_offset) {
      // Pad to 4K boundary if dio is enabled
      uint64_t aligned_size = align_up_flush_size(logbuf_offset);
      current_lsn += (aligned_size - logbuf_offset);
      logbuf_offset = aligned_size;
      issue_flush(active_logbuf, logbuf_offset);
      switch_log_buffers();
    }

    if (create_new_segment) {
      create_segment();
      current_segment()->start_offset = current_lsn;
    }
  }

  log_block *lb = (log_block *)(active_logbuf + logbuf_offset);
  logbuf_offset += alloc_size;
  if (out_cur_lsn) {
    *out_cur_lsn = current_lsn;
  }
  current_lsn += alloc_size;

  if (out_seg_num) {
    *out_seg_num = segments.size() - 1;
  }

  // Store the latest csn of log block
  latest_csn = block_csn;

  if (active_logbuf == logbuf[0]) {
    last_csns[0] = block_csn;
  } else {
    last_csns[1] = block_csn;
  }

  new (lb) log_block(payload_size);

  // CSN must be made available here - after this the caller will start to
  // use the block to populate individual log records into the block, and
  // each log record needs to carry a CSN so that during recovery/read op
  // the newly instantiated record can directly be filled out with its CSN
  lb->csn = block_csn;
  return lb;
}

void tls_log::enqueue_committed_xct(uint64_t csn) {
  bool flush = false;
  bool insert = true;
  uint count = 0;
retry :
  if (flush) {
    for (uint i = 0; i < tlogs.size(); ++i) {
      tls_log *tlog = tlogs[i];
      if (tlog &&  volatile_read(pcommit::_tls_durable_csn[i])) {
        tlog->enqueue_flush();
      }
    }

    if (ermia::config::pcommit_thread) {
      dlog::wakeup_commit_daemon();
    } else {
      dequeue_committed_xcts();
    }

    flush = false;
  }
  tcommitter.enqueue_committed_xct(csn, &flush, &insert);
  if (count >= 10) {
    tcommitter.extend_queue();
    tcommitter.enqueue_committed_xct(csn, &flush, &insert);
    count = 0;
  }
  if (flush) {
    count++;
    goto retry;
  }
}

segment::segment(int dfd, const char *segname, bool dio) : size(0), expected_size(0) {
  int flags = dio ? O_DIRECT : 0;
  flags |= (O_RDWR | O_CREAT | O_TRUNC);
  fd = openat(dfd, segname, flags, 0644);
  LOG_IF(FATAL, fd < 0);
}

segment::~segment() {
  if (fd >= 0) {
    close(fd);
  }
}

}  // namespace dlog

}  // namespace ermia
