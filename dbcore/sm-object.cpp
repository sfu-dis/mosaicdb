#include "sm-alloc.h"
#include "sm-object.h"
#include "../tuple.h"
#include "../engine.h"

namespace ermia {

// Dig out the payload from the durable log
// ptr should point to some position in the log and its size_code should refer
// to only data size (i.e., the size of the payload of dbtuple rounded up).
// Returns a fat_ptr to the object created
PROMISE(void) Object::Pin(transaction *xct) {
  uint32_t status = volatile_read(status_);
  if (status != kStatusStorage) {
    if (status == kStatusLoading) {
      while (volatile_read(status_) != kStatusMemory) {
        SUSPEND;
      }
      // We need to return immediately here, otherwise another thread might
      // try to pin the same object and modify this status to kStatusLoading.
      RETURN;
    }
    ALWAYS_ASSERT(volatile_read(status_) == kStatusMemory ||
                  volatile_read(status_) == kStatusDeleted);
    RETURN;
  }

  // Try to 'lock' the status
  // TODO(tzwang): have the thread do something else while waiting?
  uint32_t val =
      __sync_val_compare_and_swap(&status_, kStatusStorage, kStatusLoading);
  if (val == kStatusMemory) {
    RETURN;
  } else if (val == kStatusLoading) {
    while (volatile_read(status_) != kStatusMemory) {}
    RETURN;
  } else {
    ASSERT(val == kStatusStorage);
    ASSERT(volatile_read(status_) == kStatusLoading);
  }

  // Now we can load it from the durable log
  uint32_t final_status = kStatusMemory;
  ALWAYS_ASSERT(pdest_._ptr);
  uint16_t where = pdest_.asi_type();
  ALWAYS_ASSERT(where == fat_ptr::ASI_LOG || where == fat_ptr::ASI_CHK);

  AWAIT Object::LoadFromStorage(xct, pdest_, this);

  // Could be a delete
  if (((dbtuple *)GetPayload())->size == 0) {
    final_status = kStatusDeleted;
    ASSERT(next_pdest_.offset());
  }

  ASSERT(volatile_read(status_) == kStatusLoading);
  SetStatus(final_status);
}

void Object::SyncPin(transaction *xct) {
  uint32_t status = volatile_read(status_);
  if (status != kStatusStorage) {
    if (status == kStatusLoading) {
      while (volatile_read(status_) != kStatusMemory) {}
      // We need to return immediately here, otherwise another thread might
      // try to pin the same object and modify this status to kStatusLoading.
      return;
    }
    ALWAYS_ASSERT(volatile_read(status_) == kStatusMemory ||
                  volatile_read(status_) == kStatusDeleted);
    return;
  }

  // Try to 'lock' the status
  // TODO(tzwang): have the thread do something else while waiting?
  uint32_t val = __sync_val_compare_and_swap(&status_, kStatusStorage, kStatusLoading);
  if (val == kStatusMemory) {
    return;
  } else if (val == kStatusLoading) {
    while (volatile_read(status_) != kStatusMemory) {}
    return;
  } else {
    ASSERT(val == kStatusStorage);
    ASSERT(volatile_read(status_) == kStatusLoading);
  }

  // Now we can load it from the durable log
  uint32_t final_status = kStatusMemory;
  ALWAYS_ASSERT(pdest_._ptr);
  uint16_t where = pdest_.asi_type();
  ALWAYS_ASSERT(where == fat_ptr::ASI_LOG || where == fat_ptr::ASI_CHK);

  Object::SyncLoadFromStorage(xct, pdest_, this);

  // Could be a delete
  if (((dbtuple *)GetPayload())->size == 0) {
    final_status = kStatusDeleted;
    ASSERT(next_pdest_.offset());
  }

  ASSERT(volatile_read(status_) == kStatusLoading);
  SetStatus(final_status);
}

fat_ptr Object::Create(const varstr *tuple_value, epoch_num epoch) {
  // Calculate tuple size
  const uint32_t data_sz = tuple_value ? tuple_value->size() : 0;
  size_t alloc_sz = sizeof(dbtuple) + sizeof(Object) + data_sz;

  // Allocate a version
  Object *obj = new (MM::allocate(alloc_sz)) Object();
  // In case we got it from the tls reuse pool
  ASSERT(obj->GetAllocateEpoch() <= epoch - 4);
  obj->SetAllocateEpoch(epoch);

  // Tuple setup
  dbtuple *tuple = (dbtuple *)obj->GetPayload();
  new (tuple) dbtuple(data_sz);
  if (tuple_value) {
    memcpy(tuple->get_value_start(), tuple_value->p, data_sz);
  }

  size_t size_code = encode_size_aligned(alloc_sz);
  ASSERT(size_code != INVALID_SIZE_CODE);
  return fat_ptr::make(obj, size_code, 0 /* 0: in-memory */);
}

// Make sure the object has a valid csn/pdest
fat_ptr Object::GenerateCsnPtr(uint64_t csn) {
  fat_ptr csn_ptr = CSN::make(csn).to_ptr();
  uint16_t s = csn_ptr.asi_type();
  ASSERT(s == fat_ptr::ASI_CSN);
  return csn_ptr;
}

// Static function to load the tuple from the log (storage)
PROMISE(dbtuple *) Object::LoadFromStorage(transaction *xct, fat_ptr pdest, Object *object) {
  size_t data_sz = decode_size_aligned(pdest.size_code());
  dbtuple *tuple = nullptr;

  if (pdest.asi_type() == fat_ptr::ASI_LOG) {
    LSN lsn = LSN::from_ptr(pdest);
    auto *log = GetLog(lsn.logid());

    ASSERT(pdest.log_segment() == lsn.segment());
    ASSERT(lsn.segment() >= 0 && lsn.segment() <= NUM_LOG_SEGMENTS);
    auto *segment = log->get_segment(lsn.segment());
    ASSERT(segment);

    uint64_t offset_in_seg = lsn.loffset() - segment->start_offset;

    uint64_t offset = offset_in_seg;
    size_t read_size = data_sz;

    // Align to PAGE_SIZE if direct IO is enabled and read up entire 4KB chunks
    if (config::log_direct_io) {
      read_size = align_up((offset_in_seg & (PAGE_SIZE - 1)) + data_sz, PAGE_SIZE);
      offset = align_down(offset_in_seg, PAGE_SIZE);
    }

    // Allocate temporary buffer space
    char *buffer = xct->string_allocator().next_raw_aligned(PAGE_SIZE, read_size);

    xct->set_expected_io_size(read_size);
    // Now issue the read
    if (config::iouring_read_log) {
      log->issue_read(segment->fd, buffer, read_size, offset, (void *)xct->get_user_data());
      xct->set_cold(true);
      if (unlikely(ermia::config::IsLoading()) || ermia::config::read_txn_type == "sequential"
                                              || ermia::config::read_txn_type == "tpcc-sequential") {
        while (!log->peek_only((void *)xct->get_user_data(), read_size)) {
          SUSPEND;
        }
      } else {
        SUSPEND;
      }
      xct->set_cold(false);
    } else {
      size_t m = pread(segment->fd, buffer, read_size, offset);
      LOG_IF(FATAL, m != read_size) << "Error: read " << m << " out of " << read_size << " bytes of data, record size=" << data_sz;
    }

    size_t alloc_sz = sizeof(dbtuple) + sizeof(Object) + data_sz;
    if (!object) {
      // No object given - get one from arena. For now this is only for digging
      // out tuples from the cold store; other accesses should provide an object
      // that lives on the heap.

      object = (Object *)xct->string_allocator().next_raw(alloc_sz);
    }

    // Copy the entire dbtuple including dbtuple header and data
    dlog::log_record *logrec = (dlog::log_record *)&buffer[offset_in_seg - offset];
    tuple = (dbtuple *)object->GetPayload();
    new (tuple) dbtuple(0);  // set the correct size later
    memcpy(tuple, &logrec->data[0], sizeof(dbtuple) + ((dbtuple *)logrec->data)->size);
    // TODO: this assertion does not work with TPC-C
    //ASSERT(*tuple->get_value_start() == 'a');

    // Set CSN
    fat_ptr csn_ptr = GenerateCsnPtr(logrec->csn);
    object->SetCSN(csn_ptr);
    ASSERT(object->GetCSN().asi_type() == fat_ptr::ASI_CSN);
  } else {
    ALWAYS_ASSERT(0);
    // TODO(tzwang): Load tuple data form the chkpt file
  }

  RETURN tuple;
}

dbtuple* Object::SyncLoadFromStorage(transaction *xct, fat_ptr pdest, Object *object) {
  size_t data_sz = decode_size_aligned(pdest.size_code());
  dbtuple *tuple = nullptr;

  if (pdest.asi_type() == fat_ptr::ASI_LOG) {
    LSN lsn = LSN::from_ptr(pdest);
    auto *log = GetLog(lsn.logid());

    ASSERT(pdest.log_segment() == lsn.segment());
    ASSERT(lsn.segment() >= 0 && lsn.segment() <= NUM_LOG_SEGMENTS);
    auto *segment = log->get_segment(lsn.segment());
    ASSERT(segment);

    uint64_t offset_in_seg = lsn.loffset() - segment->start_offset;

    uint64_t offset = offset_in_seg;
    size_t read_size = data_sz;

    // Align to PAGE_SIZE if direct IO is enabled and read up entire 4KB chunks
    if (config::log_direct_io) {
      read_size = align_up((offset_in_seg & (PAGE_SIZE - 1)) + data_sz, PAGE_SIZE);
      offset = align_down(offset_in_seg, PAGE_SIZE);
    }

    // Allocate temporary buffer space
    char *buffer = xct->string_allocator().next_raw_aligned(PAGE_SIZE, read_size);

    xct->set_expected_io_size(read_size);
    // Now issue the read
    if (config::iouring_read_log) {
      log->issue_read(segment->fd, buffer, read_size, offset, (void *)xct->get_user_data());
      xct->set_cold(true);
      while (!log->peek_only((void *)xct->get_user_data(), read_size)) {}
      xct->set_cold(false);
    } else {
      size_t m = pread(segment->fd, buffer, read_size, offset);
      LOG_IF(FATAL, m != read_size) << "Error: read " << m << " out of " << read_size << " bytes of data, record size=" << data_sz;
    }

    size_t alloc_sz = sizeof(dbtuple) + sizeof(Object) + data_sz;
    if (!object) {
      // No object given - get one from arena. For now this is only for digging
      // out tuples from the cold store; other accesses should provide an object
      // that lives on the heap.

      object = (Object *)xct->string_allocator().next_raw(alloc_sz);
    }

    // Copy the entire dbtuple including dbtuple header and data
    dlog::log_record *logrec = (dlog::log_record *)&buffer[offset_in_seg - offset];
    tuple = (dbtuple *)object->GetPayload();
    new (tuple) dbtuple(0);  // set the correct size later
    memcpy(tuple, &logrec->data[0], sizeof(dbtuple) + ((dbtuple *)logrec->data)->size);
    // TODO: this assertion does not work with TPC-C
    //ASSERT(*tuple->get_value_start() == 'a');

    // Set CSN
    fat_ptr csn_ptr = GenerateCsnPtr(logrec->csn);
    object->SetCSN(csn_ptr);
    ASSERT(object->GetCSN().asi_type() == fat_ptr::ASI_CSN);
  } else {
    ALWAYS_ASSERT(0);
    // TODO(tzwang): Load tuple data form the chkpt file
  }

  return tuple;
}

ermia::coro::task<dbtuple *> Object::TaskLoadFromStorage(transaction *xct, fat_ptr pdest, Object *object) {
  size_t data_sz = decode_size_aligned(pdest.size_code());
  dbtuple *tuple = nullptr;

  if (pdest.asi_type() == fat_ptr::ASI_LOG) {
    LSN lsn = LSN::from_ptr(pdest);
    auto *log = GetLog(lsn.logid());

    ASSERT(pdest.log_segment() == lsn.segment());
    ASSERT(lsn.segment() >= 0 && lsn.segment() <= NUM_LOG_SEGMENTS);
    auto *segment = log->get_segment(lsn.segment());
    ASSERT(segment);

    uint64_t offset_in_seg = lsn.loffset() - segment->start_offset;

    uint64_t offset = offset_in_seg;
    size_t read_size = data_sz;

    // Align to PAGE_SIZE if direct IO is enabled and read up entire 4KB chunks
    if (config::log_direct_io) {
      read_size = align_up((offset_in_seg & (PAGE_SIZE - 1)) + data_sz, PAGE_SIZE);
      offset = align_down(offset_in_seg, PAGE_SIZE);
    }

    // Allocate temporary buffer space
    char *buffer = xct->string_allocator().next_raw_aligned(PAGE_SIZE, read_size);

    xct->set_expected_io_size(read_size);
    // Now issue the read
    if (config::iouring_read_log) {
      log->issue_read(segment->fd, buffer, read_size, offset, (void *)xct->get_user_data());
      xct->set_cold(true);
      if (unlikely(ermia::config::IsLoading()) || ermia::config::read_txn_type == "sequential"
                                              || ermia::config::read_txn_type == "tpcc-sequential") {
        while (!log->peek_only((void *)xct->get_user_data(), read_size)) {
          co_await suspend_always{};
        }
      } else {
        co_await suspend_always{};
      }
      xct->set_cold(false);
    } else {
      size_t m = pread(segment->fd, buffer, read_size, offset);
      LOG_IF(FATAL, m != read_size) << "Error: read " << m << " out of " << read_size << " bytes of data, record size=" << data_sz;
    }

    size_t alloc_sz = sizeof(dbtuple) + sizeof(Object) + data_sz;
    if (!object) {
      // No object given - get one from arena. For now this is only for digging
      // out tuples from the cold store; other accesses should provide an object
      // that lives on the heap.

      object = (Object *)xct->string_allocator().next_raw(alloc_sz);
    }

    // Copy the entire dbtuple including dbtuple header and data
    dlog::log_record *logrec = (dlog::log_record *)&buffer[offset_in_seg - offset];
    tuple = (dbtuple *)object->GetPayload();
    new (tuple) dbtuple(0);  // set the correct size later
    memcpy(tuple, &logrec->data[0], sizeof(dbtuple) + ((dbtuple *)logrec->data)->size);
    // TODO: this assertion does not work with TPC-C
    //ASSERT(*tuple->get_value_start() == 'a');

    // Set CSN
    fat_ptr csn_ptr = GenerateCsnPtr(logrec->csn);
    object->SetCSN(csn_ptr);
    ASSERT(object->GetCSN().asi_type() == fat_ptr::ASI_CSN);
  } else {
    ALWAYS_ASSERT(0);
    // TODO(tzwang): Load tuple data form the chkpt file
  }

  co_return tuple;
}
}  // namespace ermia
