#pragma once

#include <list>

#include "dlog.h"
#include "dlog-defs.h"
#include "epoch.h"
#include "sm-common.h"
#include "sm-config.h"
#include "sm-coroutine.h"
#include "../varstr.h"
#include "xid.h"


namespace ermia {

struct dbtuple;
struct sm_log_recover_mgr;

class Object {
 private:
  typedef epoch_mgr::epoch_num epoch_num;
  static const uint32_t kStatusMemory = 1;
  static const uint32_t kStatusStorage = 2;
  static const uint32_t kStatusLoading = 3;
  static const uint32_t kStatusDeleted = 4;

  // alloc_epoch_ and status_ must be the first two fields

  // When did we create this object?
  epoch_num alloc_epoch_;

  // Where exactly is the payload?
  uint32_t status_;

  // The object's permanent home in the log/chkpt
  fat_ptr pdest_;

  // The permanent home of the older version that's overwritten by me
  fat_ptr next_pdest_;

  // Volatile pointer to the next older version that's in memory.
  // There might be a gap between the versions represented by next_pdest_
  // and next_volatile_.
  fat_ptr next_volatile_;

  // Commit timestamp of this version. Type is XID (CSN) before (after)
  // commit.
  fat_ptr csn_;

 public:
  static fat_ptr Create(const varstr* tuple_value, epoch_num epoch);

  Object()
      : alloc_epoch_(0),
        status_(kStatusMemory),
        pdest_(NULL_PTR),
        next_pdest_(NULL_PTR),
        next_volatile_(NULL_PTR) {}

  Object(fat_ptr pdest, fat_ptr next, epoch_num e, bool in_memory)
      : alloc_epoch_(e),
        status_(in_memory ? kStatusMemory : kStatusStorage),
        pdest_(pdest),
        next_pdest_(next),
        next_volatile_(NULL_PTR) {}

  inline bool IsDeleted() { return status_ == kStatusDeleted; }
  inline bool IsInMemory() { return status_ == kStatusMemory; }
  inline fat_ptr* GetPersistentAddressPtr() { return &pdest_; }
  inline fat_ptr GetPersistentAddress() { return pdest_; }
  inline void SetPersistentAddress(fat_ptr ptr) { pdest_._ptr = ptr._ptr; }
  inline fat_ptr GetCSN() { return csn_; }
  inline void SetCSN(fat_ptr csnptr) { volatile_write(csn_._ptr, csnptr._ptr); }
  inline fat_ptr GetNextPersistent() { return volatile_read(next_pdest_); }
  inline fat_ptr* GetNextPersistentPtr() { return &next_pdest_; }
  inline fat_ptr GetNextVolatile() { return volatile_read(next_volatile_); }
  inline fat_ptr* GetNextVolatilePtr() { return &next_volatile_; }
  inline void SetNextPersistent(fat_ptr next) { volatile_write(next_pdest_, next); }
  inline void SetNextVolatile(fat_ptr next) { volatile_write(next_volatile_, next); }
  inline epoch_num GetAllocateEpoch() { return alloc_epoch_; }
  inline void SetAllocateEpoch(epoch_num e) { alloc_epoch_ = e; }
  inline char* GetPayload() { return (char*)((char*)this + sizeof(Object)); }
  inline void SetStatus(uint32_t s) { volatile_write(status_, s); }
  inline PROMISE(dbtuple*) GetPinnedTuple(transaction *xct) {
    if (IsDeleted()) {
      RETURN nullptr;
    }
    AWAIT Pin(xct);
    RETURN (dbtuple*)GetPayload();
  }
  inline dbtuple* SyncGetPinnedTuple(transaction *xct) {
    if (IsDeleted()) {
      return nullptr;
    }
    SyncPin(xct);
    return (dbtuple*)GetPayload();
  }

  static fat_ptr GenerateCsnPtr(uint64_t csn);
  static PROMISE(dbtuple *) LoadFromStorage(transaction *xct, fat_ptr pdest, Object *object);
  static dbtuple* SyncLoadFromStorage(transaction *xct, fat_ptr pdest, Object *object);
  static ermia::coro::task<dbtuple *> TaskLoadFromStorage(transaction *xct, fat_ptr pdest, Object *object);
  PROMISE(void) Pin(transaction *xct);  // Make sure the payload is in memory
  void SyncPin(transaction *xct);

  static inline void PrefetchHeader(Object *p) {
    uint32_t i = 0;
    do {
      ::prefetch((const char *)(p + i));
      i += CACHE_LINE_SIZE;
    } while (i < sizeof(Object));
  }

};
}  // namespace ermia
