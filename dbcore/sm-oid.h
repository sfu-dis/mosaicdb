#pragma once

#include "epoch.h"
#include "sm-common.h"
#include "sm-oid-alloc.h"
#include "sm-coroutine.h"

#include "dynarray.h"

#include "../macros.h"
#include "../tuple.h"

namespace ermia {

typedef epoch_mgr::epoch_num epoch_num;

/* OID arrays and allocators alike always occupy an integer number
   of dynarray pages, to ensure that we don't hit precision issues
   when saving dynarray contents to (and restoring from)
   disk. It's also more than big enough for the objects to reach
   full size
 */
static size_t const SZCODE_ALIGN_BITS = dynarray::page_bits();

/* An OID is essentially an array of fat_ptr. The only bit of
   magic is that it embeds the dynarray that manages the storage
   it occupies.
 */
struct oid_array {
  static size_t const MAX_SIZE = sizeof(fat_ptr) << 32;
  static uint64_t const MAX_ENTRIES =
      (size_t(1) << 32) - sizeof(dynarray) / sizeof(fat_ptr);
  static size_t const ENTRIES_PER_PAGE =
      (sizeof(fat_ptr) << SZCODE_ALIGN_BITS) / 2;

  /* How much space is required for an array with [n] entries?
   */
  static size_t alloc_size(size_t n = MAX_ENTRIES) {
    return OFFSETOF(oid_array, _entries[n]);
  }

  static fat_ptr make();

  static dynarray make_oid_dynarray() {
    return dynarray(oid_array::alloc_size(), 128 * config::MB);
  }

  static void destroy(oid_array *oa);

  oid_array(dynarray &&owner);

  // unsafe!
  oid_array() = delete;
  oid_array(oid_array const &) = delete;
  void operator=(oid_array) = delete;

  /* Return the number of entries this OID array currently holds.
   */
  inline size_t nentries() { return _backing_store.size() / sizeof(fat_ptr); }

  /* Make sure the backing store holds at least [n] entries.
   */
  void ensure_size(size_t n);

  /* Return a pointer to the given OID's slot.

     WARNING: The caller is responsible for handling races in
     case multiple threads try to update the slot concurrently.

     WARNING: this function does not perform bounds
     checking. The caller is responsible to use nentries() and
     ensure_size() as needed.
   */
  fat_ptr *get(OID o) { return &_entries[o]; }

  dynarray _backing_store;
  fat_ptr _entries[];
};

struct OIDAMACState {
  OID oid;
  int stage;
  dbtuple *tuple;
  bool done;

  fat_ptr ptr;
  Object *cur_obj;
  Object *prev_obj;
  fat_ptr tentative_next;

  OIDAMACState(OID oid)
  : oid(oid)
  , stage(0)
  , tuple(nullptr)
  , done(false)
  , ptr(NULL_PTR)
  , cur_obj(nullptr)
  , prev_obj(nullptr)
  , tentative_next(NULL_PTR)
  {}
};

class sm_oid_mgr {
public:
  friend class sm_chkpt_mgr;

  /* The object array for each file resides in the OID array for
     file 0; allocators go in file 1 (including the file allocator,
     which predictably resides at OID 0). We don't attempt to store
     the file level object array at entry 0 of itself, though.
   */
  static FID const OBJARRAY_FID = 0;
  static FID const ALLOCATOR_FID = 1;

  /* Metadata for any allocated file can be stored in this file at
     the OID that matches its FID.
   */
  static FID const METADATA_FID = 2;

private:
  static FID const FIRST_FREE_FID = 3;
  static size_t const MUTEX_COUNT = 256;

public:
  /* Create a new OID manager.

     NOTE: the scan must be positioned at the first record of the
     checkpoint transaction (or the location where the record would
     be if the checkpoint is empty). When this function returns, the
     scan will be positioned at whatever follows the OID checkpoint
     (or invalid, if there are no more records).

     tzwang: above is the orignal interface design. The implementation
     here is to checkpoint the OID arrays to an individual file, whose
     name is in the format of "chd-[chkpt-start-LSN]"; after successfully
     written this file, we use the [chkpt start LSN, chkpt end LSN] pair
     as an empty file's filename to denote this chkpt was successful.
     sm_oid_mgr::create() then accepts the chkpt start LSN to know which
     chkpt file to look for, and recovers from the chkpt file, followed by
     a log scan (if needed). The chkpt files are stored in the log dir.

     The separation of the chkpt from the log reduces interference to
     normal transaction processing during checkpointing; storing the
     chkpt file in a separate file, instead of in the log, reduces the
     amount of data to ship for replication (no need to ship chkpts,
     the backup can have its own chkpts).
   */
  static void create();

  /* Record a snapshot of the OID manager's state as part of a
     checkpoint. The data will be durable by the time this function
     returns, but will only be reachable if the checkpoint
     transaction commits and its location is properly recorded.
   */
  void Checkpoint();

  /* Create a new file and return its FID. If [needs_alloc]=true,
     the new file will be managed by an allocator and its FID can be
     passed to alloc_oid(); otherwise, the file is either unmanaged
     or a slave to some other file.
   */
  FID create_file(bool needs_alloc = true);

  /* Destroy file [f] and remove its contents. Its allocator, if
     any, will also be removed.

     The caller is responsible to destroy any "slave" files that
     depended on this one, and to remove the file's metadata entry
     (if any).

     WARNING: the caller is responsible to ensure that this file is
     no longer in use by other threads.
   */
  void destroy_file(FID f);

  /* Allocate a new OID in file [f] and return it.

     Throw fid_is_full if no more OIDs are available for allocation
     in this file.

     WARNING: This is a volatile operation. In the event of a crash,
     the OID will be freed unless recorded in some way (usually an
     insert log record).

  */
  OID alloc_oid(FID f);

  /* Free an OID and return its contents (which should be disposed
     of by the caller as appropriate).

     WARNING: This is a volatile operation. In the event of a crash,
     the OID will be freed unless recorded in some way (usually a
     delete log record).
  */
  fat_ptr free_oid(FID f, OID o);

  /* Retrieve the raw contents of the specified OID. The returned
     fat_ptr may reference memory or disk.
   */
  fat_ptr oid_get(FID f, OID o) { return *oid_access(f, o); }
  fat_ptr *oid_get_ptr(FID f, OID o) { return oid_access(f, o); }
  inline fat_ptr oid_get(oid_array *oa, OID o) { return *oa->get(o); }
  inline fat_ptr *oid_get_ptr(oid_array *oa, OID o) { return oa->get(o); }

  /* Update the contents of the specified OID. The fat_ptr may
     reference memory or disk (or be NULL).
   */
  void oid_put(FID f, OID o, fat_ptr p) { *oid_access(f, o) = p; }
  inline void oid_put(oid_array *oa, OID o, fat_ptr p) {
    auto *ptr = oa->get(o);
    *ptr = p;
  }
  void oid_put_new(FID f, OID o, fat_ptr p) {
    auto *entry = oid_access(f, o);
    ALWAYS_ASSERT(*entry == NULL_PTR);
    *entry = p;
  }
  inline void oid_put_new(oid_array *oa, OID o, fat_ptr p) {
    auto *entry = oa->get(o);
    ASSERT(*entry == NULL_PTR);
    *entry = p;
  }
  void oid_put_new_if_absent(FID f, OID o, fat_ptr p) {
    auto *entry = oid_access(f, o);
    if (*entry == NULL_PTR) {
      *entry = p;
    }
  }

  /* Return a fat_ptr to the overwritten object (could be an in-flight version!)
   */
  fat_ptr UpdateTuple(oid_array *oa, OID o, const varstr *value,
                      TXN::xid_context *updater_xc, fat_ptr *new_obj_ptr);
  inline fat_ptr UpdateTuple(FID f, OID o, const varstr *value,
                             TXN::xid_context *updater_xc, fat_ptr *new_obj_ptr) {
    return UpdateTuple(get_array(f), o, value, updater_xc, new_obj_ptr);
  }

  PROMISE(dbtuple *) oid_get_version(FID f, OID o, TXN::xid_context *visitor_xc) {
    ASSERT(f);
    return oid_get_version(get_array(f), o, visitor_xc);
  }
  PROMISE(dbtuple *) oid_get_version(oid_array *oa, OID o, TXN::xid_context *visitor_xc);
  PROMISE(dbtuple *) oid_get_cold_tuple(oid_array *oa, OID o, TXN::xid_context *visitor_xc);

  PROMISE(void) oid_get_version_amac(oid_array *oa,
                            std::vector<OIDAMACState> &requests,
                            TXN::xid_context *visitor_xc);

  /* Helper function for oid_get_version to test visibility. Returns true if the
   * version ([object]) is visible to the given transaction ([xc]). Sets [retry]
   * to true if the caller needs to retry the search from the head of the chain.
   */
  bool TestVisibility(Object *object, TXN::xid_context *xc, bool &retry);

  void oid_check_phantom(TXN::xid_context *visitor_xc, uint64_t vcstamp);

  inline Object *oid_get_latest_object(oid_array *oa, OID o) {
    auto head_offset = oa->get(o)->offset();
    if (head_offset) {
      return (Object *)head_offset;
    }
    return NULL;
  }

  inline dbtuple *oid_get_latest_version(oid_array *oa, OID o) {
    auto head_offset = oa->get(o)->offset();
    if (head_offset) return (dbtuple *)((Object *)head_offset)->GetPayload();
    return NULL;
  }

  inline dbtuple *oid_get_latest_version(FID f, OID o) {
    return oid_get_latest_version(get_array(f), o);
  }

  inline void UnlinkTuple(oid_array *oa, OID o) {
    // Now the head is guaranteed to be the only dirty version
    // because we unlink the overwritten dirty version in put,
    // essentially this function ditches the head directly.
    // Otherwise use the commented out old code.
    UnlinkTuple(oa->get(o));
  }
  inline void UnlinkTuple(fat_ptr *ptr) {
    Object *head_obj = (Object *)ptr->offset();
    // using a CAS is overkill: head is guaranteed to be the (only) dirty
    // version
    volatile_write(ptr->_ptr, head_obj->GetNextVolatile()._ptr);
    __sync_synchronize();
    // tzwang: The caller is responsible for deallocate() the head version
    // got unlinked - a update of own write will record the unlinked version
    // in the transaction's write-set, during abortor commit the version's
    // pvalue needs to be examined. So UnlinkTuple() shouldn't
    // deallocate()
    // here. Instead, the transaction does it in during commit or abort.
  }

  void recreate_file(FID f);              // for recovery only
  void recreate_allocator(FID f, OID m);  // for recovery only

  static void warm_up();
  void start_warm_up();

  sm_allocator *get_allocator(FID f) {
    // TODO: allow allocators to be paged out
    sm_allocator *alloc = oid_get(ALLOCATOR_FID, f);
    THROW_IF(not alloc, illegal_argument, "No allocator for FID %d", f);
    return alloc;
  }
  inline oid_array *get_array(FID f) {
    // TODO: allow allocators to be paged out
    oid_array *oa = (oid_array *)files->get(f)->offset();
    THROW_IF(not oa, illegal_argument, "No such file: %d", f);
    return oa;
  }
  inline void lock_file(FID f) { mutexen[f % MUTEX_COUNT].lock(); }
  inline void unlock_file(FID f) { mutexen[f % MUTEX_COUNT].unlock(); }
  inline fat_ptr *oid_access(FID f, OID o) { return get_array(f)->get(o); }
  inline bool file_exists(FID f) { return files->get(f)->offset(); }

private:
  int dfd;  // dir for storing OID chkpt data file

  /* And here they all are! */
  oid_array *files;

  /* Plus some mutexen to protect them. We don't need one per
     allocator, but we do want enough that false sharing is
     unlikely.
   */
  std::mutex mutexen[MUTEX_COUNT];

public:
  sm_oid_mgr();
  ~sm_oid_mgr();
};

extern sm_oid_mgr *oidmgr;
}  // namespace ermia
