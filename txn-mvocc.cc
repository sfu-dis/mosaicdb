#ifdef MVOCC

#include "macros.h"
#include "txn.h"
#include "dbcore/rcu.h"
#include "dbcore/serial.h"
#include "engine.h"

namespace ermia {
 
rc_t transaction::mvocc_commit() {
  ASSERT(log);
  // get clsn, abort if failed
  xc->end = log->pre_commit().offset();
  if (xc->end == 0) {
    return rc_t{RC_ABORT_INTERNAL};
  }

  if (config::phantom_prot && !MasstreeCheckPhantom()) {
    return rc_t{RC_ABORT_PHANTOM};
  }

  // Just need to check read-set
  for (uint32_t i = 0; i < read_set.size(); ++i) {
    auto &r = read_set[i];
  check_backedge:
    fat_ptr successor_clsn = volatile_read(r->sstamp);
    if (!successor_clsn.offset()) {
      continue;
    }

    // Already overwritten, see if this is a back-edge, i.e., committed before
    // me
    if (successor_clsn.asi_type() == fat_ptr::ASI_LOG) {
      if (successor_clsn.offset() < xc->end) {
        return rc_t{RC_ABORT_SERIAL};
      }
    } else {
      XID successor_xid = XID::from_ptr(successor_clsn);
      TXN::xid_context *successor_xc = TXN::xid_get_context(successor_xid);
      if (!successor_xc) {
        goto check_backedge;
      }
      if (volatile_read(successor_xc->owner) == xc->owner) {  // myself
        continue;
      }
      auto successor_state = volatile_read(successor_xc->state);
      if (!successor_xc->verify_owner(successor_xid)) {
        goto check_backedge;
      }
      if (successor_state == TXN::TXN_ACTIVE) {
        // Not yet in pre-commit, skip
        continue;
      }
      // Already in pre-commit or committed, definitely has (or will have)
      // cstamp
      uint64_t successor_end = 0;
      bool should_continue = false;
      while (!successor_end) {
        auto s = volatile_read(successor_xc->end);
        successor_state = volatile_read(successor_xc->state);
        if (not successor_xc->verify_owner(successor_xid)) {
          goto check_backedge;
        }
        if (successor_state == TXN::TXN_ABRTD) {
          // If there's a new overwriter, it must have a cstamp larger than mine
          should_continue = true;
          break;
        }
        ALWAYS_ASSERT(successor_state == TXN::TXN_CMMTD ||
                      successor_state == TXN::TXN_COMMITTING);
        successor_end = s;
      }
      if (should_continue) {
        continue;
      }
      if (successor_end < xc->end) {
        return rc_t{RC_ABORT_SERIAL};
      }
    }
  }

  log->commit(NULL);  // will populate log block

  // post-commit cleanup: install clsn to tuples
  // (traverse write-tuple)
  // stuff clsn in tuples in write-set
  auto clsn = xc->end;
  for (uint32_t i = 0; i < write_set.size(); ++i) {
    auto &w = write_set[i];
    Object *object = w.get_object();
    dbtuple *tuple = (dbtuple *)object->GetPayload();
    ASSERT(w.entry);
    tuple->DoWrite();
    dbtuple *overwritten_tuple = tuple->NextVolatile();
    fat_ptr clsn_ptr = object->GenerateClsnPtr(clsn);
    if (overwritten_tuple) {
      ASSERT(overwritten_tuple->sstamp.asi_type() == fat_ptr::ASI_XID);
      ASSERT(XID::from_ptr(overwritten_tuple->sstamp) == xid);
      volatile_write(overwritten_tuple->sstamp, clsn_ptr);
      ASSERT(overwritten_tuple->sstamp.asi_type() == fat_ptr::ASI_LOG);
      ASSERT(overwritten_tuple->sstamp.offset() == clsn_ptr.offset());
    }
    object->SetClsn(clsn_ptr);
    ASSERT(tuple->GetObject()->GetClsn().asi_type() == fat_ptr::ASI_LOG);
#ifndef NDEBUG
    Object *obj = tuple->GetObject();
    fat_ptr pdest = obj->GetPersistentAddress();
    ASSERT((pdest == NULL_PTR and not tuple->size) or
           (pdest.asi_type() == fat_ptr::ASI_LOG));
#endif
  }

  // NOTE: make sure this happens after populating log block,
  // otherwise readers will see inconsistent data!
  // This is where (committed) tuple data are made visible to readers
  volatile_write(xc->state, TXN::TXN_CMMTD);
  return rc_t{RC_TRUE};
}

rc_t transaction::mvocc_read(dbtuple *tuple) {
  read_set.emplace_back(tuple);
  return rc_t{RC_TRUE};
}
}  // namespace ermia

#endif  // MVOCC
