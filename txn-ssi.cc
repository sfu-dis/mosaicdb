#ifdef SSI

#include "macros.h"
#include "txn.h"
#include "dbcore/rcu.h"
#include "dbcore/serial.h"
#include "engine.h"

namespace ermia {

rc_t transaction::parallel_ssi_commit() {
  // tzwang: The race between the updater (if any) and me (as the reader) -
  // A reader publishes its existence by calling serial_register_reader().
  // The updater might not notice this if it already checked this tuple
  // before the reader published its existence. In this case, the writer
  // actually thinks nobody is reading the version that's being overwritten.
  // So as the reader, we need to make sure we know the existence of the
  // updater during precommit. We do this by checking the tuple's sstamp
  // (and it's basically how this parallel pre-commit works).
  //
  // Side note: this "writer-oversights-reader" problem pertains to only
  // SSI, because it needs to track "concurrent reads" or some read that
  // belongs to an evil T1 (assuming I'm the unlucky T2), while for SSN
  // it doesn't matter - SSN only cares about things got committed before,
  // and the reader that registers after writer entered precommit will
  // definitely precommit after the writer.
  //
  // Reader's protocol:
  // 1. If sstamp is not set: no updater yet, but if later an updater comes,
  //    it will enter precommit after me (if it can survive). So by the time
  //    the updater started to look at the readers bitmap, if I'm still in
  //    precommit, I need to make sure it sees me on the bitmap (trivial, as
  //    I won't change any bitmaps after enter precommit); or if I committed,
  //    I need to make sure it sees my updated xstamp. This essentially means
  //    the reader shouldn't deregister from the bitmap before setting xstamp.
  //
  // 2. If sstamp is ASI_LOG: the easy case, updater already committed, do
  //    usually SSI checks.
  //
  // 3. If sstamp is ASI_XID: the most complicated case, updater still active.
  //    a) If the updater has a cstamp < my cstamp: it will commit before
  //       me, so reader should spin on it and find out the final sstamp
  //       result, then do usual SSI checks.
  //
  //    b) If the updater has a cstamp > my cstamp: the writer entered
  //       precommit after I did, so it definitely knows my existence -
  //       b/c when it entered precommit, I won't change any bitmap's
  //       bits (b/c I'm already in precommit). The updater should check
  //       the reader's cstamp (which will be the version's xstamp), i.e.,
  //       the updater will spin if if finds the reader is in pre-commit;
  //       otherwise it will read the xstamp directly from the version.
  //
  //    c) If the updater doesn't have a cstamp: similar to 1 above),
  //       it will know about my existence after entered precommit. The
  //       rest is the same - reader has the responsibility to make sure
  //       xstamp or bitmap reflect its visibility.
  //
  //    d) If the updater has a cstamp < my cstamp: I need to spin until
  //       the updater has left to hold my position in the bitmap, so
  //       that the updater can know that I'm a concurrent reader. This
  //       can be done in post-commit, right before I have to pull myself
  //       out from the bitmap.
  //
  //  The writer then doesn't have much burden, it just needs to take a look
  //  at the readers bitmap and abort if any reader is still active or any
  //  xstamp > ct3; overwritten versions' xstamp are guaranteed to be valid
  //  because the reader won't remove itself from the bitmap unless it updated
  //  v.xstamp.

  auto cstamp = xc->end;

  // get the smallest s1 in each tuple we have read (ie, the smallest cstamp
  // of T3 in the dangerous structure that clobbered our read)
  uint64_t ct3 = xc->ct3;  // this will be the s2 of versions I clobbered

  for (uint32_t i = 0; i < read_set.size(); ++i) {
    auto &r = read_set[i];
  get_overwriter:
    fat_ptr overwriter_clsn = volatile_read(r->sstamp);
    if (overwriter_clsn == NULL_PTR) continue;

    uint64_t tuple_s1 = 0;
    if (overwriter_clsn.asi_type() == fat_ptr::ASI_LOG) {
      // already committed, read tuple's sstamp
      ALWAYS_ASSERT(overwriter_clsn.asi_type() == fat_ptr::ASI_LOG);
      tuple_s1 = volatile_read(overwriter_clsn).offset();
    } else {
      ALWAYS_ASSERT(overwriter_clsn.asi_type() == fat_ptr::ASI_XID);
      XID ox = XID::from_ptr(overwriter_clsn);
      if (ox == xc->owner)  // myself
        continue;
      ASSERT(ox != xc->owner);
      TXN::xid_context *overwriter_xc = TXN::xid_get_context(ox);
      if (not overwriter_xc) goto get_overwriter;

      // A race exists between me and the overwriter (similar to that in SSN):
      // a transaction must transition to COMMITTING state before setting its
      // cstamp, and I must read overwriter's state first, before looking at
      // its cstamp; otherwise if I look at overwriter's cstamp directly, I
      // might miss overwriterwho actually have a smaller cstamp - obtaining
      // a cstamp and storing it in xc->end are not done atomically in a single
      // instruction. So here start with reading overwriter's state.

      // Must obtain the overwriter's status first then check ownership
      auto overwriter_state = volatile_read(overwriter_xc->state);
      if (not overwriter_xc->verify_owner(ox)) {
        goto get_overwriter;
      }

      if (overwriter_state == TXN::TXN_ACTIVE) {
        // successor really still hasn't entered pre-commit, skip
        continue;
      }
      uint64_t overwriter_end = 0;
      bool should_continue = false;
      while (overwriter_end == 0) {
        auto s = volatile_read(overwriter_xc->end);
        overwriter_state = volatile_read(overwriter_xc->state);
        if (not overwriter_xc->verify_owner(ox)) {
          goto get_overwriter;
        }
        if (overwriter_state == TXN::TXN_ABRTD) {
          should_continue = true;
          break;
        }
        overwriter_end = s;
      }
      if (should_continue) {
        continue;
      }
      ALWAYS_ASSERT(overwriter_end);
      ALWAYS_ASSERT(overwriter_end != cstamp);
      if (overwriter_end > cstamp) {
        continue;
      }
      // Spin if the overwriter entered precommit before me: need to
      // find out the final sstamp value.
      // => the updater maybe doesn't know my existence and commit.
      if (overwriter_state == TXN::TXN_COMMITTING) {
        overwriter_state = TXN::spin_for_cstamp(ox, overwriter_xc);
      }
      if (overwriter_state == TXN::TXN_INVALID)  // context change, retry
        goto get_overwriter;
      else if (overwriter_state == TXN::TXN_CMMTD)
        tuple_s1 = overwriter_end;
    }

    if (tuple_s1 and (not ct3 or ct3 > tuple_s1)) ct3 = tuple_s1;

    // Now the updater (if exists) should've already concluded and stamped
    // s2 - requires updater to change state to CMMTD only after setting
    // all s2 values.
    COMPILER_MEMORY_FENCE;
    if (volatile_read(r->s2)) return {RC_ABORT_SERIAL};
    // Release read lock (readers bitmap) after setting xstamp in post-commit
  }

  if (ct3) {
    // now see if I'm the unlucky T2
    for (uint32_t i = 0; i < write_set.size(); ++i) {
      auto &w = write_set[i];
      dbtuple *overwritten_tuple =
          w.get_object()->GetPinnedTuple()->NextVolatile();
      if (not overwritten_tuple) continue;

      // Note: the bits representing readers that will commit **after**
      // me are stable; those representing readers older than my cstamp
      // could go away any time. But I can look at the version's xstamp
      // in that case. So the reader should make sure when it goes away
      // from the bitmap, xstamp is ready to be read by the updater.

      TXN::readers_bitmap_iterator readers_iter(&overwritten_tuple->readers_bitmap);
      while (true) {
        int32_t xid_idx = readers_iter.next(coro_batch_idx, true);
        if (xid_idx == -1) break;

        XID rxid = volatile_read(TXN::rlist.xids[xid_idx]);
        ASSERT(rxid != xc->owner);
        if (rxid == INVALID_XID) continue;

        uint64_t reader_end = 0;
        auto reader_state = TXN::TXN_ACTIVE;
        TXN::xid_context *reader_xc = NULL;
        if (rxid._val) {
          reader_xc = TXN::xid_get_context(rxid);
          if (reader_xc) {
            // copy everything before doing anything
            reader_state = volatile_read(reader_xc->state);
            reader_end = volatile_read(reader_xc->end);
          }
        }

        if (not rxid._val or not reader_xc or
            not reader_xc->verify_owner(rxid)) {
        context_change:
          // Context change: The guy I saw on the bitmap is gone - it
          // must had a cstamp older than mine (otherwise it couldn't
          // go away before me), or given up even before entered pre-
          // commit. So I should read the xstamp in case it did commit.
          //
          // No need to worry about the new guy who occupied this bit:
          // it'll spin on me if I'm still after pre-commit to maintain
          // its position in the bitmap.
          auto tuple_xstamp = volatile_read(overwritten_tuple->xstamp);
          // xstamp might still be 0 - if the reader aborted
          if (tuple_xstamp >= ct3) return {RC_ABORT_SERIAL};
        } else {
          bool should_continue = false;
          if (reader_state != TXN::TXN_ACTIVE and not reader_end) {
            while (not reader_end) {
              auto r = volatile_read(reader_xc->end);
              reader_state = volatile_read(reader_xc->state);
              if (not reader_xc->verify_owner(rxid)) {
                goto context_change;
              }
              if (reader_state == TXN::TXN_ABRTD) {
                should_continue = true;
                break;
              }
              reader_end = r;
            }
          }
          if (should_continue) {  // reader aborted
            continue;
          }
          if (reader_state == TXN::TXN_ACTIVE or not reader_end) {
            // Reader not in precommit yet, and not sure if it is likely
            // to commit. But aborting the pivot is easier here, so we
            // just abort (betting the reader is likely to succeed).
            //
            // Another way is don't do anything here, reader will notice
            // my presence or my legacy (sstamp on the version) once
            // entered precommit; this might cause deadlock tho.
            return {RC_ABORT_SERIAL};
          } else if (reader_end < cstamp) {
            ASSERT(ct3);
            // This reader_end will be the version's xstamp if it committed
            if (reader_end >= ct3) {
              auto cr = TXN::spin_for_cstamp(rxid, reader_xc);
              if (cr == TXN::TXN_CMMTD)
                return {RC_ABORT_SERIAL};
              else if (cr == TXN::TXN_INVALID) {
                // Context change, no clue if the reader committed
                // or aborted, read tuple's xstamp - the same
                // reasoning as in SSN
                if (volatile_read(overwritten_tuple->xstamp) >= ct3)
                  return {RC_ABORT_SERIAL};
              }
            }  // otherwise it's worth the spin - won't use it anyway
          } else {
            // Reader will commit after me, ie its cstamp will be > ct3
            // (b/c ct3 is the first committed in the dangerous structure)
            // and will not release its seat in the bitmap until I'm gone
            return {RC_ABORT_SERIAL};
          }
        }
      }
      // Check xstamp in case in-flight readers left or there was no reader
      if (volatile_read(overwritten_tuple->xstamp) >= ct3)
        return {RC_ABORT_SERIAL};
    }
  }

  if (config::phantom_prot && !MasstreeCheckPhantom()) {
    return rc_t{RC_ABORT_PHANTOM};
  }

  // survived!
  log->commit(NULL);

  // stamp overwritten versions, stuff clsn
  auto clsn = xc->end;
  for (uint32_t i = 0; i < write_set.size(); ++i) {
    auto &w = write_set[i];
    Object *object = w.get_object();
    dbtuple *tuple = (dbtuple *)object->GetPayload();
    tuple->DoWrite();
    dbtuple *overwritten_tuple = tuple->NextVolatile();

    fat_ptr clsn_ptr = object->GenerateClsnPtr(clsn);
    if (overwritten_tuple) {  // update
      ASSERT(not overwritten_tuple->s2);
      // Must set sstamp first before setting s2 (ssi_read assumes sstamp is
      // available once s2 is available)
      ASSERT(XID::from_ptr(volatile_read(overwritten_tuple->sstamp)) == xid);
      volatile_write(overwritten_tuple->sstamp, clsn_ptr);

      // Must set s2 first, before setting clsn
      volatile_write(overwritten_tuple->s2, ct3);
      COMPILER_MEMORY_FENCE;
    }
    volatile_write(tuple->xstamp, cstamp);
    object->SetClsn(clsn_ptr);
    ASSERT(tuple->GetObject()->GetClsn().asi_type() == fat_ptr::ASI_LOG);
  }

  // NOTE: make sure this happens after populating log block,
  // otherwise readers will see inconsistent data!
  // This is where (committed) tuple data are made visible to readers
  //
  // This needs to happen after setting overwritten tuples' s2, b/c
  // the reader needs to check this during pre-commit.
  COMPILER_MEMORY_FENCE;
  volatile_write(xc->state, TXN::TXN_CMMTD);

  // Similar to SSN implementation, xstamp's availability depends solely
  // on when to deregister_reader_tx, not when to transitioning to the
  // "committed" state.
  for (uint32_t i = 0; i < read_set.size(); ++i) {
    auto &r = read_set[i];
    // Update xstamps in read versions, this should happen before
    // deregistering from the bitmap, so when the updater found a
    // context change, it'll get a stable xtamp from the tuple.
    // No need to look into write set and skip: DoTupleRead will
    // skip inserting to read set if it's already in write set; it's
    // possible to see a tuple in both read and write sets, only if
    // the tuple is first read, then updated - updating the xstamp
    // of such a tuple won't hurt, and it eliminates unnecessary
    // cycles spent on hashtable.
    set_tuple_xstamp(r, cstamp);

    // Now wait for the updater that pre-committed before me to go
    // so effectively means I'm holding my position on in the bitmap
    // and preventing it from being reused by another reader before
    // the overwriter leaves. So the overwriter will be able to see
    // a stable readers bitmap and tell if there's an active reader
    // and if so then whether it has to abort because it found itself
    // being an unlucky T2.
    auto sstamp = volatile_read(r->sstamp);
    if (sstamp.asi_type() == fat_ptr::ASI_XID) {
      XID oxid = XID::from_ptr(sstamp);
      if (oxid != this->xid) {  // exclude myself
        TXN::xid_context *ox = TXN::xid_get_context(oxid);
        if (ox) {
          auto ox_end = volatile_read(ox->end);
          if (ox->verify_owner(oxid) and ox_end and ox_end < cstamp)
            TXN::spin_for_cstamp(oxid, ox);
        }
        // if !ox or ox_owner != oxid then the guy is
        // already gone, don't bother
      }
    }
    COMPILER_MEMORY_FENCE;
    // now it's safe to release my seat!
    // Need to do this after setting xstamp, so that the updater can
    // see the xstamp if it didn't find the bit in the bitmap is set.
    serial_deregister_reader_tx(coro_batch_idx, &r->readers_bitmap);
  }
  return rc_t{RC_TRUE};
}

rc_t transaction::ssi_read(dbtuple *tuple) {
  // Consider the dangerous structure that could lead to non-serializable
  // execution: T1 r:w T2 r:w T3 where T3 committed first. Read() needs
  // to check if I'm the T1 and do bookeeping if I'm the T2 (pivot).
  // See tuple.h for explanation on what s2 means.
  if (volatile_read(tuple->s2)) {
    // Read-only optimization: s2 is not a problem if we're read-only and
    // my begin ts is earlier than s2.
    if (not config::enable_ssi_read_only_opt or write_set.size() > 0 or
        xc->begin >= tuple->s2) {
      // sstamp will be valid too if s2 is valid
      ASSERT(tuple->sstamp.asi_type() == fat_ptr::ASI_LOG);
      return rc_t{RC_ABORT_SERIAL};
    }
  }

  fat_ptr tuple_s1 = volatile_read(tuple->sstamp);
  // see if there was a guy with cstamp=tuple_s1 who overwrote this version
  if (tuple_s1.asi_type() == fat_ptr::ASI_LOG) {  // I'm T2
    // remember the smallest sstamp and use it during precommit
    if (not xc->ct3 or xc->ct3 > tuple_s1.offset()) xc->ct3 = tuple_s1.offset();
    // The purpose of adding a version to read-set is to re-check its
    // sstamp status at precommit and set its xstamp for updaters' (if
    // exist) reference during pre-commit thru the readers bitmap.
    // The xstamp is only useful for versions that haven't been updated,
    // or whose updates haven't been finalized. It's only used by the
    // updater at precommit. Once updated (ie v.sstamp is ASI_LOG), future
    // readers of this version (ie it started realy early and is perhaps a
    // long tx) will read the version's sstamp and s2 values. So read won't
    // need to add the version in read-set if it's overwritten.
  } else {
    // survived, register as a reader
    // After this point, I'll be visible to the updater (if any)
    serial_register_reader_tx(coro_batch_idx, &tuple->readers_bitmap);
    read_set.emplace_back(tuple);
  }
  return {RC_TRUE};
}

}  // namespace ermia

#endif  // SSI
