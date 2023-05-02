#ifdef SSN
#include "macros.h"
#include "txn.h"
#include "dbcore/rcu.h"
#include "dbcore/serial.h"
#include "engine.h"

namespace ermia {

rc_t transaction::parallel_ssn_commit() {
  auto cstamp = xc->end;

  // note that sstamp comes from reads, but the read optimization might
  // ignore looking at tuple's sstamp at all, so if tx sstamp is still
  // the initial value so far, we need to initialize it as cstamp. (so
  // that later we can fill tuple's sstamp as cstamp in case sstamp still
  // remained as the initial value.) Consider the extreme case where
  // old_version_threshold = 0: means no read set at all...
  if (is_read_mostly() && config::ssn_read_opt_enabled()) {
    if (xc->sstamp.load(std::memory_order_acquire) == 0)
      xc->sstamp.store(cstamp, std::memory_order_release);
  } else {
    if (xc->sstamp.load(std::memory_order_relaxed) == 0)
      xc->sstamp.store(cstamp, std::memory_order_relaxed);
  }

  // find out my largest predecessor (\eta) and smallest sucessor (\pi)
  // for reads, see if sb. has written the tuples - look at sucessor lsn
  // for writes, see if sb. has read the tuples - look at access lsn

  // Process reads first for a stable sstamp to be used for the
  // read-optimization
  for (uint32_t i = 0; i < read_set.size(); ++i) {
    auto &r = read_set[i];
  try_get_successor:
    ASSERT(r->GetObject()->GetClsn().asi_type() == fat_ptr::ASI_LOG);
    // read tuple->slsn to a local variable before doing anything relying on it,
    // it might be changed any time...
    fat_ptr successor_clsn = volatile_read(r->sstamp);
    if (successor_clsn == NULL_PTR) continue;

    if (successor_clsn.asi_type() == fat_ptr::ASI_LOG) {
      // overwriter already fully committed/aborted or no overwriter at all
      xc->set_sstamp(successor_clsn.offset());
      if (not ssn_check_exclusion(xc)) {
        return rc_t{RC_ABORT_SERIAL};
      }
    } else {
      // overwriter in progress
      ALWAYS_ASSERT(successor_clsn.asi_type() == fat_ptr::ASI_XID);
      XID successor_xid = XID::from_ptr(successor_clsn);
      TXN::xid_context *successor_xc = TXN::xid_get_context(successor_xid);
      if (not successor_xc) {
        goto try_get_successor;
      }

      if (volatile_read(successor_xc->owner) == xc->owner)  // myself
        continue;

      // Must obtain the successor's status first then check ownership
      auto successor_state = volatile_read(successor_xc->state);
      if (not successor_xc->verify_owner(successor_xid)) {
        goto try_get_successor;
      }

      // Note the race between reading the successor's cstamp and the successor
      // setting its cstamp after got one from the log: the successor could
      // have got a cstamp but hasn't stored it in its cstamp field, so here
      // must rely on the successor's state (set before obtaining cstamp) and
      // then spin on successor's cstamp if necessary (state is not
      // committing). Directly reading successor's cstamp might miss successors
      // that have already got cstamp but hasn't stored it in successor_xc->end
      // (esp. dangerous if its cstamp is smaller than mine - could miss
      // successor).
      if (successor_state == TXN::TXN_ACTIVE) {
        // Not yet in pre-commit, skip
        continue;
      }
      // Already in pre-commit or committed, definitely has (or will have)
      // cstamp
      uint64_t successor_end = 0;
      bool should_continue = false;
      while (not successor_end) {
        // Must in the order of 1. read cstamp, 2. read state, 3. verify owner
        auto s = volatile_read(successor_xc->end);
        successor_state = volatile_read(successor_xc->state);
        if (not successor_xc->verify_owner(successor_xid)) {
          goto try_get_successor;
        }
        if (successor_state == TXN::TXN_ABRTD) {
          // If there's a new overwriter, it must have a cstamp larger than mine
          should_continue = true;
          break;
        }
        ALWAYS_ASSERT(successor_state == TXN::TXN_CMMTD or
                      successor_state == TXN::TXN_COMMITTING);
        successor_end = s;
      }
      if (should_continue) {
        continue;
      }
      // overwriter might haven't committed, be commited after me, or before me
      // we only care if the successor is committed *before* me.
      ALWAYS_ASSERT(successor_end);
      ALWAYS_ASSERT(successor_end != cstamp);
      if (successor_end > cstamp) {
        continue;
      }
      if (successor_state == TXN::TXN_COMMITTING) {
        // When we got successor_end, the successor was committing, use
        // successor_end
        // if it indeed committed
        successor_state = TXN::spin_for_cstamp(successor_xid, successor_xc);
      }
      // Context change, previous overwriter was gone, retry (should see ASI_LOG
      // this time).
      if (successor_state == TXN::TXN_INVALID)
        goto try_get_successor;
      else if (successor_state == TXN::TXN_CMMTD) {
        // Again, successor_xc->sstamp might change any time (i.e., successor_xc
        // might get reused because successor concludes), so must
        // read-then-verify.
        auto s = successor_xc->sstamp.load(
            (config::ssn_read_opt_enabled() && is_read_mostly())
                ? std::memory_order_acquire
                : std::memory_order_relaxed);
        if (not successor_xc->verify_owner(successor_xid)) {
          goto try_get_successor;
        }
        xc->set_sstamp(s);
        if (not ssn_check_exclusion(xc)) {
          return rc_t{RC_ABORT_SERIAL};
        }
      }
    }
  }

  for (uint32_t i = 0; i < write_set.size(); ++i) {
    auto &w = write_set[i];
    dbtuple *tuple = (dbtuple *)w.get_object()->GetPayload();

    // go to the precommitted or committed version I (am about to)
    // overwrite for the reader list
    dbtuple *overwritten_tuple = tuple->NextVolatile();
    ASSERT(not overwritten_tuple or
           (tuple->GetObject())->GetNextVolatile().offset() ==
               (uint64_t)(overwritten_tuple->GetObject()));
    if (not overwritten_tuple)  // insert
      continue;

    ASSERT(XID::from_ptr(volatile_read(overwritten_tuple->sstamp)) == xid);

    // Do this before examining the preader field and reading the readers bitmap
    overwritten_tuple->lockout_read_mostly_tx();

    // Now readers who think this is an old version won't be able to read it
    // Then read the readers bitmap - it's guaranteed to cover all possible
    // readers (those who think it's an old version) as we
    // lockout_read_mostly_tx()
    // first. Readers who think this is a young version can still come at any
    // time - they will be handled by the orignal SSN machinery.
    TXN::readers_bitmap_iterator readers_iter(&overwritten_tuple->readers_bitmap);
    while (true) {
      int32_t xid_idx = readers_iter.next(coro_batch_idx, true);
      if (xid_idx == -1) break;

      XID rxid = volatile_read(TXN::rlist.xids[xid_idx]);
      ASSERT(rxid != xc->owner);

      TXN::xid_context *reader_xc = NULL;
      uint64_t reader_end = 0;
      auto reader_state = TXN::TXN_ACTIVE;

      if (rxid != INVALID_XID) {
        reader_xc = TXN::xid_get_context(rxid);
        if (reader_xc) {
          // Copy everything before doing anything:
          // reader_end for getting pstamp;
          reader_state = volatile_read(reader_xc->state);
          reader_end = volatile_read(reader_xc->end);
        }
      }

      if (rxid == INVALID_XID or not reader_xc or
          not reader_xc->verify_owner(rxid)) {
      context_change:
        // Context change - the guy I saw was already gone, should read tuple
        // xstamp
        // (i.e., the reader should make sure it has set the xstamp for the
        // tuple once
        // it deregisters from the bitmap). The new guy that inherits this bit
        // position
        // will spin on me b/c it'll commit after me, and it will also set
        // xstamp after
        // spinning on me, so I'll still be reading the xstamp that really
        // belongs to
        // the older reader - reduces false +ves.)
        if (config::ssn_read_opt_enabled() and
            overwritten_tuple->has_persistent_reader()) {
          // If somebody thought this was an old version, xstamp alone won't be
          // accurate,
          // should consult both tls read_mostly cstamp and xstamp (we consult
          // xstamp in the
          // end of the readers loop for each version later in one go), e.g., a
          // read-mostly
          // tx read it as an old version after a normal tx. Note that we can't
          // just set
          // pstamp to cstamp-1 because the updater here has no clue what the
          // previous
          // owner of this bit position did and how its cstamp compares to mine.
          uint64_t last_cstamp = TXN::serial_get_last_read_mostly_cstamp(xid_idx);
          if (last_cstamp > cstamp) {
            // Reader committed without knowing my existence with a larger
            // cstamp,
            // ie it didn't account me as successor, nothing else to do than
            // abort.
            return {RC_ABORT_RW_CONFLICT};
          }
          xc->set_pstamp(last_cstamp);
          if (not ssn_check_exclusion(xc)) {
            return rc_t{RC_ABORT_SERIAL};
          }
        }  // otherwise we will catch the tuple's xstamp outside the loop
      } else {
        // We have a valid context, now see if we should get reader's commit ts.
        if (reader_state != TXN::TXN_ACTIVE and not reader_end) {
          while (not reader_end) {
            auto r = volatile_read(reader_xc->end);
            reader_state = volatile_read(reader_xc->state);
            if (not reader_xc->verify_owner(rxid)) {
              goto context_change;
            }
            if (reader_state == TXN::TXN_ABRTD) {
              reader_end = 0;
              break;
            }
            reader_end = r;
          }
        }
        if (reader_state == TXN::TXN_ACTIVE or not reader_end or
            reader_end > cstamp) {
          // Not in pre-commit yet or will (attempt to) commit after me or
          // aborted,
          // don't care... unless it's considered an old version by some reader.
          // Still there's a chance to set its sstamp so that the reader will
          // (implicitly) know my existence.
          if (config::ssn_read_opt_enabled() and
              overwritten_tuple->has_persistent_reader()) {
            // Only read-mostly transactions will mark the persistent_reader
            // bit;
            // if reader_xc isn't read-mostly, then it's definitely not him,
            // consult last_read_mostly_cstamp.
            // Need to account for previously committed read-mostly txs anyway
            uint64_t last_cstamp = TXN::serial_get_last_read_mostly_cstamp(xid_idx);
            if (reader_xc->xct->is_read_mostly() and
                not reader_xc->set_sstamp(
                    (~TXN::xid_context::sstamp_final_mark) &
                    xc->sstamp.load(std::memory_order_acquire))) {
              // Failed setting the tx's sstamp - it must have finalized sstamp,
              // i.e., entered precommit, so it must have a valid cstamp.
              if (reader_end == 0) {
                reader_end = volatile_read(reader_xc->end);
              }
              if (reader_xc->verify_owner(rxid)) {
                ALWAYS_ASSERT(reader_end);
                while (last_cstamp < reader_end) {
                  // Wait until the tx sets last_cstamp or aborts
                  last_cstamp = TXN::serial_get_last_read_mostly_cstamp(xid_idx);
                  if (volatile_read(reader_xc->state) == TXN::TXN_ABRTD or
                      !reader_xc->verify_owner(rxid)) {
                    last_cstamp = TXN::serial_get_last_read_mostly_cstamp(xid_idx);
                    break;
                  }
                }
              } else {
                // context change - the tx must have already updated its
                // last_cstamp if committed.
                last_cstamp = TXN::serial_get_last_read_mostly_cstamp(xid_idx);
              }
              if (last_cstamp > cstamp) {
                // committed without knowing me
                return {RC_ABORT_RW_CONFLICT};
              }
            }  // else it must be another transaction is using this context or
            // we succeeded setting the read-mostly tx's sstamp
            xc->set_pstamp(last_cstamp);
            if (not ssn_check_exclusion(xc)) {
              return rc_t{RC_ABORT_SERIAL};
            }
          }
        } else {
          ALWAYS_ASSERT(reader_end and reader_end < cstamp);
          if (config::ssn_read_opt_enabled() and
              overwritten_tuple->has_persistent_reader()) {
            // Some reader who thinks this tuple is old existed, in case
            // reader_xc
            // is read-mostly, spin on it and consult the read_mostly_cstamp.
            // Note
            // still we need to refresh and read xstamp outside the loop in case
            // this
            // isn't the real reader that I should care.
            if (reader_xc->xct->is_read_mostly()) {
              TXN::spin_for_cstamp(rxid, reader_xc);
            }
            xc->set_pstamp(TXN::serial_get_last_read_mostly_cstamp(xid_idx));
            if (not ssn_check_exclusion(xc)) {
              return rc_t{RC_ABORT_SERIAL};
            }
          } else {
            // (Pre-) committed before me, need to wait for its xstamp to
            // finalize.
            if (TXN::spin_for_cstamp(rxid, reader_xc) == TXN::TXN_CMMTD) {
              xc->set_pstamp(reader_end);
              if (not ssn_check_exclusion(xc)) {
                return rc_t{RC_ABORT_SERIAL};
              }
            }
            // else aborted or context change during the spin, no clue if the
            // reader
            // committed - read the xstamp in case it did committ (xstamp is
            // stable
            // now, b/c it'll only deregister from the bitmap after setting
            // xstamp,
            // and a context change means the reader must've concluded - either
            // aborted or committed - and so deregistered from the bitmap). We
            // do this
            // outside the loop in one go.
          }
        }
      }
    }
    // Still need to re-read xstamp in case we missed any reader
    xc->set_pstamp(volatile_read(overwritten_tuple->xstamp));
    if (not ssn_check_exclusion(xc)) {
      return rc_t{RC_ABORT_SERIAL};
    }
  }

  if (config::ssn_read_opt_enabled() and is_read_mostly()) {
    xc->finalize_sstamp();
  }

  if (not ssn_check_exclusion(xc)) return rc_t{RC_ABORT_SERIAL};

  if (config::phantom_prot && !MasstreeCheckPhantom()) {
    return rc_t{RC_ABORT_PHANTOM};
  }

  // ok, can really commit if we reach here
  log->commit(NULL);

  // Do this before setting TXN_CMMTD state so that it'll be stable
  // no matter the guy spinning on me noticed a context change or
  // my real state (CMMTD or ABRTD)
  // XXX: one optimization might be setting this only when read some
  // old versions.
  if (config::ssn_read_opt_enabled() and is_read_mostly())
    TXN::serial_stamp_last_committed_lsn(coro_batch_idx, xc->end);

  uint64_t my_sstamp = 0;
  if (config::ssn_read_opt_enabled() and is_read_mostly()) {
    my_sstamp = xc->sstamp.load(std::memory_order_acquire) &
                (~TXN::xid_context::sstamp_final_mark);
  } else {
    my_sstamp = xc->sstamp.load(std::memory_order_relaxed);
  }
  ALWAYS_ASSERT(my_sstamp and
                (my_sstamp & TXN::xid_context::sstamp_final_mark) == 0);

  // post-commit: stuff access stamps for reads; init new versions
  auto clsn = xc->end;
  for (uint32_t i = 0; i < write_set.size(); ++i) {
    auto &w = write_set[i];
    Object *object = w.get_object();
    dbtuple *tuple = (dbtuple *)object->GetPayload();
    tuple->DoWrite();
    dbtuple *next_tuple = tuple->NextVolatile();
    ASSERT(not next_tuple or (object->GetNextVolatile().offset() ==
                              (uint64_t)next_tuple->GetObject()));
    if (next_tuple) {  // update, not insert
      ASSERT(next_tuple->GetObject()->GetClsn().asi_type());
      ASSERT(XID::from_ptr(next_tuple->sstamp) == xid);
      volatile_write(next_tuple->sstamp, LSN::make(my_sstamp, 0).to_log_ptr());
      ASSERT(next_tuple->sstamp.asi_type() == fat_ptr::ASI_LOG);
      next_tuple->welcome_read_mostly_tx();
    }
    volatile_write(tuple->xstamp, cstamp);
    fat_ptr clsn_ptr = object->GenerateClsnPtr(clsn);
    object->SetClsn(clsn_ptr);
    ASSERT(tuple->GetObject()->GetClsn().asi_type() == fat_ptr::ASI_LOG);
  }

  // This state change means:
  // 1. New data generated by me are available to be read
  //    (need to this after finished post-commit for write set)
  // 2. My cstamp is valid and stable, can be used by
  //    conflicting readers as their sstamp or by conflicting
  //    writers as their pstamp.
  COMPILER_MEMORY_FENCE;
  volatile_write(xc->state, TXN::TXN_CMMTD);

  // The availability of xtamp, solely depends on when
  // serial_deregister_reader_tx is called. So the point
  // here is to do the following strictly one by one in order:
  // 1. Spin on older successor
  // 2. Set xstamp
  // 3. Deregister from bitmap
  // Without 1, the updater might see a larger-than-it-should
  // xstamp and use it as its pstamp -> more unnecessary aborts
  for (uint32_t i = 0; i < read_set.size(); ++i) {
    auto &r = read_set[i];
    ASSERT(r->GetObject()->GetClsn().asi_type() == fat_ptr::ASI_LOG);

    // Spin to hold this position until the older successor is gone,
    // so the updater can get a resonable xstamp (not too high)
    auto sstamp = volatile_read(r->sstamp);
    if (sstamp.asi_type() == fat_ptr::ASI_XID) {
      XID oxid = XID::from_ptr(sstamp);
      if (oxid != this->xid) {  // exclude myself
        TXN::xid_context *ox = TXN::xid_get_context(oxid);
        if (ox) {
          auto ox_end = volatile_read(ox->end);
          auto ox_owner = volatile_read(ox->owner);
          if (ox_owner == oxid and ox_end and ox_end < cstamp)
            TXN::spin_for_cstamp(oxid, ox);
        }
        // if !ox or ox_owner != oxid then the guy is
        // already gone, don't bother
      }
    }

    // Now set the access stamp - do this after the above spin so
    // the writer will read the xstamp that was really set by the
    // preceeding reader, instead of some younger reader that will
    // commit after it (like me).
    set_tuple_xstamp(r, cstamp);

    // Must deregister from the bitmap **after** set xstamp, so that
    // the updater will be able to see the correct xstamp after noticed
    // a context change; otherwise it might miss it and read a too-old
    // xstamp that was set by some earlier reader.
    serial_deregister_reader_tx(coro_batch_idx, &r->readers_bitmap);
  }
  return rc_t{RC_TRUE};
}

rc_t transaction::ssn_read(dbtuple *tuple) {
  auto v_clsn = tuple->GetObject()->GetClsn().offset();
  // \eta - largest predecessor. So if I read this tuple, I should commit
  // after the tuple's creator (trivial, as this is committed version, so
  // this tuple's clsn can only be a predecessor of me): so just update
  // my \eta if needed.
  if (xc->pstamp < v_clsn) xc->pstamp = v_clsn;

  auto tuple_sstamp = volatile_read(tuple->sstamp);
  if (tuple_sstamp.asi_type() == fat_ptr::ASI_LOG) {
    // have committed overwrite
    if (xc->sstamp > tuple_sstamp.offset() or xc->sstamp == 0)
      xc->sstamp = tuple_sstamp.offset();  // \pi
  } else {
    ASSERT(tuple_sstamp == NULL_PTR or
           tuple_sstamp.asi_type() == fat_ptr::ASI_XID);
    // Exclude myself
    if (tuple_sstamp != NULL_PTR and XID::from_ptr(tuple_sstamp) == xc->owner)
      return {RC_TRUE};

    // If there's no (committed) overwrite so far, we need to track this read,
    // unless it's an old version.
    ASSERT(tuple_sstamp == NULL_PTR or
           XID::from_ptr(tuple_sstamp) != xc->owner);
    if (tuple->is_old(xc)) {
      ASSERT(is_read_mostly());
      // Aborting long read-mostly transactions is expensive, spin first
      static const uint32_t kSpins = 100000;
      uint32_t spins = 0;
      while (not tuple->set_persistent_reader()) {
        if (++spins >= kSpins) {
          return {RC_ABORT_RW_CONFLICT};
        }
      }
    } else {
      // Now if this tuple was overwritten by somebody, this means if I read
      // it, that overwriter will have anti-dependency on me (I must be
      // serialized before the overwriter), and it already committed (as a
      // successor of mine), so I need to update my \pi for the SSN check.
      // This is the easier case of anti-dependency (the other case is T1
      // already read a (then latest) version, then T2 comes to overwrite it).
      read_set.emplace_back(tuple);
    }
    serial_register_reader_tx(coro_batch_idx, &tuple->readers_bitmap);
  }

#ifdef EARLY_SSN_CHECK
  if (not ssn_check_exclusion(xc)) return {RC_ABORT_SERIAL};
#endif
  return {RC_TRUE};
}

}  // namespace ermia
#endif  // SSN
