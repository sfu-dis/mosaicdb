#include "dbcore/rcu.h"

#include "engine.h"
#include "txn.h"

#include "masstree/masstree_scan.hh"

namespace ermia {

ermia::coro::task<rc_t>
ConcurrentMasstreeIndex::flat_GetRecord(transaction *t, const varstr &key,
                                        varstr &value, OID *out_oid) {
  OID oid = INVALID_OID;
  rc_t rc = rc_t{RC_INVALID};

  // start: masstree search
  ConcurrentMasstree::threadinfo ti(t->xc->begin_epoch);
  ConcurrentMasstree::unlocked_tcursor_type lp(*masstree_.get_table(),
                                               key.data(), key.size());

  // start: find_unlocked
  int match;
  key_indexed_position kx;
  ConcurrentMasstree::node_base_type *root =
      const_cast<ConcurrentMasstree::node_base_type *>(lp.root_);

retry:
  // start: reach_leaf
  const ConcurrentMasstree::node_base_type *n[2];
  ConcurrentMasstree::nodeversion_type v[2];
  bool sense;

  // Get a non-stale root.
  // Detect staleness by checking whether n has ever split.
  // The true root has never split.
  sense = false;
  n[sense] = root;
  while (1) {
    v[sense] = n[sense]->stable_annotated(ti.stable_fence());
    if (!v[sense].has_split())
      break;
    n[sense] = n[sense]->unsplit_ancestor();
  }

  // Loop over internal nodes.
  while (!v[sense].isleaf()) {
    const ConcurrentMasstree::internode_type *in =
        static_cast<const ConcurrentMasstree::internode_type *>(n[sense]);
    in->prefetch();
    co_await suspend_always{};
    int kp = ConcurrentMasstree::internode_type::bound_type::upper(lp.ka_, *in);
    n[!sense] = in->child_[kp];
    if (!n[!sense])
      goto retry;

    // const ConcurrentMasstree::internode_type* in2 = static_cast<const ConcurrentMasstree::internode_type*>(n[!sense]);
    // in2->prefetch();
    // co_await suspend_always{};
    v[!sense] = n[!sense]->stable_annotated(ti.stable_fence());

    if (likely(!in->has_changed(v[sense]))) {
      sense = !sense;
      continue;
    }

    ConcurrentMasstree::nodeversion_type oldv = v[sense];
    v[sense] = in->stable_annotated(ti.stable_fence());
    if (oldv.has_split(v[sense]) &&
        in->stable_last_key_compare(lp.ka_, v[sense], ti) > 0) {
      goto retry;
    }
  }

  lp.v_ = v[sense];
  lp.n_ = const_cast<ConcurrentMasstree::leaf_type *>(
      static_cast<const ConcurrentMasstree::leaf_type *>(n[sense]));
  // end: reach_leaf

forward:
  if (lp.v_.deleted())
    goto retry;

  // XXX(tzwang): already working on this node, no need to prefetch+yield again?
  // lp.n_->prefetch();
  // co_await suspend_always{};
  lp.perm_ = lp.n_->permutation();
  kx = ConcurrentMasstree::leaf_type::bound_type::lower(lp.ka_, lp);
  if (kx.p >= 0) {
    lp.lv_ = lp.n_->lv_[kx.p];
    lp.lv_.prefetch(lp.n_->keylenx_[kx.p]);
    co_await suspend_always{};
    match = lp.n_->ksuf_matches(kx.p, lp.ka_);
  } else
    match = 0;
  if (lp.n_->has_changed(lp.v_)) {
    lp.n_ = lp.n_->advance_to_key(lp.ka_, lp.v_, ti);
    goto forward;
  }

  if (match < 0) {
    lp.ka_.shift_by(-match);
    root = lp.lv_.layer();
    goto retry;
  }
  // end: find_unlocked

  bool found = match;
  dbtuple *tuple = nullptr;
  if (found) {
    oid = lp.value();
    // end: masstree search

    // start: oid_get_version
    oid_array *oa = table_descriptor->GetTupleArray();
    TXN::xid_context *visitor_xc = t->xc;
    fat_ptr *entry = oa->get(oid);
  start_over:
    ::prefetch((const char *)entry);
    co_await suspend_always{};

    fat_ptr ptr = volatile_read(*entry);
    ASSERT(ptr.asi_type() == 0 || ptr.asi_type() == fat_ptr::ASI_LOG);
    if (ptr.asi_type() == fat_ptr::ASI_LOG) {
      if (t->abort_if_cold()) {
        t->set_forced_abort(true);
        co_return {RC_ABORT};
      }
 
      transaction *xct = visitor_xc->xct;
      fat_ptr pdest = ptr;
      Object *object = nullptr;
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
        fat_ptr csn_ptr = CSN::make(logrec->csn).to_ptr();
        uint16_t s = csn_ptr.asi_type();
        ASSERT(s == fat_ptr::ASI_CSN);
        object->SetCSN(csn_ptr);
        ASSERT(object->GetCSN().asi_type() == fat_ptr::ASI_CSN);
      } else {
        ALWAYS_ASSERT(0);
        // TODO(tzwang): Load tuple data form the chkpt file
      }

      if (!tuple) {
        found = false;
      }

      if (found) {
        volatile_write(rc._val, t->DoTupleRead(tuple, &value)._val);
      } else if (config::phantom_prot) {
        ALWAYS_ASSERT(0);
        // TODO(khuang): volatile_write(rc._val, DoNodeRead(t, sinfo.first, sinfo.second)._val);
      } else {
        volatile_write(rc._val, RC_FALSE);
      }

#ifndef SSN
      ASSERT(rc._val == RC_FALSE || rc._val == RC_TRUE);
#endif

      if (out_oid) {
        *out_oid = oid;
      }

      co_return rc;
    } else {
      Object *prev_obj = nullptr;
      while (ptr.offset()) {
        Object *cur_obj = nullptr;
        // Must read next_ before reading cur_obj->_clsn:
        // the version we're currently reading (ie cur_obj) might be unlinked
        // and thus recycled by the memory allocator at any time if it's not
        // a committed version. If so, cur_obj->_next will be pointing to some
        // other object in the allocator's free object pool - we'll probably
        // end up at la-la land if we followed this _next pointer value...
        // Here we employ some flavor of OCC to solve this problem:
        // the aborting transaction that will unlink cur_obj will update
        // cur_obj->_clsn to NULL_PTR, then deallocate(). Before reading
        // cur_obj->_clsn, we (as the visitor), first dereference pp to get
        // a stable value that "should" contain the right address of the next
        // version. We then read cur_obj->_clsn to verify: if it's NULL_PTR
        // that means we might have read a wrong _next value that's actually
        // pointing to some irrelevant object in the allocator's memory pool,
        // hence must start over from the beginning of the version chain.
        fat_ptr tentative_next = NULL_PTR;
        ASSERT(ptr.asi_type() == 0);
        cur_obj = (Object *)ptr.offset();
        // Object::PrefetchHeader(cur_obj);
        // co_await suspend_always{};
        tentative_next = cur_obj->GetNextVolatile();
        ASSERT(tentative_next.asi_type() == 0);

        // bool retry = false;
        // bool visible = oidmgr->TestVisibility(cur_obj, visitor_xc, retry);
        {
          fat_ptr csn = cur_obj->GetCSN();
          if (csn == NULL_PTR) {
            // dead tuple that was (or about to be) unlinked, start over
            goto start_over;
          }
          uint16_t asi_type = csn.asi_type();
          ALWAYS_ASSERT(asi_type == fat_ptr::ASI_XID ||
                        asi_type == fat_ptr::ASI_CSN);

          if (asi_type == fat_ptr::ASI_XID) { // in-flight
            XID holder_xid = XID::from_ptr(csn);
            // Dirty data made by me is visible!
            if (holder_xid == t->xc->owner) {
              ASSERT(!cur_obj->GetNextVolatile().offset() ||
                     ((Object *)cur_obj->GetNextVolatile().offset())
                             ->GetCSN()
                             .asi_type() == fat_ptr::ASI_CSN);
              goto handle_visible;
            }
            auto *holder = TXN::xid_get_context(holder_xid);
            if (!holder) {
              goto start_over;
            }

            auto state = volatile_read(holder->state);
            auto owner = volatile_read(holder->owner);

            // context still valid for this XID?
            if (owner != holder_xid) {
              goto start_over;
            }

            if (state == TXN::TXN_CMMTD) {
              ASSERT(volatile_read(holder->end));
              ASSERT(owner == holder_xid);
              if (holder->end < t->xc->begin) {
                goto handle_visible;
              }
              goto handle_invisible;
            }
          } else {
            // Already committed, now do visibility test
            ASSERT(
                cur_obj->GetPersistentAddress().asi_type() == fat_ptr::ASI_LOG ||
                cur_obj->GetPersistentAddress().asi_type() == fat_ptr::ASI_CHK ||
                cur_obj->GetPersistentAddress() == NULL_PTR); // Delete
            uint64_t csn_value = CSN::from_ptr(csn).offset();
            if (csn_value <= t->xc->begin) {
              goto handle_visible;
            }
          }
          goto handle_invisible;
        }
      handle_visible:
        if (out_oid) {
          *out_oid = oid;
        }

        co_return t->DoTupleRead(cur_obj->SyncGetPinnedTuple(t), &value);

      handle_invisible:
        ptr = tentative_next;
        prev_obj = cur_obj;
      }
    }
  }
  co_return {RC_FALSE};
}

} // namespace ermia