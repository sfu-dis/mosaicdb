#pragma once

/* Common definitions for the storage manager.
 */

#include "sm-defs.h"
#include "sm-exceptions.h"

#include "size-encode.h"
#include "defer.h"

#include <cstddef>
#include <stdarg.h>
#include <stdint.h>
#include <pthread.h>

#include <dirent.h>
#include <cerrno>

namespace ermia {

typedef uint32_t OID;
typedef uint32_t FID;

static OID const INVALID_OID = ~uint32_t{0};

static size_t const NUM_LOG_SEGMENT_BITS = 4;
static size_t const NUM_LOG_SEGMENTS = 1 << NUM_LOG_SEGMENT_BITS;
// A tlog LSN only uses 40 bits of the fat_ptr, and segment id uses 4 bits.
static size_t const SEGMENT_MAX_SIZE = 1UL << (40 - 4);

/* A "fat" pointer that carries an address, augmented with encoded
   allocation size information, plus 8 bits for implementations to use
   as they see fit (e.g. to identify pointers that refer to data
   stored in a file rather than memory).

   Fat pointers are primarily used to identify version objects stored
   in various OID arrays (and corresponding log records), but are
   useful any time it is helpful to know the size of an object the
   pointer refers to.

   The size and flag information occupy 16 bits, leaving 48 bits for
   the actual address. On x86_64 architectures, this fits well with
   "canonical" addresses [1], which are interpreted as signed 48-bit
   values. On other systems, some other mechanism for reducing the
   address space to 48 bits might be required.

   In case 48 bits (= 256 TB) is not enough, we could exploit the fact
   that all loggable data types are 16-byte aligned and gain another 4
   bits of address space. 4 PB should keep a main-memory system happy
   for the foreseeable future. For now, though 256TB is plenty.

   [1] See http://en.wikipedia.org/wiki/X86-64#Canonical_form_addresses

   Incidentally, AMD appears to have patented the idea of enforcing
   canonical addresses as 48-bit pointers sign-extended 64 bits:
   http://www.google.com/patents/US6807616. Not particularly relevant
   here, but still an interesting factoid.
 */
struct fat_ptr {
  /* The enums below carve up the range 0x0000 to 0xffff, comprising
     the 16 bits that fat_ptr makes available for non-pointer data.

     The mask 0x00ff is dedicated to a size code. Implementations
     can use this (perhaps in conjunction with other metadata) to
     determine the size of the object this fat_ptr references.

     The mask 0x7f00 is the "address space identifier" (ASI) for
     this fat_ptr (see details below).

     The mask 0x8000 is a dirty flag (the meaning of which is
     implementation-defined, but probably implies a need to write
     the data to disk soon).

     The 7-bit ASI space is divided as follows:

     0x00 -- main memory

     0x01...0x0f -- currently unused

     0x10...0x1f -- <LogID, TLog LSN>; low 4 bits give phsyical log segment file

     0x20...0x2f -- heap file; low 4 bits give heap segment file

     0x30...0x3f -- indirect LSN (points to a fat_pointer of the true location)

     0x40...0x4f -- an XID encoded as a fat_ptr.

     0x50...0x5f -- pointer to some location in the chkpt file.

     0x60...0x7f -- a CSN encoded as a fat_ptr.
   */
  static uint64_t const VALUE_START_BIT = 16;

  static uint64_t const SIZE_BITS = 8;
  static uint64_t const SIZE_MASK = (1 << SIZE_BITS) - 1;

  static uint64_t const FLAG_START_BIT = SIZE_BITS;
  static uint64_t const FLAG_BITS = VALUE_START_BIT - SIZE_BITS;
  static uint64_t const FLAG_MASK = ((1 << FLAG_BITS) - 1) << FLAG_START_BIT;

  static uint64_t const ASI_START_BIT = FLAG_START_BIT;
  static uint64_t const ASI_BITS = FLAG_BITS - 1;
  static uint64_t const ASI_MASK = (1 << ASI_BITS) - 1;
  static uint64_t const ASI_FLAG_MASK = ASI_MASK << ASI_START_BIT;

  static uint64_t const DIRTY_BIT = VALUE_START_BIT - 1;
  static uint64_t const DIRTY_MASK = 1 << DIRTY_BIT;

  static uint64_t const ASI_LOG = 0x10;
  static uint64_t const ASI_HEAP = 0x20;
  static uint64_t const ASI_EXT = 0x30;
  static uint64_t const ASI_XID = 0x40;
  static uint64_t const ASI_CHK = 0x50;
  static uint64_t const ASI_CSN = 0x60;

  static uint64_t const ASI_LOG_FLAG = ASI_LOG << ASI_START_BIT;
  static uint64_t const ASI_HEAP_FLAG = ASI_HEAP << ASI_START_BIT;
  static uint64_t const ASI_EXT_FLAG = ASI_EXT << ASI_START_BIT;
  static uint64_t const ASI_XID_FLAG = ASI_XID << ASI_START_BIT;
  static uint64_t const ASI_CHK_FLAG = ASI_CHK << ASI_START_BIT;
  static uint64_t const ASI_CSN_FLAG = ASI_CSN << ASI_START_BIT;

  static uint64_t const ASI_SEGMENT_MASK = 0x0f;

  static_assert(!((ASI_LOG | ASI_CSN | ASI_HEAP | ASI_EXT | ASI_XID | ASI_CHK) & ~ASI_MASK), "Go fix ASI_MASK");
  static_assert(NUM_LOG_SEGMENTS == 16, "The constant above is out of sync");
  static_assert(FLAG_BITS >= 1 + 1 + NUM_LOG_SEGMENT_BITS, "Need more bits");

  /* Make a fat_ptr that points to an address in memory.
   */
  static fat_ptr make(void *ptr, uint8_t sz_code, uint16_t flags = 0) {
    union {
      void *v;
      intptr_t n;
    } u = {ptr};
    return make(u.n, sz_code, flags);
  }

  static fat_ptr make(uintptr_t n, uint8_t sz_code, uint16_t flags = 0) {
    ASSERT(not(flags & SIZE_MASK));
    n <<= VALUE_START_BIT;
    n |= sz_code;
    n |= flags;
    return (fat_ptr){n};
  }

  uint64_t _ptr;

  template <typename T>
  operator T *() const {
    ASSERT(not asi());
    return (T *)offset();
  }

  uintptr_t offset() const { return _ptr >> VALUE_START_BIT; }

  uint8_t size_code() const { return _ptr & SIZE_MASK; }

  // return the ASI as a 16-bit number
  uint16_t asi() const { return (_ptr >> ASI_START_BIT) & ASI_MASK; }

  uint16_t asi_type() const { return asi() & ~ASI_SEGMENT_MASK; }

  uint16_t asi_segment() const { return asi() & ASI_SEGMENT_MASK; }

  uint16_t is_dirty() const { return _ptr & DIRTY_MASK; }

  // return dirty + ASI without any shifting
  uint16_t flags() const { return _ptr & FLAG_MASK; }

  /* Return the log segment of this fat_ptr, or -1 if not in the log
     ASI.
   */
  int log_segment() const {
    return (asi_type() == ASI_LOG) ? asi_segment() : -1;
  }

  int heap_segment() const {
    return (asi_type() == ASI_HEAP) ? asi_segment() : -1;
  }

  int ext_segment() const {
    return (asi_type() == ASI_EXT) ? asi_segment() : -1;
  }

  bool operator==(fat_ptr const &other) const { return _ptr == other._ptr; }

  bool operator!=(fat_ptr const &other) const { return _ptr != other._ptr; }
};

static inline fat_ptr volatile_read(fat_ptr volatile &p) {
  return fat_ptr{volatile_read(p._ptr)};
}

static inline void volatile_write(fat_ptr volatile &x, fat_ptr const &y) {
  volatile_write(x._ptr, y._ptr);
}

// The equivalent of a NULL pointer
static fat_ptr const NULL_PTR = {0};

/* An LSN consists of two parts: log ID and local offset (loffset) within the
 * log. Within a log, the loffset increases monotonically and represents
 * ordering of events recorded by the same log. Across logs, however, loffsets
 * are not comparable. LSNs also identify the location of a log record on disk.
 * We accomplish this in a similar fashion to fat_ptr, exploiting the fact that
 * we have to be able to embed an LSN in a fat_ptr. That means each LSN can only
 * have 48 significant bits, but it also means we can embed the physical segment
 * number. As long as we know the starting LSN of each segment, an LSN can be
 * converted to a fat_ptr and vice-versa in constant time.
 *
 * LSN embeds a size_code, just like fat_ptr does, but it is not directly used
 * by the log. Instead, the LSN size_code is used when converting between LSN
 * and fat_ptr, and corresponds to a size for the object that was encoded
 * (identified by its FID, with associated scaling factor). An LSN that is not
 * associated with any FID will use INVALID_SIZE_CODE as its size_code.
 *
 * For comparison purposes, we can leave all the extra bits in place: if two LSN
 * differ, they do so in the most significant bits and the trailing extras won't
 * matter. If two LSN are the same, they should have the same trailing bits as
 * well (if they do not, undefined behaviour should be expected).
 */
struct LSN {
  static const uint64_t LOG_ID_BITS = 8;
  static const uint64_t LOFFSET_BITS = 40;
  static const uint64_t LOFFSET_MASK = (1UL << LOFFSET_BITS) - 1;
  static const uint64_t LOG_ID_MASK = ~LOFFSET_MASK;

  static LSN make(uint64_t logid, uint64_t loff, int segnum,
                  uint8_t size_code = INVALID_SIZE_CODE) {
    ASSERT(!(segnum & ~fat_ptr::ASI_SEGMENT_MASK));
    ASSERT(!(loff & LOG_ID_MASK));
    uintptr_t flags = segnum << fat_ptr::ASI_START_BIT;
    return (LSN){(logid << (fat_ptr::VALUE_START_BIT + LOFFSET_BITS)) | (loff << fat_ptr::VALUE_START_BIT) | flags | size_code};
  }

  static LSN from_ptr(fat_ptr const &p) {
    LOG_IF(FATAL, p.asi_type() != fat_ptr::ASI_LOG) <<
        "Attempt to convert non-LSN fat_ptr to LSN";
    uint64_t off = p.offset();
    return LSN::make(off >> LOFFSET_BITS, off & LOFFSET_MASK, p.asi_segment());
  }

  fat_ptr to_ptr() const { return fat_ptr{_val | fat_ptr::ASI_LOG_FLAG}; }

  uint64_t _val;

  inline uint64_t logid() const { return _val >> (fat_ptr::VALUE_START_BIT + LOFFSET_BITS); }
  inline uint64_t loffset() const { return (_val >> fat_ptr::VALUE_START_BIT) & LOFFSET_MASK; }
  inline uint16_t flags() const { return _val & fat_ptr::ASI_FLAG_MASK; }
  inline uint32_t segment() const {
    return (_val >> fat_ptr::ASI_START_BIT) & fat_ptr::ASI_SEGMENT_MASK;
  }
  uint8_t size_code() const { return _val & fat_ptr::SIZE_MASK; }

  // true comparison operators
  bool operator<(LSN const &other) const {
    LOG_IF(FATAL, logid() != other.logid()) << "Cannot compare two LSNs globally"; 
    return _val < other._val;
  }
  bool operator==(LSN const &other) const { return _val == other._val; }

  // synthesized comparison operators
  bool operator>(LSN const &other) const { return other < *this; }
  bool operator<=(LSN const &other) const { return !(*this > other); }
  bool operator>=(LSN const &other) const { return other <= *this; }
  bool operator!=(LSN const &other) const { return !(*this == other); }
};

static inline LSN volatile_read(LSN volatile &x) {
  return LSN{volatile_read(x._val)};
}

static LSN const INVALID_LSN = {0};

/* Transaction ids uniquely identify each transaction in the system
   without implying any particular ordering. Over time they show an
   increasing trend, but the sequence can have gaps and significant
   local disorder.

   Although they occupy 64 bits, XIDs are actually the composition of
   the low 16 bits of a 32-bit epoch number (with high 16 bits unused),
   a 32-bit local identifier and 16 bits for the size and flag. The
   combination is globally unique (overflow would take roughly 7 years
   at 1Mtps), and any two transactions that coexist are guaranteed to
   have different local identifiers.
 */
struct XID {
  static XID make(uint32_t e, uint32_t i) {
    uint64_t x = e;
    x <<= 32;
    x |= i;
    x <<= 16;
    x |= fat_ptr::ASI_XID_FLAG;
    x |= INVALID_SIZE_CODE;
    return XID{x};
  }

  static XID from_ptr(fat_ptr const &p) {
    LOG_IF(FATAL, p.asi_type() != fat_ptr::ASI_XID)
      << "Attempt to convert non-XID fat_ptr to XID";
    return XID{p._ptr};
  }

  uint64_t _val;

  fat_ptr to_ptr() const { return fat_ptr{_val | INVALID_SIZE_CODE}; }
  uint16_t epoch() const { return _val >> 48; }
  uint32_t local() const { return _val >> 16; }
  uint16_t flags() const { return _val & fat_ptr::FLAG_MASK; }

  // true comparison operators
  bool operator==(XID const &other) const { return _val == other._val; }

  // synthesized comparison operators
  bool operator!=(XID const &other) const { return not(*this == other); }
};

static inline XID volatile_read(XID volatile &x) {
  return XID{volatile_read(x._val)};
}

static XID const INVALID_XID = {fat_ptr::ASI_XID_FLAG};

struct CSN {
  static CSN make(uint64_t c) {
    uint64_t x = c;
    x <<= fat_ptr::VALUE_START_BIT;
    x |= fat_ptr::ASI_CSN_FLAG;
    x |= INVALID_SIZE_CODE;
    return CSN{x};
  }

  static CSN from_ptr(fat_ptr const &p) {
    LOG_IF(FATAL, p.asi_type() != fat_ptr::ASI_CSN)
      << "Attempt to convert non-CSN fat_ptr to CSN";
    return CSN{p._ptr};
  }

  uint64_t _val;

  fat_ptr to_ptr() const { return fat_ptr{_val | INVALID_SIZE_CODE}; }
  uint64_t offset() const { return _val >> fat_ptr::VALUE_START_BIT; }
  uint16_t flags() const { return _val & fat_ptr::FLAG_MASK; }

  // true comparison operators
  bool operator==(CSN const &other) const { return _val == other._val; }

  // synthesized comparison operators
  bool operator!=(CSN const &other) const { return !(*this == other); }
};

static inline CSN volatile_read(CSN volatile &x) {
  return CSN{volatile_read(x._val)};
}

static CSN const INVALID_CSN = {fat_ptr::ASI_CSN_FLAG};

int os_dup(int fd);

/* A class for iterating over file name entries in a directory using a
   range-based for loop.

   WARNING: the class only supports one iteration at a time, and
   implement just enough of the iterator protocol to work with
   range-based for loops. In particular, this means that operator!=
   can only distinguish between "end" and "not end."
 */
struct dirent_iterator {
  struct iterator {
    DIR *_d;
    dirent *_dent;
    void operator++();
    char const *operator*() { return _dent->d_name; }
    bool operator!=(iterator const &other) { return _d != other._d; }
  };

  dirent_iterator(char const *dname);

  iterator begin();
  iterator end();

  int dup();

  ~dirent_iterator();

  // sorry, can't copy or move
  dirent_iterator(dirent_iterator const &other) = delete;
  dirent_iterator(dirent_iterator &&other) = delete;
  void operator=(dirent_iterator other) = delete;

  DIR *_d;
  bool used;
};

}  // namespace ermia
