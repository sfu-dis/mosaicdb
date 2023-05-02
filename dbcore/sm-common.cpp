#include "sm-common.h"
#include <string.h>
#include "rcu.h"

#include <glog/logging.h>

#include <cstdio>
#include <cstdlib>
#include <cerrno>
#include <unistd.h>
#include <sys/fcntl.h>
#include <vector>

namespace ermia {

int os_dup(int fd) {
  int rval = dup(fd);
  THROW_IF(rval < 0, os_error, errno, "Unable to duplicate fd %d", fd);
  return rval;
}

dirent_iterator::dirent_iterator(char const *dname)
    : _d(opendir(dname)), used(false) {
  THROW_IF(not _d, os_error, errno, "Unable to open/create directory: %s",
           dname);
}

dirent_iterator::~dirent_iterator() {
  int err = closedir(_d);
  WARN_IF(err, "Closing dirent iterator gave errno %d", errno);
}

void dirent_iterator::iterator::operator++() {
  errno = 0;
  _dent = readdir(_d);
  if (not _dent) {
    THROW_IF(errno, os_error, errno, "Error during directory scan");
    _d = NULL;
  }
}

dirent_iterator::iterator dirent_iterator::begin() {
  if (used) rewinddir(_d);

  used = true;
  iterator rval{_d, NULL};
  ++rval;  // prime it
  return rval;
}

dirent_iterator::iterator dirent_iterator::end() {
  return iterator{NULL, NULL};
}

int dirent_iterator::dup() { return os_dup(dirfd(_d)); }
}  // namespace ermia
