# Generate burt-hash.cpp
if(NOT EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/burt-hash.cpp")
  set(BURT_HASH_GEN python2 ${CMAKE_CURRENT_SOURCE_DIR}/burt-hash.py)
  execute_process(COMMAND ${BURT_HASH_GEN} ERROR_VARIABLE out OUTPUT_VARIABLE out)
  if ("${out}" STREQUAL "")
    message(FATAL_ERROR "Generating burt-hash.cpp failed. Please make sure python2 is installed.")
  else()
    file(WRITE ${CMAKE_CURRENT_SOURCE_DIR}/burt-hash.cpp "${out}")
  endif()
endif()

set(DBCORE_SRC
  ${CMAKE_CURRENT_SOURCE_DIR}/burt-hash.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/dlog.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/dynarray.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/epoch.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/mcs_lock.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/pcommit.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/rcu.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/serial.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/size-encode.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/sm-alloc.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/sm-common.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/sm-config.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/sm-coroutine.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/sm-exceptions.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/sm-table.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/sm-object.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/sm-oid-alloc.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/sm-oid.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/sm-spinlock.cpp
  ${CMAKE_CURRENT_SOURCE_DIR}/xid.cpp
)

set_property(GLOBAL APPEND PROPERTY ALL_SRC ${DBCORE_SRC})
