add_subdirectory(egen)
add_subdirectory(record)

set_property(GLOBAL APPEND PROPERTY ALL_SRC
  ${CMAKE_CURRENT_SOURCE_DIR}/bench.cc
  ${CMAKE_CURRENT_SOURCE_DIR}/dbtest.cc
)

