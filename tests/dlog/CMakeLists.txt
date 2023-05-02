set(DLOG_TEST_SRCS
    ${CMAKE_SOURCE_DIR}/dbcore/dlog.cpp
    test_dlog.cpp
)

add_executable(test_dlog ${DLOG_TEST_SRCS})
target_link_libraries(test_dlog)

