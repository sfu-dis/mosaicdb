set_property(GLOBAL APPEND PROPERTY ALL_SRC
  ${CMAKE_CURRENT_SOURCE_DIR}/ycsb-config.cc
  ${CMAKE_CURRENT_SOURCE_DIR}/../bench.cc
  ${CMAKE_CURRENT_SOURCE_DIR}/../dbtest.cc
)

add_executable(ycsb_SI_sequential ${CMAKE_CURRENT_SOURCE_DIR}/ycsb-sequential.cc)
set_target_properties(ycsb_SI_sequential PROPERTIES COMPILE_FLAGS "")
#set_target_properties(corobase_SI PROPERTIES COMPILE_FLAGS "-DCORO_BATCH_COMMIT -DGROUP_SAME_TRX -DCORO_UPDATE_VERSION_CHAIN")
target_link_libraries(ycsb_SI_sequential corobase_si thread_pool)

add_executable(ycsb_SI_simple_coro ${CMAKE_CURRENT_SOURCE_DIR}/ycsb-simple-coro.cc)
set_target_properties(ycsb_SI_simple_coro PROPERTIES COMPILE_FLAGS "")
#set_target_properties(corobase_SI PROPERTIES COMPILE_FLAGS "-DCORO_BATCH_COMMIT -DGROUP_SAME_TRX -DCORO_UPDATE_VERSION_CHAIN")
target_link_libraries(ycsb_SI_simple_coro corobase_si thread_pool)

add_executable(ycsb_SI_nested_coro ${CMAKE_CURRENT_SOURCE_DIR}/ycsb-nested-coro.cc)
set_target_properties(ycsb_SI_nested_coro PROPERTIES COMPILE_FLAGS "-DNESTED_COROUTINE")
#set_target_properties(corobase_SI PROPERTIES COMPILE_FLAGS "-DCORO_BATCH_COMMIT -DGROUP_SAME_TRX -DCORO_UPDATE_VERSION_CHAIN")
target_link_libraries(ycsb_SI_nested_coro corobase_nested_coro_si thread_pool)

add_executable(ycsb_SI_hybrid_coro ${CMAKE_CURRENT_SOURCE_DIR}/ycsb-hybrid-coro.cc)
set_target_properties(ycsb_SI_hybrid_coro PROPERTIES COMPILE_FLAGS "")
#set_target_properties(corobase_SI PROPERTIES COMPILE_FLAGS "-DCORO_BATCH_COMMIT -DGROUP_SAME_TRX -DCORO_UPDATE_VERSION_CHAIN")
target_link_libraries(ycsb_SI_hybrid_coro corobase_hybrid_coro_si thread_pool)

add_executable(ycsb_SI_flat_coro ${CMAKE_CURRENT_SOURCE_DIR}/ycsb-flat-coro.cc)
set_target_properties(ycsb_SI_flat_coro PROPERTIES COMPILE_FLAGS "")
#set_target_properties(corobase_SI PROPERTIES COMPILE_FLAGS "-DCORO_BATCH_COMMIT -DGROUP_SAME_TRX -DCORO_UPDATE_VERSION_CHAIN")
target_link_libraries(ycsb_SI_flat_coro corobase_flat_coro_si thread_pool)
