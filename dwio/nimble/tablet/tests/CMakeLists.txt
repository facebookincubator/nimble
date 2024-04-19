add_executable(nimble_tablet_tests TabletTests.cpp)

add_test(nimble_tablet_tests nimble_tablet_tests)

target_link_libraries(
  nimble_tablet_tests
  nimble_tablet
  nimble_common
  velox_memory
  velox_file
  gtest
  gtest_main
  glog::glog
  Folly::folly)
