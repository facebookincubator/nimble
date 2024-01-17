add_executable(alpha_tablet_tests TabletTests.cpp)

add_test(alpha_tablet_tests alpha_tablet_tests)

target_link_libraries(
  alpha_tablet_tests
  alpha_tablet
  alpha_common
  velox_memory
  velox_file
  gtest
  gtest_main
  glog::glog
  Folly::folly)
