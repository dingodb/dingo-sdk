enable_testing()

SET(INTEGRATION_TEST_BIN "dingodb_integration_test")

file(GLOB REPORT_SRCS ${CMAKE_CURRENT_SOURCE_DIR}/report/*.cc)
file(GLOB COMMON_SRCS ${CMAKE_CURRENT_SOURCE_DIR}/common/*.cc)

set(INTEGRATION_TEST_DEPEND_SRCS
  ${REPORT_SRCS}
  ${COMMON_SRCS}
)

file(GLOB INTEGRATION_TEST_SRCS "*.cc")
list(REMOVE_ITEM INTEGRATION_TEST_SRCS "main.cc")

include_directories(${CMAKE_CURRENT_SOURCE_DIR})

add_executable(${INTEGRATION_TEST_BIN}
                main.cc
                ${INTEGRATION_TEST_DEPEND_SRCS}
                ${INTEGRATION_TEST_SRCS}
                )

target_link_libraries(${INTEGRATION_TEST_BIN}
  PRIVATE
    sdk
    GTest::gtest
)
