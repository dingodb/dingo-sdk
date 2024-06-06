enable_testing()

find_package(HDF5 COMPONENTS CXX REQUIRED)
message("HDF5 libraries: ${HDF5_LIBRARIES}, include dir: ${HDF5_INCLUDE_DIR}")
include_directories(${HDF5_INCLUDE_DIR})

SET(BENCHMARK_BIN "dingodb_bench")

file(GLOB BENCHMARK_SRCS "*.cc")

include_directories(${CMAKE_CURRENT_SOURCE_DIR})

add_executable(${BENCHMARK_BIN}
                main.cc
                ${BENCHMARK_SRCS}
                )

target_link_libraries(${BENCHMARK_BIN}
                      PRIVATE
                      $<TARGET_OBJECTS:PROTO_OBJS>
                      sdk
                      ${HDF5_LIBRARIES}
                      ${HDF5_CXX_LIBRARIES}
                      brpc
                      )