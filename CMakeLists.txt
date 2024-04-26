cmake_minimum_required(VERSION 3.6)

project(Rep VERSION 1.0)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)

option(CHECK_ATOMIC "check txn atomic" OFF)
if (CHECK_ATOMIC)
add_definitions(-DCHECK_ATOMIC)
endif()

add_definitions(-DTEST)

message(STATUS "CMAKE_SYSTEM: ${CMAKE_SYSTEM}")
message(STATUS "CMAKE_VERSION: ${CMAKE_VERSION}")

message(STATUS "CMAKE_CXX_COMPILER: ${CMAKE_CXX_COMPILER}")
message(STATUS "CMAKE_CXX_FLAGS: ${CMAKE_CXX_FLAGS}")
message(STATUS "CMAKE_INCLUDE_PATH: ${CMAKE_INCLUDE_PATH}")
message(STATUS "CMAKE_LIBRARY_PATH: ${CMAKE_LIBRARY_PATH}")
message(STATUS "PROJECT_NAME: ${PROJECT_NAME}")

add_subdirectory(proto)

add_executable(client cmain.cc 
      ./client/client.cc
      ./common/common.cc
      ./common/utils.cc
      ./client/tpcc.cc)

add_executable(server smain.cc
      ./server/server.cc
      ./server/epoch.cc
      ./common/common.cc
      ./storage/storage.cc
      ./common/utils.cc)

add_executable(proxy pmain.cc
      ./proxy/proxy.cc
      ./client/client.cc
      ./common/common.cc
      ./common/utils.cc
      ./client/tpcc.cc)

target_include_directories(client PRIVATE
      ${PROJECT_SOURCE_DIR}/proto/include 
      ${PROJECT_SOURCE_DIR}/client
      ${PROJECT_SOURCE_DIR}/common)

target_include_directories(server PRIVATE
      ${PROJECT_SOURCE_DIR}/proto/include
      ${PROJECT_SOURCE_DIR}/server
      ${PROJECT_SOURCE_DIR}/common
      ${PROJECT_SOURCE_DIR}/storage)

target_include_directories(proxy PRIVATE
      ${PROJECT_SOURCE_DIR}/proto/include 
      ${PROJECT_SOURCE_DIR}/proxy
      ${PROJECT_SOURCE_DIR}/common
      ${PROJECT_SOURCE_DIR}/client)

target_link_libraries(client PRIVATE Proto pthread zmqpp zmq glog)

target_link_libraries(server PRIVATE Proto pthread zmqpp zmq glog)

target_link_libraries(proxy PRIVATE Proto pthread zmqpp zmq glog)

# 编译器警告信息flag
set(gcc_like_cxx "$<COMPILE_LANG_AND_ID:CXX,ARMClang,AppleClang,Clang,GNU,LCC>")
set(msvc_cxx "$<COMPILE_LANG_AND_ID:CXX,MSVC>")
target_compile_options(server INTERFACE
  "$<${gcc_like_cxx}:$<BUILD_INTERFACE:-Wall;-Wextra;-Wshadow;-Wformat=2;-Wunused>>"
  "$<${msvc_cxx}:$<BUILD_INTERFACE:-W3>>"
)
target_compile_options(client INTERFACE
  "$<${gcc_like_cxx}:$<BUILD_INTERFACE:-Wall;-Wextra;-Wshadow;-Wformat=2;-Wunused>>"
  "$<${msvc_cxx}:$<BUILD_INTERFACE:-W3>>"
)