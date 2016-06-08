cmake_minimum_required(VERSION 2.8)
project(s3-perf)
find_package(aws-sdk-cpp)
add_executable(s3-perf main.cpp)
target_link_libraries(s3-perf aws-cpp-sdk-s3 aws-cpp-sdk-core)
set(CMAKE_CXX_FLAGS "-Wall -fno-rtti")

