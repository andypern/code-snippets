cmake_minimum_required(VERSION 2.8)
project(s3-sample)
find_package(aws-sdk-cpp)
add_executable(s3-sample main.cpp)
target_link_libraries(s3-sample aws-cpp-sdk-s3 aws-cpp-sdk-core)
set(CMAKE_CXX_FLAGS "-Wall -fno-rtti")

