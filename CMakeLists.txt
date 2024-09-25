cmake_minimum_required(VERSION 3.10)
project(cps)

find_package(Boost 1.65.1 REQUIRED)

include_directories(${Boost_INCLUDE_DIRS})
include_directories(include)

add_subdirectory(include) 
add_subdirectory(tests)