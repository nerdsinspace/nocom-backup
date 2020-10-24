cmake_minimum_required(VERSION 3.16)
project(nocom_backup)

set(CMAKE_CXX_STANDARD 20)

set(libpqxx_DIR "${CMAKE_CURRENT_SOURCE_DIR}/libs/libpqxx/lib/cmake/libpqxx")
find_package(libpqxx REQUIRED)

add_subdirectory(args)

add_executable(nocom_backup main.cpp pqxx_extensions.h tables.h serialization.h)

target_link_libraries(nocom_backup libpqxx::pqxx)
target_link_libraries(nocom_backup args)