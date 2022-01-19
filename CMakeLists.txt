cmake_minimum_required(VERSION 3.16)
project(nocom_backup)

set(CMAKE_CXX_STANDARD 20)

find_package(PkgConfig)
find_package(simdjson)
pkg_check_modules(LIBPQXX REQUIRED IMPORTED_TARGET libpqxx)

add_subdirectory(args)

add_executable(nocom_backup main.cpp pqxx_extensions.h tables.h serialization.h)

target_link_libraries(nocom_backup args)
target_link_libraries(nocom_backup PkgConfig::LIBPQXX)
target_link_libraries(nocom_backup simdjson)
