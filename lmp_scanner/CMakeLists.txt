cmake_minimum_required(VERSION 3.15)
project(lmp_scanner CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Aggressive optimization flags
set(CMAKE_CXX_FLAGS_RELEASE "-O3 -march=native -flto -ffast-math -funroll-loops")

# Build executable
add_executable(lmp_scanner
    main.cpp
    scanner.cpp
    output.cpp
)

# Link threading library
find_package(Threads REQUIRED)
target_link_libraries(lmp_scanner Threads::Threads)

# Default to Release build
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif()