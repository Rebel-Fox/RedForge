cmake_minimum_required(VERSION 3.13)

project(redforge LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)

add_executable(redforge src/server.cpp)

target_link_libraries(redforge PRIVATE Threads::Threads)

target_compile_options(redforge PRIVATE
    -O3
    -Wall
    -Wextra
    -Wpedantic
)
