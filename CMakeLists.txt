cmake_minimum_required(VERSION 3.15)

if ("${CMAKE_BINARY_DIR}" STREQUAL "${CMAKE_SOURCE_DIR}")
  message(FATAL_ERROR "In-source builds are not allowed.
    Please create a subfolder and use `cmake ..` inside it.
    NOTE: cmake creates CMakeCache.txt and CMakeFiles/*.
          Remove them, or cmake will refuse to work.")
endif()

project(Direct_access_mr C)

set(CMAKE_C_STANDARD 99)
set(CMAKE_C_STANDARD_REQUIRED ON)

add_compile_options(-Wall -Wextra -Wno-unused-parameter)

include(FetchContent)

if(CMAKE_VERSION VERSION_GREATER_EQUAL 3.28)
    Fetchcontent_Declare(
        Msg_pass
        GIT_REPOSITORY https://github.com/wjnlim/msg_pass.git
        EXCLUDE_FROM_ALL
    )
    FetchContent_MakeAvailable(Msg_pass)

else()
    Fetchcontent_Declare(
        Msg_pass
        GIT_REPOSITORY https://github.com/wjnlim/msg_pass.git
    )
    FetchContent_GetProperties(Msg_pass)
    string(TOLOWER Msg_pass lc_Msg_pass)
    if(NOT ${lc_Msg_pass}_POPULATED)
        FetchContent_Populate(Msg_pass)
        add_subdirectory(${${lc_Msg_pass}_SOURCE_DIR} ${${lc_Msg_pass}_BINARY_DIR} EXCLUDE_FROM_ALL)
    endif()
endif()

add_library(direct_access_mr_objs OBJECT 
    # src/job.c
    src/mapreduce.c
    # src/master.c
    # src/task.c
    # src/worker.c
)
target_include_directories(direct_access_mr_objs 
    PUBLIC
    ${PROJECT_SOURCE_DIR}/include
)
target_compile_options(direct_access_mr_objs 
    PRIVATE
        $<$<CONFIG:Debug>: -D DEBUG_MP -O0 -g>
)
find_library(PTHREAD_LIB pthread REQUIRED)
if (PTHREAD_LIB)
    target_link_libraries(direct_access_mr_objs PUBLIC ${PTHREAD_LIB})
endif()
if(DEBUG_MP)
    target_compile_definitions(direct_access_mr_objs PRIVATE DEBUG_MP)
endif()
# link to utils_objs, utils_ds_objs, ep_engine_objs, mp_objs to get usage requirements
target_link_libraries(direct_access_mr_objs PUBLIC 
    utils_objs
    utils_ds_objs
    ep_engine_objs
    mp_objs
)

add_library(direct_access_mr
    # $<TARGET_OBJECTS:mp_objs>
)
target_link_libraries(direct_access_mr PUBLIC
    direct_access_mr_objs
    mp_objs
    ep_engine_objs
    utils_objs
    utils_ds_objs
)

add_executable(master src/master.c src/job.c)
target_link_libraries(master PRIVATE 
    direct_access_mr_objs
    mp_objs
    ep_engine_objs
    utils_objs
    utils_ds_objs
)
target_compile_options(master
    PRIVATE
        $<$<CONFIG:Debug>: -O0 -g>
)

add_executable(worker src/worker.c src/task.c)
target_link_libraries(worker PRIVATE 
    direct_access_mr_objs
    mp_objs
    ep_engine_objs
    utils_objs
    utils_ds_objs
)
target_compile_options(worker
    PRIVATE
        $<$<CONFIG:Debug>: -O0 -g>
)

install(DIRECTORY include/direct_access_mr DESTINATION include)
install(TARGETS direct_access_mr
        ARCHIVE DESTINATION lib)

install(TARGETS master DESTINATION ${PROJECT_SOURCE_DIR}/bin)
install(TARGETS worker DESTINATION ${PROJECT_SOURCE_DIR}/bin)