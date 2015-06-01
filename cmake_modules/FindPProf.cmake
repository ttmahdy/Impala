# - Find pprof (libprofiler.a)
# This module defines
#  PPROF_INCLUDE_DIR, directory containing headers
#  PPROF_LIBS, directory containing pprof libraries
#  PPROF_STATIC_LIB, path to libprofiler.a
#  PPROF_FOUND, whether pprof has been found

set(PPROF_SEARCH_HEADER_PATHS
  $ENV{IMPALA_TOOLCHAIN}/gperftools-$ENV{IMPALA_GPERFTOOLS_VERSION}/include
  ${CMAKE_SOURCE_DIR}/thirdparty/gperftools-2.0/src
)

set(PPROF_SEARCH_LIB_PATH
  $ENV{IMPALA_TOOLCHAIN}/gperftools-$ENV{IMPALA_GPERFTOOLS_VERSION}/lib
  ${CMAKE_SOURCE_DIR}/thirdparty/gperftools-2.0/.libs
)

find_path(PPROF_INCLUDE_DIR google/profiler.h PATHS
  ${PPROF_SEARCH_HEADER_PATHS}
  # make sure we don't accidentally pick up a different version
  NO_DEFAULT_PATH
)

find_library(PPROF_LIB_PATH profiler ${PPROF_SEARCH_LIB_PATH})
find_library(PPROF_STATIC_LIB libprofiler.a ${PPROF_SEARCH_LIB_PATH})
find_library(HEAPPROF_STATIC_LIB libtcmalloc.a ${PPROF_SEARCH_LIB_PATH})

if (PPROF_LIB_PATH STREQUAL "PPROF_LIB_PATH-NOTFOUND" OR
    PPROF_STATIC_LIB STREQUAL "PPROF_STATIC_LIB-NOTFOUND" OR
    HEAPPROF_STATIC_LIB STREQUAL "HEAPPROF_STATIC_LIB-NOTFOUND")
  message(FATAL_ERROR "gperftools libraries NOT found. "
    "Looked for libs in ${PPROF_SEARCH_LIB_PATH}")
endif ()

mark_as_advanced(
  PPROF_INCLUDE_DIR
  PPROF_LIBS
  PPROF_STATIC_LIB
)
