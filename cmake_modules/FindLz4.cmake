# - Find LZ4 (lz4.h, liblz4.a, liblz4.so, and liblz4.so.1)
# This module defines
# LZ4_INCLUDE_DIR, directory containing headers
# LZ4_LIBS, directory containing lz4 libraries
# LZ4_STATIC_LIB, path to liblz4.a
# LZ4_FOUND, whether lz4 has been found

set(LZ4_SEARCH_LIB_PATH
  $ENV{IMPALA_TOOLCHAIN}/lz4-$ENV{IMPALA_LZ4_VERSION}/lib
  ${CMAKE_SOURCE_DIR}/thirdparty/lz4
)

set(LZ4_SEARCH_INCLUDE_DIR
  $ENV{IMPALA_TOOLCHAIN}/lz4-$ENV{IMPALA_LZ4_VERSION}/include
  ${CMAKE_SOURCE_DIR}/thirdparty/lz4
)

find_path(LZ4_INCLUDE_DIR lz4.h
  PATHS ${LZ4_SEARCH_INCLUDE_DIR}
  NO_DEFAULT_PATH
  DOC "Path to LZ4 headers"
  )

find_library(LZ4_LIBS NAMES lz4
  PATHS ${LZ4_SEARCH_LIB_PATH}
        NO_DEFAULT_PATH
  DOC "Path to LZ4 library"
)

find_library(LZ4_STATIC_LIB NAMES liblz4.a
  PATHS ${LZ4_SEARCH_LIB_PATH}
        NO_DEFAULT_PATH
  DOC "Path to LZ4 static library"
)

if (LZ4_LIBS STREQUAL "LZ4_LIBS-NOTFOUND" OR
    LZ4_STATIC_LIB STREQUAL "LZ4_STATIC_LIB-NOTFOUND")
  message(FATAL_ERROR "Lz4 includes and libraries NOT found. "
    "Looked for headers in ${LZ4_SEARCH_HEADER_PATH}, "
    "and for libs in ${LZ4_SEARCH_LIB_PATH}")
endif ()

mark_as_advanced(
  LZ4_INCLUDE_DIR
  LZ4_LIBS
  LZ4_STATIC_LIB
)
