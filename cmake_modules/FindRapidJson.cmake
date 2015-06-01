# - Find rapidjson headers and lib.
# This module defines RAPIDJSON_INCLUDE_DIR, the directory containing headers

set(RAPIDJSON_SEARCH_HEADER_PATHS
  $ENV{IMPALA_TOOLCHAIN}/rapidjson-$ENV{IMPALA_RAPIDJSON_VERSION}/include
  ${CMAKE_SOURCE_DIR}/thirdparty/rapidjson/include/
)

find_path(RAPIDJSON_INCLUDE_DIR rapidjson/rapidjson.h HINTS
  ${RAPIDJSON_SEARCH_HEADER_PATHS})

if (RAPIDJSON_INCLUDE_DIR STREQUAL "RAPIDJSON_INCLUDE_DIR-NOTFOUND")
  message(FATA_ERROR "RapidJson headers NOT found.")
endif ()

mark_as_advanced(
  RAPIDJSON_INCLUDE_DIR
)
