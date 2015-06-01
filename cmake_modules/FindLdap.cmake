# - Find Openldap
# This module defines
#  LDAP_INCLUDE_DIR, where to find LDAP headers
#  LDAP_STATIC_LIBRARY, the LDAP library to use.
#  LBER_STATIC_LIBRARY, a support library for LDAP.
#  LDAP_FOUND, If false, do not try to use.

set(THIRDPARTY_LDAP thirdparty/openldap-$ENV{IMPALA_OPENLDAP_VERSION})

set (THIRDPARTY ${CMAKE_SOURCE_DIR}/thirdparty)
set(LDAP_SEARCH_LIB_PATH
  $ENV{IMPALA_TOOLCHAIN}/openldap-$ENV{IMPALA_OPENLDAP_VERSION}/lib
  ${THIRDPARTY}/openldap-$ENV{IMPALA_OPENLDAP_VERSION}/impala_install/lib
)

set(LDAP_INCLUDE_DIR
  $ENV{IMPALA_TOOLCHAIN}/openldap-$ENV{IMPALA_OPENLDAP_VERSION}/include
  ${THIRDPARTY_LDAP}/impala_install/include
)

find_library(LDAP_STATIC_LIBRARY libldap.a
  PATHS ${LDAP_SEARCH_LIB_PATH}
        NO_DEFAULT_PATH
        DOC   "Static Openldap library"
)

find_library(LBER_STATIC_LIBRARY liblber.a
  PATHS ${LDAP_SEARCH_LIB_PATH}
        NO_DEFAULT_PATH
        DOC   "Static Openldap lber library"
)

if (LDAP_STATIC_LIBRARY STREQUAL "LDAP_STATIC_LIBRARY-NOTFOUND" OR
    LBER_STATIC_LIBRARY STREQUAL "LBER_STATIC_LIBRARY-NOTFOUND")
  message(FATAL_ERROR "LDAP includes and libraries NOT found.")
endif ()


mark_as_advanced(
  LDAP_STATIC_LIBRARY
  LBER_STATIC_LIBRARY
  LDAP_INCLUDE_DIR
)
