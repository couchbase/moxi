PROJECT(Moxi)
CMAKE_MINIMUM_REQUIRED(VERSION 2.8)

ADD_SUBDIRECTORY(libmemcached)

INCLUDE(CheckIncludeFiles)

INCLUDE_DIRECTORIES(BEFORE ${LIBEVENT_INCLUDE_DIR}
                           ${CMAKE_INSTALL_PREFIX}/include
                           ${CMAKE_CURRENT_SOURCE_DIR}/include
                           ${CMAKE_SOURCE_DIR}/platform/include
                           ${PROJECT_SOURCE_DIR}/libmemcached
                           ${CMAKE_CURRENT_BINARY_DIR}
                           ${CMAKE_CURRENT_SOURCE_DIR})

INCLUDE_DIRECTORIES(AFTER ${CURL_INCLUDE_DIRS})


ADD_LIBRARY(conflate SHARED
            conflate/adhoc_commands.c
            conflate/conflate.c
            conflate/conflate_internal.h
            conflate/kvpair.c
            conflate/logging.c
            conflate/persist.c
            conflate/rest.c
            conflate/rest.h
            conflate/util.c
            conflate/xmpp.c)

IF(WIN32)
    INCLUDE_DIRECTORIES(AFTER ${CMAKE_SOURCE_DIR}/platform/include/win32)
    ADD_DEFINITIONS(-Dsnprintf=_snprintf)
ELSE(WIN32)
    SET(ZLIB z)
ENDIF(WIN32)

TARGET_LINK_LIBRARIES(conflate ${CURL_LIBRARIES} platform ${ZLIB})
SET_TARGET_PROPERTIES(conflate PROPERTIES SOVERSION 1.0.0)
SET_TARGET_PROPERTIES(conflate PROPERTIES COMPILE_FLAGS
                               -DBUILDING_LIBCONFLATE=1)

IF (INSTALL_HEADER_FILES)
    INSTALL(FILES include/libconflate/conflate.h
            DESTINATION include/libconflate)
ENDIF (INSTALL_HEADER_FILES)

INSTALL(TARGETS conflate
        RUNTIME DESTINATION bin
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib)

ADD_EXECUTABLE(tests_check_kvpair
               include/libconflate/conflate.h
               tests/conflate/check_kvpair.c
               tests/conflate/test_common.c
               tests/conflate/test_common.h)
TARGET_LINK_LIBRARIES(tests_check_kvpair conflate)
ADD_TEST(libconflate-test-suite tests_check_kvpair)

ADD_LIBRARY(vbucket SHARED
            include/libvbucket/vbucket.h
            include/libvbucket/visibility.h
            vbucket/crc32.c
            vbucket/hash.h
            vbucket/hash.h
            vbucket/ketama.c
            vbucket/rfc1321/global.h
            vbucket/rfc1321/md5.h
            vbucket/vbucket.c)

SET_TARGET_PROPERTIES(vbucket PROPERTIES SOVERSION 1.1.1)
SET_TARGET_PROPERTIES(vbucket PROPERTIES
                      COMPILE_FLAGS -DBUILDING_LIBVBUCKET=1)
SET_TARGET_PROPERTIES(vbucket PROPERTIES INSTALL_NAME_DIR ${CMAKE_INSTALL_PREFIX}/lib)

IF (WIN32)
    TARGET_LINK_LIBRARIES(vbucket cJSON)
ELSE (WIN32)
    TARGET_LINK_LIBRARIES(vbucket cJSON m)
ENDIF (WIN32)

IF (INSTALL_HEADER_FILES)
   INSTALL(FILES
           include/libvbucket/vbucket.h
           include/libvbucket/visibility.h
           DESTINATION include/libvbucket)
ENDIF(INSTALL_HEADER_FILES)

INSTALL(TARGETS vbucket
        RUNTIME DESTINATION bin
        LIBRARY DESTINATION lib
        ARCHIVE DESTINATION lib)

ADD_EXECUTABLE(vbuckettool
               include/libvbucket/vbucket.h
               include/libvbucket/visibility.h
               vbucket/vbuckettool.c)
TARGET_LINK_LIBRARIES(vbuckettool vbucket)

ADD_EXECUTABLE(vbucketkeygen
               include/libvbucket/vbucket.h
               include/libvbucket/visibility.h
               vbucket/vbucketkeygen.c)
TARGET_LINK_LIBRARIES(vbucketkeygen vbucket)

#
# The tests. These are automatically executed as part of the build!
#

ADD_EXECUTABLE(vbucket_testapp
               include/libvbucket/vbucket.h
               include/libvbucket/visibility.h
               tests/vbucket/macros.h
               tests/vbucket/testapp.c)
TARGET_LINK_LIBRARIES(vbucket_testapp vbucket)

ADD_EXECUTABLE(vbucket_regression
               include/libvbucket/vbucket.h
               include/libvbucket/visibility.h
               tests/vbucket/macros.h
               tests/vbucket/regression.c)
TARGET_LINK_LIBRARIES(vbucket_regression vbucket)

ADD_EXECUTABLE(vbucket_testketama
               vbucket/ketama.c
               vbucket/rfc1321/global.h
               vbucket/rfc1321/md5.h
               tests/vbucket/testketama.c)
TARGET_LINK_LIBRARIES(vbucket_testketama vbucket)

ADD_TEST(vbucket-basic-tests vbucket_testapp ${CMAKE_CURRENT_SOURCE_DIR})
ADD_TEST(vbucket-regression-tests vbucket_regression ${CMAKE_CURRENT_SOURCE_DIR})
ADD_TEST(vbucket-ketama-tests vbucket_testketama)





IF ("${CMAKE_SYSTEM_NAME}" STREQUAL "SunOS")
   SET(UMEM_LIBRARY -lumem)
   SET(PRVILEGES_SOURCES src/solaris_priv.c)
ENDIF ("${CMAKE_SYSTEM_NAME}" STREQUAL "SunOS")

IF (WIN32)
   INCLUDE_DIRECTORIES(AFTER ${CMAKE_CURRENT_SOURCE_DIR}/../platform/include/win32)
ENDIF (WIN32)

CHECK_INCLUDE_FILES("umem.h" HAVE_UMEM_H)
CHECK_INCLUDE_FILES("sysexits.h" HAVE_SYSEXITS_H)
CHECK_FUNCTION_EXISTS(getpwnam HAVE_GETPWNAM)
CHECK_FUNCTION_EXISTS(getrlimit HAVE_GETRLIMIT)
CHECK_FUNCTION_EXISTS(mlockall HAVE_MLOCKALL)
CHECK_FUNCTION_EXISTS(getpagesizes HAVE_GETPAGESIZES)

SET(CONFLATE_DB_PATH ${CMAKE_INSTALL_PREFIX}/var/lib/moxi)

EXECUTE_PROCESS(COMMAND git describe
                WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
                OUTPUT_VARIABLE MOXI_VERSION
                ERROR_QUIET
                OUTPUT_STRIP_TRAILING_WHITESPACE)

CONFIGURE_FILE (${CMAKE_CURRENT_SOURCE_DIR}/src/config.cmake.h
                ${CMAKE_CURRENT_BINARY_DIR}/src/config.h)

ADD_EXECUTABLE(moxi_sizes tests/moxi/sizes.c)
ADD_EXECUTABLE(moxi_htgram_test tests/moxi/htgram_test.c src/htgram.c)

ADD_EXECUTABLE(moxi
               src/memcached.c src/genhash.c src/hash.c src/slabs.c
               src/items.c src/assoc.c src/thread.c src/stats.c
               src/util.c src/work.c src/cproxy.c src/cproxy_config.c
               src/cproxy_protocol_a.c src/cproxy_protocol_a2a.c
               src/cproxy_protocol_a2b.c src/cproxy_protocol_b.c
               src/cproxy_protocol_b2b.c src/cproxy_multiget.c
               src/cproxy_stats.c src/cproxy_front.c src/matcher.c
               src/murmur_hash.c src/mcs.c src/stdin_check.c src/log.c
               src/htgram.c src/agent_config.c src/agent_ping.c
               src/agent_stats.c src/daemon.c src/cache.c src/strsep.c
               ${PRVILEGES_SOURCES})

TARGET_LINK_LIBRARIES(moxi conflate vbucket platform mcd ${LIBEVENT_LIBRARIES} ${COUCHBASE_NETWORK_LIBS} ${UMEM_LIBRARY})

INSTALL(TARGETS moxi
        RUNTIME DESTINATION bin)

ADD_TEST(moxi-sizes moxi_sizes)
ADD_TEST(moxi-htgram-test moxi_htgram_test)
