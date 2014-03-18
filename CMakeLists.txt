CMAKE_MINIMUM_REQUIRED(VERSION 2.8)

INCLUDE(CheckIncludeFiles)

INCLUDE_DIRECTORIES(BEFORE ${DEPS_INCLUDE_DIR}
                           ${LIBEVENT_INCLUDE_DIR}
                           ${CMAKE_INSTALL_PREFIX}/include
                           ${CMAKE_CURRENT_SOURCE_DIR}/include
                           ${CMAKE_CURRENT_SOURCE_DIR}/../platform/include
                           ${CMAKE_CURRENT_SOURCE_DIR}/../libconflate/include
                           ${CMAKE_CURRENT_SOURCE_DIR}/../libmemcached
                           ${CMAKE_CURRENT_BINARY_DIR}
                           ${CMAKE_CURRENT_SOURCE_DIR})

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
