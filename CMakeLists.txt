CMAKE_MINIMUM_REQUIRED(VERSION 2.8)

INCLUDE(CheckIncludeFiles)

INCLUDE_DIRECTORIES(BEFORE ${DEPS_INCLUDE_DIR}
                           ${LIBEVENT_INCLUDE_DIR}
                           ${CMAKE_INSTALL_PREFIX}/include
                           ${CMAKE_CURRENT_SOURCE_DIR}/include
                           ${CMAKE_CURRENT_SOURCE_DIR}
                           ${CMAKE_CURRENT_SOURCE_DIR}/../platform/include
                           ${CMAKE_CURRENT_SOURCE_DIR}/../libvbucket/include
                           ${CMAKE_CURRENT_SOURCE_DIR}/../libconflate/include
                           ${CMAKE_CURRENT_SOURCE_DIR}/../libmemcached)

IF (WIN32)
   INCLUDE_DIRECTORIES(AFTER ${CMAKE_CURRENT_SOURCE_DIR}/../platform/include/win32)
ENDIF (WIN32)

CHECK_INCLUDE_FILES("umem.h" HAVE_UMEM_H)
CHECK_INCLUDE_FILES("sysexits.h" HAVE_SYSEXITS_H)

SET(CONFLATE_DB_PATH ${CMAKE_INSTALL_PREFIX}/var/lib/moxi)

EXECUTE_PROCESS(COMMAND git describe
                WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
                OUTPUT_VARIABLE MOXI_VERSION
                ERROR_QUIET
                OUTPUT_STRIP_TRAILING_WHITESPACE)

CONFIGURE_FILE (${CMAKE_CURRENT_SOURCE_DIR}/config.cmake.h
                ${CMAKE_CURRENT_BINARY_DIR}/config.h)

ADD_EXECUTABLE(moxi_sizes sizes.c)
ADD_EXECUTABLE(moxi_htgram_test htgram_test.c htgram.c)

ADD_EXECUTABLE(moxi
               memcached.c genhash.c hash.c slabs.c items.c assoc.c
               thread.c stats.c util.c work.c cproxy.c cproxy_config.c
               cproxy_protocol_a.c cproxy_protocol_a2a.c
               cproxy_protocol_a2b.c cproxy_protocol_b.c
               cproxy_protocol_b2b.c cproxy_multiget.c cproxy_stats.c
               cproxy_front.c matcher.c murmur_hash.c mcs.c
               stdin_check.c log.c htgram.c agent_config.c
               agent_ping.c agent_stats.c daemon.c cache.c strsep.c)

TARGET_LINK_LIBRARIES(moxi conflate vbucket platform mcd ${LIBEVENT_LIBRARIES} ${COUCHBASE_NETWORK_LIBS})

INSTALL(TARGETS moxi
        RUNTIME DESTINATION bin)

ADD_TEST(moxi-sizes moxi_sizes)
ADD_TEST(moxi-htgram-test moxi_htgram_test)
