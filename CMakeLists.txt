cmake_minimum_required(VERSION 3.10)

# ------------------------------------------------------------
# Project setup
# ------------------------------------------------------------
set(TARGET_NAME read_rdf)

project(${TARGET_NAME} LANGUAGES C CXX)

set(EXTENSION_NAME         ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

set(EXTENSION_SOURCES
    src/read_rdf_extension.cpp
    src/serd_buffer.cpp
)

# ------------------------------------------------------------
# SERD — via vcpkg
# ------------------------------------------------------------
find_package(serd CONFIG REQUIRED)

# ------------------------------------------------------------
# DuckDB extension: static + loadable
# ------------------------------------------------------------
build_static_extension(
    ${TARGET_NAME}
    ${EXTENSION_SOURCES}
)

build_loadable_extension(
    ${TARGET_NAME}
    ""
    ${EXTENSION_SOURCES}
)

# Add target-local include paths
target_include_directories(${EXTENSION_NAME}
    PRIVATE
        src/include
)
target_include_directories(${LOADABLE_EXTENSION_NAME}
    PRIVATE
        src/include
)

# Link SERD from vcpkg
target_link_libraries(${EXTENSION_NAME}
    PRIVATE
        serd::serd
)

target_link_libraries(${LOADABLE_EXTENSION_NAME}
    PRIVATE
        serd::serd
)

install(
    TARGETS ${EXTENSION_NAME}
    EXPORT  "${DUCKDB_EXPORT_SET}"
    LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
    ARCHIVE DESTINATION "${INSTALL_LIB_DIR}"
)
