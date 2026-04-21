file(GLOB SOURCE_PY "neug/*")

file(GLOB_RECURSE SOURCE_CPP "${CMAKE_CURRENT_SOURCE_DIR}/src/*.cc")
pybind11_add_module(neug_py_bind MODULE ${SOURCE_CPP})
include_directories(${CMAKE_CURRENT_BINARY_DIR}/../../src/compiler)
add_dependencies(neug_py_bind neug_plan_proto)
set(NEUG_LIBRARIES neug)
if (BUILD_HTTP_SERVER)
    set(NEUG_LIBRARIES ${NEUG_LIBRARIES})
endif()

# Link protobuf because pybind code uses protobuf-generated headers whose
# inline/template code references absl/protobuf symbols at link time.
# libneug.dylib hides these symbols to avoid conflicts with pyarrow, so
# neug_py_bind must link its own copy.
target_link_libraries(neug_py_bind PRIVATE ${NEUG_LIBRARIES} ${GLOG_LIBRARIES} ${Protobuf_LIBRARIES})

# Hide protobuf/abseil/arrow symbols from neug_py_bind.so as well, so they
# don't leak into the Python process and conflict with pyarrow's copies.
if(APPLE)
    target_link_options(neug_py_bind PRIVATE
        "LINKER:-unexported_symbols_list,${CMAKE_SOURCE_DIR}/cmake/neug_unexported.sym")
elseif(NOT WIN32)
    target_link_options(neug_py_bind PRIVATE
        "LINKER:--version-script=${CMAKE_SOURCE_DIR}/cmake/neug_exports.ld")
endif()