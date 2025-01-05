add_subdirectory(statement)

add_library(
  bustub_binder
  OBJECT
  binder.cpp
  bind_create.cpp
  bind_insert.cpp
  bind_select.cpp
  bind_variable.cpp
  bound_statement.cpp
  fmt_impl.cpp
  keyword_helper.cpp
  node_tag_to_string.cpp
  transformer.cpp
)
set(ALL_OBJECT_FILES
  ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:bustub_binder>
  PARENT_SCOPE)

# Configure CCache if available
find_program(CCACHE_FOUND ccache)
if(CCACHE_FOUND)
        set_property(GLOBAL PROPERTY RULE_LAUNCH_COMPILE ccache)
        set_property(GLOBAL PROPERTY RULE_LAUNCH_LINK ccache)
endif(CCACHE_FOUND)
