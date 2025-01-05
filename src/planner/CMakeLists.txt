add_library(
  bustub_planner
  OBJECT
  expression_factory.cpp
  plan_aggregation.cpp
  plan_expression.cpp
  plan_insert.cpp
  plan_table_ref.cpp
  plan_select.cpp
  planner.cpp)

set(ALL_OBJECT_FILES
  ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:bustub_planner>
  PARENT_SCOPE)

# Configure CCache if available
find_program(CCACHE_FOUND ccache)
if(CCACHE_FOUND)
        set_property(GLOBAL PROPERTY RULE_LAUNCH_COMPILE ccache)
        set_property(GLOBAL PROPERTY RULE_LAUNCH_LINK ccache)
endif(CCACHE_FOUND)
