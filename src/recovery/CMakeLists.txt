add_library(
  bustub_recovery
  OBJECT
  checkpoint_manager.cpp
  log_manager.cpp
  log_recovery.cpp)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:bustub_recovery>
    PARENT_SCOPE)

# Configure CCache if available
find_program(CCACHE_FOUND ccache)
if(CCACHE_FOUND)
        set_property(GLOBAL PROPERTY RULE_LAUNCH_COMPILE ccache)
        set_property(GLOBAL PROPERTY RULE_LAUNCH_LINK ccache)
endif(CCACHE_FOUND)
