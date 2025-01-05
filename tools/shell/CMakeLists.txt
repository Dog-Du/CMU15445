set(SHELL_SOURCES shell.cpp)
add_executable(shell ${SHELL_SOURCES})

target_link_libraries(shell bustub bustub_linenoise argparse utf8proc)
set_target_properties(shell PROPERTIES OUTPUT_NAME bustub-shell)

# Configure CCache if available
find_program(CCACHE_FOUND ccache)
if(CCACHE_FOUND)
        set_property(GLOBAL PROPERTY RULE_LAUNCH_COMPILE ccache)
        set_property(GLOBAL PROPERTY RULE_LAUNCH_LINK ccache)
endif(CCACHE_FOUND)
