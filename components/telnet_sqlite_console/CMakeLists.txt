idf_component_register(
    SRCS "telnet_sqlite_console.c"
    INCLUDE_DIRS "."
    REQUIRES freertos lwip sqlite3
)
