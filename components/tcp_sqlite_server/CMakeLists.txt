idf_component_register(
    SRCS "tcp_sqlite_server.c"
    INCLUDE_DIRS "."
    REQUIRES freertos lwip sqlite3 json
)
