idf_component_register(
  SRCS "main.c"
  INCLUDE_DIRS "."
  REQUIRES sdcard sql_api sqlite3 http_file_server telnet_sqlite_console mdns
)
