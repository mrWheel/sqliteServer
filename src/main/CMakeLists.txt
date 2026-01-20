idf_component_register(
  SRCS "main.c"
  INCLUDE_DIRS "."
  REQUIRES sdcard sqlite3 telnet_sqlite_console tcp_sqlite_server mdns
)
