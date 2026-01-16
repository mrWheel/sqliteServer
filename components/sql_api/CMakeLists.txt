idf_component_register(
  SRCS "sql_api.c"
  INCLUDE_DIRS "."
  REQUIRES esp_http_server sqlite3
)