idf_component_register(
  SRCS "db_psram.c"
  INCLUDE_DIRS "."
  REQUIRES sqlite3 esp_psram heap
)
