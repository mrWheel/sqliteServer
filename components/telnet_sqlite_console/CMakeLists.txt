idf_component_register(
    SRCS "telnet_sqlite_console.c"
    INCLUDE_DIRS "."
    REQUIRES freertos lwip sqlite3 esp_telnet
)
# libtelnet.h zit in esp_telnet/src, niet in include/
# dus extra include pad:
target_include_directories(${COMPONENT_LIB} PRIVATE
  ${CMAKE_CURRENT_LIST_DIR}/../esp_telnet/src
)