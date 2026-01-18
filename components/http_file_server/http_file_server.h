#pragma once

#include "esp_err.h"
#include "esp_http_server.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
    const char *base_path;        // bv "/spiffs" of "/littlefs" of "/sdcard/www"
    const char *uri_prefix;       // bv "/static" (bestanden via /static/...)
    const char *index_path;       // bv "/index.html"
    bool cache_control_no_store;  // handig voor development
} http_file_server_config_t;

/**
 * Registreert handlers:
 *  - GET  <uri_prefix>/  *  -> serve files uit <base_path>
 *  - GET  /                 -> serve <base_path><index_path>
 *
 * Voorbeeld:
 *  base_path="/spiffs", uri_prefix="/static", index_path="/index.html"
 *  - "/" -> "/spiffs/index.html"
 *  - "/static/app.js" -> "/spiffs/app.js"
 */
esp_err_t http_file_server_register(httpd_handle_t server, const http_file_server_config_t *cfg);

#ifdef __cplusplus
}
#endif
