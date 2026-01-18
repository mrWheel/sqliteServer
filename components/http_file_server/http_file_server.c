#include "http_file_server.h"

#include <stdio.h>
#include <string.h>
#include <sys/stat.h>

#include "esp_log.h"

static const char *TAG = "FILESRV";

// Config wordt gekopieerd naar statische storage (simpel en veilig)
static char s_base_path[128];
static char s_uri_prefix[32];
static char s_index_path[64];
static bool s_no_store = true;

static const char *mime_from_path(const char *path)
{
    const char *ext = strrchr(path, '.');
    if (!ext) return "application/octet-stream";
    ext++;

    if (!strcasecmp(ext, "html")) return "text/html";
    if (!strcasecmp(ext, "css"))  return "text/css";
    if (!strcasecmp(ext, "js"))   return "application/javascript";
    if (!strcasecmp(ext, "json")) return "application/json";
    if (!strcasecmp(ext, "png"))  return "image/png";
    if (!strcasecmp(ext, "jpg") || !strcasecmp(ext, "jpeg")) return "image/jpeg";
    if (!strcasecmp(ext, "svg"))  return "image/svg+xml";
    if (!strcasecmp(ext, "ico"))  return "image/x-icon";
    if (!strcasecmp(ext, "txt"))  return "text/plain";
    return "application/octet-stream";
}

static esp_err_t send_file(httpd_req_t *req, const char *filepath)
{
    FILE *f = fopen(filepath, "rb");
    if (!f) {
        ESP_LOGW(TAG, "Not found: %s", filepath);
        httpd_resp_send_err(req, HTTPD_404_NOT_FOUND, "File not found");
        return ESP_FAIL;
    }

    httpd_resp_set_type(req, mime_from_path(filepath));
    if (s_no_store) {
        httpd_resp_set_hdr(req, "Cache-Control", "no-store");
    }

    char buf[1024];
    size_t n;
    while ((n = fread(buf, 1, sizeof(buf), f)) > 0) {
        esp_err_t err = httpd_resp_send_chunk(req, buf, n);
        if (err != ESP_OK) {
            fclose(f);
            httpd_resp_sendstr_chunk(req, NULL); // end
            return err;
        }
    }

    fclose(f);
    return httpd_resp_sendstr_chunk(req, NULL); // end
}

static bool has_prefix(const char *s, const char *prefix)
{
    size_t n = strlen(prefix);
    return strncmp(s, prefix, n) == 0;
}

// GET /
static esp_err_t root_get_handler(httpd_req_t *req)
{
    char filepath[512];
    snprintf(filepath, sizeof(filepath), "%s%s", s_base_path, s_index_path);
    ESP_LOGI(TAG, "GET / -> %s", filepath);
    return send_file(req, filepath);
}

// GET /static/*
static esp_err_t static_get_handler(httpd_req_t *req)
{
    // traversal check
    if (strstr(req->uri, "..")) {
        httpd_resp_send_err(req, HTTPD_400_BAD_REQUEST, "Bad path");
        return ESP_FAIL;
    }

    // uri is like "/static/app.js"
    // we strip "/static" and map to "<base_path>/app.js"
    if (!has_prefix(req->uri, s_uri_prefix)) {
        httpd_resp_send_err(req, HTTPD_400_BAD_REQUEST, "Bad uri");
        return ESP_FAIL;
    }

    const char *rel = req->uri + strlen(s_uri_prefix); // now "/app.js"
    if (rel[0] == '\0') rel = "/";                      // edge

    char filepath[512];
    snprintf(filepath, sizeof(filepath), "%s%s", s_base_path, rel);

    // als het een directory is -> index.html (optioneel)
    struct stat st;
    if (stat(filepath, &st) == 0 && S_ISDIR(st.st_mode)) {
        strncat(filepath, "/index.html", sizeof(filepath) - strlen(filepath) - 1);
    }

    ESP_LOGI(TAG, "GET %s -> %s", req->uri, filepath);
    return send_file(req, filepath);
}

esp_err_t http_file_server_register(httpd_handle_t server, const http_file_server_config_t *cfg)
{
    if (!server || !cfg || !cfg->base_path || !cfg->uri_prefix || !cfg->index_path) {
        return ESP_ERR_INVALID_ARG;
    }

    // copy config strings
    strncpy(s_base_path, cfg->base_path, sizeof(s_base_path) - 1);
    s_base_path[sizeof(s_base_path) - 1] = 0;

    strncpy(s_uri_prefix, cfg->uri_prefix, sizeof(s_uri_prefix) - 1);
    s_uri_prefix[sizeof(s_uri_prefix) - 1] = 0;

    strncpy(s_index_path, cfg->index_path, sizeof(s_index_path) - 1);
    s_index_path[sizeof(s_index_path) - 1] = 0;

    s_no_store = cfg->cache_control_no_store;

    // 1) Root handler ("/")
    httpd_uri_t uri_root = {
        .uri = "/",
        .method = HTTP_GET,
        .handler = root_get_handler,
        .user_ctx = NULL
    };
    esp_err_t err = httpd_register_uri_handler(server, &uri_root);
    if (err != ESP_OK) return err;

    // 2) Static handler ("/static/*")
    // Let op: dit is een wildcard URI -> je httpd_config moet uri_match_fn = httpd_uri_match_wildcard hebben
    char wildcard[64];
    snprintf(wildcard, sizeof(wildcard), "%s/*", s_uri_prefix);

    httpd_uri_t uri_static = {
        .uri = wildcard,
        .method = HTTP_GET,
        .handler = static_get_handler,
        .user_ctx = NULL
    };

    ESP_LOGI(TAG, "Serve root: %s%s", s_base_path, s_index_path);
    ESP_LOGI(TAG, "Serve static: %s/*  -> %s/", s_uri_prefix, s_base_path);

    return httpd_register_uri_handler(server, &uri_static);
}
