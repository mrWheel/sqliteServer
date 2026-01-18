#include "sql_api.h"

#include <string.h>
#include <stdlib.h>
#include <stdbool.h>
#include <ctype.h>

#include "freertos/FreeRTOS.h"
#include "freertos/semphr.h"

#include "esp_log.h"
#include "esp_http_server.h"
#include "esp_system.h"
#include "nvs_flash.h"

#include "http_file_server.h" // <-- JOUW component (niet https_file_server.h)

static const char* TAG = "SQLAPI";

static sqlite3* g_db = NULL;
static SemaphoreHandle_t g_db_mutex;
static httpd_handle_t g_server = NULL;

/* -------------------- JSON helpers (output) -------------------- */
static void json_write_escaped(httpd_req_t* req, const char* s)
{
  httpd_resp_sendstr_chunk(req, "\"");
  for (const unsigned char* p = (const unsigned char*)s; p && *p; p++)
  {
    if (*p == '\"')
      httpd_resp_sendstr_chunk(req, "\\\"");
    else if (*p == '\\')
      httpd_resp_sendstr_chunk(req, "\\\\");
    else if (*p == '\n')
      httpd_resp_sendstr_chunk(req, "\\n");
    else if (*p == '\r')
      httpd_resp_sendstr_chunk(req, "\\r");
    else if (*p == '\t')
      httpd_resp_sendstr_chunk(req, "\\t");
    else if (*p < 0x20)
    {
      char buf[8];
      snprintf(buf, sizeof(buf), "\\u%04x", (unsigned)*p);
      httpd_resp_sendstr_chunk(req, buf);
    }
    else
    {
      char c[2] = {(char)*p, 0};
      httpd_resp_sendstr_chunk(req, c);
    }
  }
  httpd_resp_sendstr_chunk(req, "\"");
}

/* -------------------- minimal JSON parsing (input) --------------------
   NOTE: bewust simpel: verwacht {"key":"value"} zonder escaped quotes.
*/
static bool json_get_str(const char* body, const char* key, char* out, size_t outlen)
{
  if (!body || !key || !out || outlen == 0)
    return false;

  char pat[48];
  snprintf(pat, sizeof(pat), "\"%s\"", key);

  const char* p = strstr(body, pat);
  if (!p)
    return false;

  p = strchr(p, ':');
  if (!p)
    return false;
  p++;

  while (*p && isspace((unsigned char)*p))
    p++;
  if (*p != '"')
    return false;
  p++;

  const char* e = strchr(p, '"');
  if (!e)
    return false;

  size_t n = (size_t)(e - p);
  if (n >= outlen)
    n = outlen - 1;
  memcpy(out, p, n);
  out[n] = 0;
  return true;
}

static esp_err_t recv_body_all(httpd_req_t* req, char** out_body, int max_len)
{
  if (!out_body)
    return ESP_ERR_INVALID_ARG;
  *out_body = NULL;

  int len = req->content_len;
  if (len <= 0 || len > max_len)
    return ESP_ERR_INVALID_SIZE;

  char* body = (char*)calloc(1, len + 1);
  if (!body)
    return ESP_ERR_NO_MEM;

  int got = 0;
  while (got < len)
  {
    int r = httpd_req_recv(req, body + got, len - got);
    if (r <= 0)
    {
      free(body);
      return ESP_FAIL;
    }
    got += r;
  }
  body[len] = 0;
  *out_body = body;
  return ESP_OK;
}

/* -------------------- NVS WiFi save -------------------- */
static esp_err_t nvs_save_wifi(const char* ssid, const char* pass)
{
  if (!ssid || ssid[0] == 0)
    return ESP_ERR_INVALID_ARG;

  nvs_handle_t h;
  esp_err_t err = nvs_open("wifi", NVS_READWRITE, &h);
  if (err != ESP_OK)
    return err;

  err = nvs_set_str(h, "ssid", ssid);
  if (err == ESP_OK)
    err = nvs_set_str(h, "pass", pass ? pass : "");
  if (err == ESP_OK)
    err = nvs_commit(h);

  nvs_close(h);
  return err;
}

/* -------------------- SQLite runner (multi-statement) --------------------
Response:
{
  "results":[
    {"type":"select","columns":["a"],"rows":[[1],[2]]},
    {"type":"ok","changes":1,"last_insert_rowid":123}
  ],
  "error": null
}
*/
static esp_err_t exec_sql_all(httpd_req_t* req, const char* sql_in)
{
  xSemaphoreTake(g_db_mutex, portMAX_DELAY);

  const char* sql = sql_in;
  sqlite3_stmt* stmt = NULL;

  httpd_resp_set_type(req, "application/json");
  httpd_resp_sendstr_chunk(req, "{\"results\":[");

  bool first_result = true;

  while (1)
  {
    int rc = sqlite3_prepare_v2(g_db, sql, -1, &stmt, &sql);
    if (rc != SQLITE_OK)
    {
      const char* msg = sqlite3_errmsg(g_db);
      httpd_resp_sendstr_chunk(req, "],\"error\":");
      json_write_escaped(req, msg ? msg : "sqlite error");
      httpd_resp_sendstr_chunk(req, "}");
      httpd_resp_sendstr_chunk(req, NULL);
      if (stmt)
        sqlite3_finalize(stmt);
      xSemaphoreGive(g_db_mutex);
      return ESP_OK; // 200 met error payload
    }

    if (!stmt)
      break;

    int col_count = sqlite3_column_count(stmt);
    bool is_select_like = (col_count > 0);

    if (!first_result)
      httpd_resp_sendstr_chunk(req, ",");
    first_result = false;

    if (is_select_like)
    {
      httpd_resp_sendstr_chunk(req, "{\"type\":\"select\",\"columns\":[");
      for (int c = 0; c < col_count; c++)
      {
        if (c)
          httpd_resp_sendstr_chunk(req, ",");
        json_write_escaped(req, sqlite3_column_name(stmt, c));
      }
      httpd_resp_sendstr_chunk(req, "],\"rows\":[");

      bool first_row = true;
      int step_rc;
      while ((step_rc = sqlite3_step(stmt)) == SQLITE_ROW)
      {
        if (!first_row)
          httpd_resp_sendstr_chunk(req, ",");
        first_row = false;

        httpd_resp_sendstr_chunk(req, "[");
        for (int c = 0; c < col_count; c++)
        {
          if (c)
            httpd_resp_sendstr_chunk(req, ",");

          int t = sqlite3_column_type(stmt, c);
          if (t == SQLITE_NULL)
            httpd_resp_sendstr_chunk(req, "null");
          else if (t == SQLITE_INTEGER)
          {
            char buf[32];
            snprintf(buf, sizeof(buf), "%lld", (long long)sqlite3_column_int64(stmt, c));
            httpd_resp_sendstr_chunk(req, buf);
          }
          else if (t == SQLITE_FLOAT)
          {
            char buf[64];
            snprintf(buf, sizeof(buf), "%.17g", sqlite3_column_double(stmt, c));
            httpd_resp_sendstr_chunk(req, buf);
          }
          else
          {
            const unsigned char* txt = sqlite3_column_text(stmt, c);
            json_write_escaped(req, txt ? (const char*)txt : "");
          }
        }
        httpd_resp_sendstr_chunk(req, "]");
      }

      httpd_resp_sendstr_chunk(req, "]}");

      if (step_rc != SQLITE_DONE)
      {
        const char* msg = sqlite3_errmsg(g_db);
        httpd_resp_sendstr_chunk(req, "],\"error\":");
        json_write_escaped(req, msg ? msg : "sqlite step error");
        httpd_resp_sendstr_chunk(req, "}");
        httpd_resp_sendstr_chunk(req, NULL);
        sqlite3_finalize(stmt);
        xSemaphoreGive(g_db_mutex);
        return ESP_OK;
      }
    }
    else
    {
      int step_rc = sqlite3_step(stmt);
      if (step_rc != SQLITE_DONE)
      {
        const char* msg = sqlite3_errmsg(g_db);
        httpd_resp_sendstr_chunk(req, "],\"error\":");
        json_write_escaped(req, msg ? msg : "sqlite step error");
        httpd_resp_sendstr_chunk(req, "}");
        httpd_resp_sendstr_chunk(req, NULL);
        sqlite3_finalize(stmt);
        xSemaphoreGive(g_db_mutex);
        return ESP_OK;
      }

      int changes = sqlite3_changes(g_db);
      long long last_id = (long long)sqlite3_last_insert_rowid(g_db);

      char buf[128];
      snprintf(buf, sizeof(buf),
               "{\"type\":\"ok\",\"changes\":%d,\"last_insert_rowid\":%lld}",
               changes, last_id);
      httpd_resp_sendstr_chunk(req, buf);
    }

    sqlite3_finalize(stmt);
    stmt = NULL;
  }

  httpd_resp_sendstr_chunk(req, "],\"error\":null}");
  httpd_resp_sendstr_chunk(req, NULL);

  xSemaphoreGive(g_db_mutex);
  return ESP_OK;
}

/* -------------------- HTTP handlers -------------------- */
static bool content_type_is_json(httpd_req_t* req)
{
  char ctype[64] = {0};
  if (httpd_req_get_hdr_value_str(req, "Content-Type", ctype, sizeof(ctype)) != ESP_OK)
    return false;
  return (strncmp(ctype, "application/json", 16) == 0);
}

static esp_err_t sql_post_handler(httpd_req_t* req)
{
  if (!content_type_is_json(req))
  {
    httpd_resp_send_err(req, HTTPD_400_BAD_REQUEST, "Content-Type must be application/json");
    return ESP_FAIL;
  }

  char* body = NULL;
  esp_err_t err = recv_body_all(req, &body, 64 * 1024);
  if (err != ESP_OK)
  {
    httpd_resp_send_err(req, HTTPD_400_BAD_REQUEST, "bad body");
    return ESP_FAIL;
  }

  char* sql = NULL;

  // Extract {"sql":"..."}
  char* sql_buf = (char*)calloc(1, 64 * 1024);
  if (!sql_buf)
  {
    free(body);
    httpd_resp_send_err(req, HTTPD_500_INTERNAL_SERVER_ERROR, "oom");
    return ESP_FAIL;
  }
  bool ok = json_get_str(body, "sql", sql_buf, 64 * 1024);
  free(body);

  if (!ok || sql_buf[0] == 0)
  {
    free(sql_buf);
    httpd_resp_send_err(req, HTTPD_400_BAD_REQUEST, "missing sql");
    return ESP_FAIL;
  }
  sql = sql_buf;

  err = exec_sql_all(req, sql);
  free(sql);
  return err;
}

static esp_err_t wifi_save_handler(httpd_req_t* req)
{
  if (!content_type_is_json(req))
  {
    httpd_resp_send_err(req, HTTPD_400_BAD_REQUEST, "Content-Type must be application/json");
    return ESP_FAIL;
  }

  char* body = NULL;
  esp_err_t err = recv_body_all(req, &body, 1024);
  if (err != ESP_OK)
  {
    httpd_resp_send_err(req, HTTPD_400_BAD_REQUEST, "bad body");
    return ESP_FAIL;
  }

  char ssid[33] = {0};
  char pass[65] = {0};

  bool ok1 = json_get_str(body, "ssid", ssid, sizeof(ssid));
  bool ok2 = json_get_str(body, "pass", pass, sizeof(pass));
  (void)ok2; // pass mag leeg zijn

  free(body);

  if (!ok1 || ssid[0] == 0)
  {
    httpd_resp_send_err(req, HTTPD_400_BAD_REQUEST, "missing ssid");
    return ESP_FAIL;
  }

  err = nvs_save_wifi(ssid, pass);
  if (err != ESP_OK)
  {
    httpd_resp_send_err(req, HTTPD_500_INTERNAL_SERVER_ERROR, "nvs save failed");
    return ESP_FAIL;
  }

  httpd_resp_set_type(req, "application/json");
  httpd_resp_sendstr(req, "{\"ok\":true,\"saved\":true,\"rebooting\":true}");

  // reboot nadat response verstuurd is
  vTaskDelay(pdMS_TO_TICKS(500));
  esp_restart();
  return ESP_OK;
}

/* -------------------- Public start -------------------- */
esp_err_t sql_api_start(sqlite3* db)
{
  if (!db)
    return ESP_ERR_INVALID_ARG;
  g_db = db;

  if (!g_db_mutex)
    g_db_mutex = xSemaphoreCreateMutex();

  httpd_config_t config = HTTPD_DEFAULT_CONFIG();
  config.stack_size = 12288;

  // Belangrijk: we draaien op 8080 om niet te botsen met portal (80)
  config.server_port = 8080;
  config.ctrl_port = 32768 + 8080;

  // Nodig voor "/static/*"
  config.uri_match_fn = httpd_uri_match_wildcard;

  if (httpd_start(&g_server, &config) != ESP_OK)
  {
    ESP_LOGE(TAG, "httpd_start failed");
    return ESP_FAIL;
  }

  // --- API handlers ---
  httpd_uri_t sql_uri = {
      .uri = "/sql",
      .method = HTTP_POST,
      .handler = sql_post_handler,
      .user_ctx = NULL};
  httpd_register_uri_handler(g_server, &sql_uri);

  httpd_uri_t wifi_uri = {
      .uri = "/wifi/save",
      .method = HTTP_POST,
      .handler = wifi_save_handler,
      .user_ctx = NULL};
  httpd_register_uri_handler(g_server, &wifi_uri);

  // --- Static file server (SPIFFS/LittleFS data/) ---
  // Zorg dat je FS gemount is op "/spiffs" voordat je dit aanroept.
  http_file_server_config_t fcfg = {
      .base_path = "/spiffs",
      .uri_prefix = "/static",
      .index_path = "/index.html",
      .cache_control_no_store = true};
  esp_err_t ferr = http_file_server_register(g_server, &fcfg);
  if (ferr != ESP_OK)
  {
    ESP_LOGW(TAG, "http_file_server_register failed: %s", esp_err_to_name(ferr));
  }
  else
  {
    ESP_LOGI(TAG, "Static UI:");
    ESP_LOGI(TAG, "  GET  http://<ip>:8080/            (index.html)");
    ESP_LOGI(TAG, "  GET  http://<ip>:8080/static/...  (assets)");
  }

  ESP_LOGI(TAG, "SQL API ready:");
  ESP_LOGI(TAG, "  POST http://<ip>:8080/sql  body: {\"sql\":\"SELECT 1;\"}");
  ESP_LOGI(TAG, "  POST http://<ip>:8080/wifi/save body: {\"ssid\":\"...\",\"pass\":\"...\"}");

  return ESP_OK;
}