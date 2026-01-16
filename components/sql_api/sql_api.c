#include "sql_api.h"

#include <string.h>
#include <stdlib.h>
#include <stdbool.h>
#include <ctype.h>

#include "freertos/FreeRTOS.h"
#include "freertos/semphr.h"

#include "esp_log.h"
#include "esp_http_server.h"

static const char *TAG = "SQLAPI";

static sqlite3 *g_db = NULL;
static SemaphoreHandle_t g_db_mutex;
static httpd_handle_t g_server = NULL;

/* ---- minimal JSON escape for output ---- */
static void json_write_escaped(httpd_req_t *req, const char *s) {
  httpd_resp_sendstr_chunk(req, "\"");
  for (const unsigned char *p = (const unsigned char*)s; p && *p; p++) {
    if (*p == '\"') httpd_resp_sendstr_chunk(req, "\\\"");
    else if (*p == '\\') httpd_resp_sendstr_chunk(req, "\\\\");
    else if (*p == '\n') httpd_resp_sendstr_chunk(req, "\\n");
    else if (*p == '\r') httpd_resp_sendstr_chunk(req, "\\r");
    else if (*p == '\t') httpd_resp_sendstr_chunk(req, "\\t");
    else if (*p < 0x20) {
      char buf[8];
      snprintf(buf, sizeof(buf), "\\u%04x", (unsigned)*p);
      httpd_resp_sendstr_chunk(req, buf);
    } else {
      char c[2] = {(char)*p, 0};
      httpd_resp_sendstr_chunk(req, c);
    }
  }
  httpd_resp_sendstr_chunk(req, "\"");
}

/* ---- JSON input: only {"sql":"..."} (simple parser, no escaping support) ---- */
static char *json_extract_sql(const char *body) {
  const char *k = "\"sql\"";
  const char *p = strstr(body, k);
  if (!p) return NULL;
  p = strchr(p, ':'); if (!p) return NULL;
  p++;
  while (*p && isspace((unsigned char)*p)) p++;
  if (*p != '"') return NULL;
  p++;
  const char *e = strchr(p, '"');
  if (!e) return NULL;

  size_t n = (size_t)(e - p);
  char *out = (char*)malloc(n + 1);
  memcpy(out, p, n);
  out[n] = 0;
  return out;
}

/* ---- Execute multi-statement SQL and stream JSON ---- */
static esp_err_t exec_sql_all(httpd_req_t *req, const char *sql_in) {
  xSemaphoreTake(g_db_mutex, portMAX_DELAY);

  const char *sql = sql_in;
  sqlite3_stmt *stmt = NULL;

  httpd_resp_set_type(req, "application/json");
  httpd_resp_sendstr_chunk(req, "{\"results\":[");

  bool first_result = true;

  while (1) {
    int rc = sqlite3_prepare_v2(g_db, sql, -1, &stmt, &sql);
    if (rc != SQLITE_OK) {
      const char *msg = sqlite3_errmsg(g_db);
      httpd_resp_sendstr_chunk(req, "],\"error\":");
      json_write_escaped(req, msg ? msg : "sqlite error");
      httpd_resp_sendstr_chunk(req, "}");
      httpd_resp_sendstr_chunk(req, NULL);
      if (stmt) sqlite3_finalize(stmt);
      xSemaphoreGive(g_db_mutex);
      return ESP_OK;
    }

    if (!stmt) break; // done

    int col_count = sqlite3_column_count(stmt);
    bool is_select_like = (col_count > 0);

    if (!first_result) httpd_resp_sendstr_chunk(req, ",");
    first_result = false;

    if (is_select_like) {
      httpd_resp_sendstr_chunk(req, "{\"type\":\"select\",\"columns\":[");
      for (int c = 0; c < col_count; c++) {
        if (c) httpd_resp_sendstr_chunk(req, ",");
        json_write_escaped(req, sqlite3_column_name(stmt, c));
      }
      httpd_resp_sendstr_chunk(req, "],\"rows\":[");

      bool first_row = true;
      int step_rc;
      while ((step_rc = sqlite3_step(stmt)) == SQLITE_ROW) {
        if (!first_row) httpd_resp_sendstr_chunk(req, ",");
        first_row = false;

        httpd_resp_sendstr_chunk(req, "[");
        for (int c = 0; c < col_count; c++) {
          if (c) httpd_resp_sendstr_chunk(req, ",");

          int t = sqlite3_column_type(stmt, c);
          if (t == SQLITE_NULL) httpd_resp_sendstr_chunk(req, "null");
          else if (t == SQLITE_INTEGER) {
            char buf[32];
            snprintf(buf, sizeof(buf), "%lld", (long long)sqlite3_column_int64(stmt, c));
            httpd_resp_sendstr_chunk(req, buf);
          } else if (t == SQLITE_FLOAT) {
            char buf[64];
            snprintf(buf, sizeof(buf), "%.17g", sqlite3_column_double(stmt, c));
            httpd_resp_sendstr_chunk(req, buf);
          } else {
            const unsigned char *txt = sqlite3_column_text(stmt, c);
            json_write_escaped(req, txt ? (const char*)txt : "");
          }
        }
        httpd_resp_sendstr_chunk(req, "]");
      }

      httpd_resp_sendstr_chunk(req, "]}");

      if (step_rc != SQLITE_DONE) {
        const char *msg = sqlite3_errmsg(g_db);
        httpd_resp_sendstr_chunk(req, "],\"error\":");
        json_write_escaped(req, msg ? msg : "sqlite step error");
        httpd_resp_sendstr_chunk(req, "}");
        httpd_resp_sendstr_chunk(req, NULL);
        sqlite3_finalize(stmt);
        xSemaphoreGive(g_db_mutex);
        return ESP_OK;
      }
    } else {
      int step_rc = sqlite3_step(stmt);
      if (step_rc != SQLITE_DONE) {
        const char *msg = sqlite3_errmsg(g_db);
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

static esp_err_t sql_post_handler(httpd_req_t *req) {
  // Enforce JSON only
  char ctype[64] = {0};
  if (httpd_req_get_hdr_value_str(req, "Content-Type", ctype, sizeof(ctype)) != ESP_OK ||
    strncmp(ctype, "application/json", 16) != 0) {
    httpd_resp_send_err(req, HTTPD_400_BAD_REQUEST, "Content-Type must be application/json");
    return ESP_FAIL;
  }

  int len = req->content_len;
  if (len <= 0 || len > 64 * 1024) {
    httpd_resp_send_err(req, HTTPD_400_BAD_REQUEST, "bad body size");
    return ESP_FAIL;
  }

  char *body = (char*)calloc(1, len + 1);
  int r = httpd_req_recv(req, body, len);
  if (r <= 0) {
    free(body);
    httpd_resp_send_err(req, HTTPD_400_BAD_REQUEST, "recv failed");
    return ESP_FAIL;
  }

  char *sql = json_extract_sql(body);
  free(body);

  if (!sql || sql[0] == 0) {
    free(sql);
    httpd_resp_send_err(req, HTTPD_400_BAD_REQUEST, "missing sql");
    return ESP_FAIL;
  }

  esp_err_t err = exec_sql_all(req, sql);
  free(sql);
  return err;
}

esp_err_t sql_api_start(sqlite3 *db) {
  if (!db) return ESP_ERR_INVALID_ARG;
  g_db = db;

  if (!g_db_mutex) g_db_mutex = xSemaphoreCreateMutex();

  httpd_config_t config = HTTPD_DEFAULT_CONFIG();
  config.stack_size = 12288;

  if (httpd_start(&g_server, &config) != ESP_OK) {
    ESP_LOGE(TAG, "httpd_start failed");
    return ESP_FAIL;
  }

  httpd_uri_t sql_uri = {
    .uri = "/sql",
    .method = HTTP_POST,
    .handler = sql_post_handler,
    .user_ctx = NULL
  };
  httpd_register_uri_handler(g_server, &sql_uri);

  ESP_LOGI(TAG, "SQL API ready: POST /sql (JSON only)");
  return ESP_OK;
}
