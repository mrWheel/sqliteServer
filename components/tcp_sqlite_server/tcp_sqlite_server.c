#include "tcp_sqlite_server.h"

#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <errno.h>

#include "esp_log.h"
#include "lwip/sockets.h"
#include "lwip/inet.h"
#include "freertos/task.h"

#include "cJSON.h"

static const char* TAG = "TCP_SQLITE";

typedef struct {
  int in_use;
  int id;              // 1..N
  sqlite3_stmt* stmt;
  int col_count;
  char** col_names;    // allocated per stmt
} stmt_slot_t;

typedef struct {
  int sock;
  sqlite3* db;
  SemaphoreHandle_t db_mutex;
  tcp_sqlite_server_cfg_t cfg;
  stmt_slot_t* slots;  // array [max_stmts_per_client]
  int next_stmt_id;
} client_ctx_t;

static void free_col_names(stmt_slot_t* s) {
  if (!s || !s->col_names) return;
  for (int i = 0; i < s->col_count; i++) {
    free(s->col_names[i]);
  }
  free(s->col_names);
  s->col_names = NULL;
}

static stmt_slot_t* find_slot_by_id(client_ctx_t* c, int id) {
  if (!c || id <= 0) return NULL;
  for (int i = 0; i < c->cfg.max_stmts_per_client; i++) {
    if (c->slots[i].in_use && c->slots[i].id == id) return &c->slots[i];
  }
  return NULL;
}

static stmt_slot_t* alloc_slot(client_ctx_t* c) {
  for (int i = 0; i < c->cfg.max_stmts_per_client; i++) {
    if (!c->slots[i].in_use) {
      c->slots[i].in_use = 1;
      c->slots[i].id = c->next_stmt_id++;
      c->slots[i].stmt = NULL;
      c->slots[i].col_count = 0;
      c->slots[i].col_names = NULL;
      return &c->slots[i];
    }
  }
  return NULL;
}

static void release_slot(stmt_slot_t* s) {
  if (!s) return;
  if (s->stmt) {
    sqlite3_finalize(s->stmt);
    s->stmt = NULL;
  }
  free_col_names(s);
  s->col_count = 0;
  s->id = 0;
  s->in_use = 0;
}

static int send_json_line(int sock, const cJSON* obj, int tx_max) {
  char* txt = cJSON_PrintUnformatted(obj);
  if (!txt) return -1;

  size_t len = strlen(txt);
  if ((int)len + 1 > tx_max) {
    free(txt);
    return -1;
  }

  // add newline
  int rc = send(sock, txt, len, 0);
  if (rc > 0) {
    send(sock, "\n", 1, 0);
  }
  free(txt);
  return rc;
}

static cJSON* make_err(int code, const char* msg) {
  cJSON* root = cJSON_CreateObject();
  cJSON_AddBoolToObject(root, "ok", false);
  cJSON* err = cJSON_CreateObject();
  cJSON_AddNumberToObject(err, "code", code);
  cJSON_AddStringToObject(err, "message", msg ? msg : "error");
  cJSON_AddItemToObject(root, "error", err);
  return root;
}

static cJSON* make_ok(void) {
  cJSON* root = cJSON_CreateObject();
  cJSON_AddBoolToObject(root, "ok", true);
  return root;
}

// Simple line receiver: reads until '\n' (strips \r\n), returns length, 0 on disconnect, -1 on error
static int recv_line(int sock, char* buf, int maxlen) {
  int idx = 0;
  while (idx < maxlen - 1) {
    char ch;
    int r = recv(sock, &ch, 1, 0);
    if (r == 0) return 0;      // disconnected
    if (r < 0) return -1;      // error

    if (ch == '\n') break;
    if (ch == '\r') continue;  // ignore CR
    buf[idx++] = ch;
  }
  buf[idx] = 0;
  return idx;
}

static const char* json_get_string(cJSON* obj, const char* key) {
  cJSON* it = cJSON_GetObjectItem(obj, key);
  if (!cJSON_IsString(it)) return NULL;
  return it->valuestring;
}

static int json_get_int(cJSON* obj, const char* key, int def) {
  cJSON* it = cJSON_GetObjectItem(obj, key);
  if (!cJSON_IsNumber(it)) return def;
  return it->valueint;
}

static bool json_get_bool(cJSON* obj, const char* key, bool def) {
  cJSON* it = cJSON_GetObjectItem(obj, key);
  if (!cJSON_IsBool(it)) return def;
  return cJSON_IsTrue(it);
}

static void handle_exec(client_ctx_t* c, cJSON* req) {
  const char* sql = json_get_string(req, "sql");
  if (!sql || sql[0] == 0) {
    cJSON* e = make_err(400, "missing sql");
    send_json_line(c->sock, e, c->cfg.tx_line_max);
    cJSON_Delete(e);
    return;
  }

  cJSON* resp = NULL;

  if (xSemaphoreTake(c->db_mutex, pdMS_TO_TICKS(5000)) != pdTRUE) {
    resp = make_err(500, "db mutex timeout");
    send_json_line(c->sock, resp, c->cfg.tx_line_max);
    cJSON_Delete(resp);
    return;
  }

  char* errmsg = NULL;
  int rc = sqlite3_exec(c->db, sql, NULL, NULL, &errmsg);

  int changes = sqlite3_changes(c->db);
  int total_changes = sqlite3_total_changes(c->db);
  sqlite3_int64 last_id = sqlite3_last_insert_rowid(c->db);

  xSemaphoreGive(c->db_mutex);

  if (rc != SQLITE_OK) {
    char msg[256];
    snprintf(msg, sizeof(msg), "sqlite rc=%d: %s", rc, errmsg ? errmsg : sqlite3_errmsg(c->db));
    sqlite3_free(errmsg);
    resp = make_err(500, msg);
    send_json_line(c->sock, resp, c->cfg.tx_line_max);
    cJSON_Delete(resp);
    return;
  }

  if (errmsg) sqlite3_free(errmsg);

  resp = make_ok();
  cJSON_AddNumberToObject(resp, "changes", changes);
  cJSON_AddNumberToObject(resp, "total_changes", total_changes);
  cJSON_AddNumberToObject(resp, "last_insert_rowid", (double)last_id);
  send_json_line(c->sock, resp, c->cfg.tx_line_max);
  cJSON_Delete(resp);
}

static void handle_prepare(client_ctx_t* c, cJSON* req) {
  const char* sql = json_get_string(req, "sql");
  if (!sql || sql[0] == 0) {
    cJSON* e = make_err(400, "missing sql");
    send_json_line(c->sock, e, c->cfg.tx_line_max);
    cJSON_Delete(e);
    return;
  }

  stmt_slot_t* slot = alloc_slot(c);
  if (!slot) {
    cJSON* e = make_err(409, "no free stmt slots");
    send_json_line(c->sock, e, c->cfg.tx_line_max);
    cJSON_Delete(e);
    return;
  }

  if (xSemaphoreTake(c->db_mutex, pdMS_TO_TICKS(5000)) != pdTRUE) {
    release_slot(slot);
    cJSON* e = make_err(500, "db mutex timeout");
    send_json_line(c->sock, e, c->cfg.tx_line_max);
    cJSON_Delete(e);
    return;
  }

  sqlite3_stmt* stmt = NULL;
  int rc = sqlite3_prepare_v2(c->db, sql, -1, &stmt, NULL);

  xSemaphoreGive(c->db_mutex);

  if (rc != SQLITE_OK || !stmt) {
    release_slot(slot);
    char msg[256];
    snprintf(msg, sizeof(msg), "sqlite rc=%d: %s", rc, sqlite3_errmsg(c->db));
    cJSON* e = make_err(500, msg);
    send_json_line(c->sock, e, c->cfg.tx_line_max);
    cJSON_Delete(e);
    return;
  }

  slot->stmt = stmt;
  slot->col_count = sqlite3_column_count(stmt);

  // copy column names
  slot->col_names = (char**)calloc(slot->col_count, sizeof(char*));
  for (int i = 0; i < slot->col_count; i++) {
    const char* n = sqlite3_column_name(stmt, i);
    slot->col_names[i] = strdup(n ? n : "");
  }

  cJSON* resp = make_ok();
  cJSON_AddNumberToObject(resp, "stmt", slot->id);
  cJSON_AddNumberToObject(resp, "cols", slot->col_count);

  cJSON* names = cJSON_CreateArray();
  for (int i = 0; i < slot->col_count; i++) {
    cJSON_AddItemToArray(names, cJSON_CreateString(slot->col_names[i] ? slot->col_names[i] : ""));
  }
  cJSON_AddItemToObject(resp, "col_names", names);

  send_json_line(c->sock, resp, c->cfg.tx_line_max);
  cJSON_Delete(resp);
}

static void handle_bind(client_ctx_t* c, cJSON* req) {
  int stmt_id = json_get_int(req, "stmt", -1);
  int index = json_get_int(req, "index", -1);
  const char* type = json_get_string(req, "type");

  if (stmt_id <= 0 || index <= 0 || !type) {
    cJSON* e = make_err(400, "missing stmt/index/type");
    send_json_line(c->sock, e, c->cfg.tx_line_max);
    cJSON_Delete(e);
    return;
  }

  stmt_slot_t* slot = find_slot_by_id(c, stmt_id);
  if (!slot || !slot->stmt) {
    cJSON* e = make_err(404, "stmt not found");
    send_json_line(c->sock, e, c->cfg.tx_line_max);
    cJSON_Delete(e);
    return;
  }

  int rc = SQLITE_ERROR;

  // Binding calls are safe without DB mutex if single-thread per connection,
  // but SQLite connection is shared across clients; keep mutex to be safe.
  if (xSemaphoreTake(c->db_mutex, pdMS_TO_TICKS(5000)) != pdTRUE) {
    cJSON* e = make_err(500, "db mutex timeout");
    send_json_line(c->sock, e, c->cfg.tx_line_max);
    cJSON_Delete(e);
    return;
  }

  if (strcmp(type, "null") == 0) {
    rc = sqlite3_bind_null(slot->stmt, index);
  } else if (strcmp(type, "int") == 0) {
    cJSON* v = cJSON_GetObjectItem(req, "value");
    if (!cJSON_IsNumber(v)) rc = SQLITE_MISMATCH;
    else rc = sqlite3_bind_int64(slot->stmt, index, (sqlite3_int64)v->valuedouble);
  } else if (strcmp(type, "double") == 0) {
    cJSON* v = cJSON_GetObjectItem(req, "value");
    if (!cJSON_IsNumber(v)) rc = SQLITE_MISMATCH;
    else rc = sqlite3_bind_double(slot->stmt, index, v->valuedouble);
  } else if (strcmp(type, "text") == 0) {
    cJSON* v = cJSON_GetObjectItem(req, "value");
    if (!cJSON_IsString(v)) rc = SQLITE_MISMATCH;
    else rc = sqlite3_bind_text(slot->stmt, index, v->valuestring, -1, SQLITE_TRANSIENT);
  } else {
    rc = SQLITE_MISUSE;
  }

  xSemaphoreGive(c->db_mutex);

  if (rc != SQLITE_OK) {
    char msg[128];
    snprintf(msg, sizeof(msg), "bind rc=%d", rc);
    cJSON* e = make_err(500, msg);
    send_json_line(c->sock, e, c->cfg.tx_line_max);
    cJSON_Delete(e);
    return;
  }

  cJSON* resp = make_ok();
  send_json_line(c->sock, resp, c->cfg.tx_line_max);
  cJSON_Delete(resp);
}

static const char* sqlite_type_to_str(int t) {
  switch (t) {
    case SQLITE_INTEGER: return "int";
    case SQLITE_FLOAT:   return "double";
    case SQLITE_TEXT:    return "text";
    case SQLITE_BLOB:    return "blob";
    case SQLITE_NULL:    return "null";
    default:             return "unknown";
  }
}

static void handle_step(client_ctx_t* c, cJSON* req) {
  int stmt_id = json_get_int(req, "stmt", -1);
  if (stmt_id <= 0) {
    cJSON* e = make_err(400, "missing stmt");
    send_json_line(c->sock, e, c->cfg.tx_line_max);
    cJSON_Delete(e);
    return;
  }

  stmt_slot_t* slot = find_slot_by_id(c, stmt_id);
  if (!slot || !slot->stmt) {
    cJSON* e = make_err(404, "stmt not found");
    send_json_line(c->sock, e, c->cfg.tx_line_max);
    cJSON_Delete(e);
    return;
  }

  if (xSemaphoreTake(c->db_mutex, pdMS_TO_TICKS(5000)) != pdTRUE) {
    cJSON* e = make_err(500, "db mutex timeout");
    send_json_line(c->sock, e, c->cfg.tx_line_max);
    cJSON_Delete(e);
    return;
  }

  int rc = sqlite3_step(slot->stmt);

  cJSON* resp = make_ok();

  if (rc == SQLITE_ROW) {
    int cols = sqlite3_column_count(slot->stmt);
    cJSON* row = cJSON_CreateArray();
    cJSON* types = cJSON_CreateArray();

    for (int i = 0; i < cols; i++) {
      int t = sqlite3_column_type(slot->stmt, i);
      cJSON_AddItemToArray(types, cJSON_CreateString(sqlite_type_to_str(t)));

      if (t == SQLITE_NULL) {
        cJSON_AddItemToArray(row, cJSON_CreateNull());
      } else {
        // Voor eenvoud sturen we alles als string (zoals veel PHP libs ook doen)
        const unsigned char* txt = sqlite3_column_text(slot->stmt, i);
        cJSON_AddItemToArray(row, cJSON_CreateString(txt ? (const char*)txt : ""));
      }
    }
    cJSON_AddItemToObject(resp, "row", row);
    cJSON_AddItemToObject(resp, "types", types);
  } else if (rc == SQLITE_DONE) {
    cJSON_AddBoolToObject(resp, "done", true);
  } else {
    char msg[256];
    snprintf(msg, sizeof(msg), "sqlite step rc=%d: %s", rc, sqlite3_errmsg(c->db));
    cJSON_Delete(resp);
    resp = make_err(500, msg);
  }

  xSemaphoreGive(c->db_mutex);

  send_json_line(c->sock, resp, c->cfg.tx_line_max);
  cJSON_Delete(resp);
}

static void handle_reset(client_ctx_t* c, cJSON* req) {
  int stmt_id = json_get_int(req, "stmt", -1);
  bool clear_binds = json_get_bool(req, "clear_binds", true);

  if (stmt_id <= 0) {
    cJSON* e = make_err(400, "missing stmt");
    send_json_line(c->sock, e, c->cfg.tx_line_max);
    cJSON_Delete(e);
    return;
  }

  stmt_slot_t* slot = find_slot_by_id(c, stmt_id);
  if (!slot || !slot->stmt) {
    cJSON* e = make_err(404, "stmt not found");
    send_json_line(c->sock, e, c->cfg.tx_line_max);
    cJSON_Delete(e);
    return;
  }

  if (xSemaphoreTake(c->db_mutex, pdMS_TO_TICKS(5000)) != pdTRUE) {
    cJSON* e = make_err(500, "db mutex timeout");
    send_json_line(c->sock, e, c->cfg.tx_line_max);
    cJSON_Delete(e);
    return;
  }

  int rc1 = sqlite3_reset(slot->stmt);
  int rc2 = SQLITE_OK;
  if (clear_binds) rc2 = sqlite3_clear_bindings(slot->stmt);

  xSemaphoreGive(c->db_mutex);

  if (rc1 != SQLITE_OK || rc2 != SQLITE_OK) {
    char msg[128];
    snprintf(msg, sizeof(msg), "reset rc=%d clear rc=%d", rc1, rc2);
    cJSON* e = make_err(500, msg);
    send_json_line(c->sock, e, c->cfg.tx_line_max);
    cJSON_Delete(e);
    return;
  }

  cJSON* resp = make_ok();
  send_json_line(c->sock, resp, c->cfg.tx_line_max);
  cJSON_Delete(resp);
}

static void handle_finalize(client_ctx_t* c, cJSON* req) {
  int stmt_id = json_get_int(req, "stmt", -1);
  if (stmt_id <= 0) {
    cJSON* e = make_err(400, "missing stmt");
    send_json_line(c->sock, e, c->cfg.tx_line_max);
    cJSON_Delete(e);
    return;
  }

  stmt_slot_t* slot = find_slot_by_id(c, stmt_id);
  if (!slot) {
    cJSON* e = make_err(404, "stmt not found");
    send_json_line(c->sock, e, c->cfg.tx_line_max);
    cJSON_Delete(e);
    return;
  }

  if (xSemaphoreTake(c->db_mutex, pdMS_TO_TICKS(5000)) != pdTRUE) {
    cJSON* e = make_err(500, "db mutex timeout");
    send_json_line(c->sock, e, c->cfg.tx_line_max);
    cJSON_Delete(e);
    return;
  }

  release_slot(slot);

  xSemaphoreGive(c->db_mutex);

  cJSON* resp = make_ok();
  send_json_line(c->sock, resp, c->cfg.tx_line_max);
  cJSON_Delete(resp);
}

static void handle_ping(client_ctx_t* c) {
  cJSON* resp = make_ok();
  cJSON_AddBoolToObject(resp, "pong", true);
  send_json_line(c->sock, resp, c->cfg.tx_line_max);
  cJSON_Delete(resp);
}

static void dispatch(client_ctx_t* c, const char* line) {
  cJSON* req = cJSON_Parse(line);
  if (!req) {
    cJSON* e = make_err(400, "invalid json");
    send_json_line(c->sock, e, c->cfg.tx_line_max);
    cJSON_Delete(e);
    return;
  }

  const char* op = json_get_string(req, "op");
  if (!op) {
    cJSON* e = make_err(400, "missing op");
    send_json_line(c->sock, e, c->cfg.tx_line_max);
    cJSON_Delete(e);
    cJSON_Delete(req);
    return;
  }

  if (strcmp(op, "ping") == 0) {
    handle_ping(c);
  } else if (strcmp(op, "exec") == 0) {
    handle_exec(c, req);
  } else if (strcmp(op, "prepare") == 0) {
    handle_prepare(c, req);
  } else if (strcmp(op, "bind") == 0) {
    handle_bind(c, req);
  } else if (strcmp(op, "step") == 0) {
    handle_step(c, req);
  } else if (strcmp(op, "reset") == 0) {
    handle_reset(c, req);
  } else if (strcmp(op, "finalize") == 0) {
    handle_finalize(c, req);
  } else {
    cJSON* e = make_err(501, "unknown op");
    send_json_line(c->sock, e, c->cfg.tx_line_max);
    cJSON_Delete(e);
  }

  cJSON_Delete(req);
}

static void client_task(void* arg) {
  client_ctx_t* c = (client_ctx_t*)arg;

  char* line = (char*)malloc(c->cfg.rx_line_max);
  if (!line) {
    ESP_LOGE(TAG, "No mem for rx buffer");
    close(c->sock);
    free(c->slots);
    free(c);
    vTaskDelete(NULL);
    return;
  }

  ESP_LOGI(TAG, "Client connected (sock=%d)", c->sock);

  // greet
  {
    cJSON* hello = make_ok();
    cJSON_AddStringToObject(hello, "hello", "sqlite-tcp-v1");
    send_json_line(c->sock, hello, c->cfg.tx_line_max);
    cJSON_Delete(hello);
  }

  while (1) {
    int n = recv_line(c->sock, line, c->cfg.rx_line_max);
    if (n == 0) {
      ESP_LOGI(TAG, "Client disconnected");
      break;
    }
    if (n < 0) {
      ESP_LOGW(TAG, "recv error: errno=%d", errno);
      break;
    }
    if (n == 0 || line[0] == 0) continue;

    dispatch(c, line);
  }

  // cleanup statements
  if (xSemaphoreTake(c->db_mutex, pdMS_TO_TICKS(5000)) == pdTRUE) {
    for (int i = 0; i < c->cfg.max_stmts_per_client; i++) {
      if (c->slots[i].in_use) release_slot(&c->slots[i]);
    }
    xSemaphoreGive(c->db_mutex);
  } else {
    // best effort without mutex
    for (int i = 0; i < c->cfg.max_stmts_per_client; i++) {
      if (c->slots[i].in_use) release_slot(&c->slots[i]);
    }
  }

  close(c->sock);
  free(line);
  free(c->slots);
  free(c);
  vTaskDelete(NULL);
}

static void server_task(void* arg) {
  client_ctx_t* template_ctx = (client_ctx_t*)arg;

  int listen_sock = socket(AF_INET, SOCK_STREAM, IPPROTO_IP);
  if (listen_sock < 0) {
    ESP_LOGE(TAG, "socket() failed: errno=%d", errno);
    free(template_ctx);
    vTaskDelete(NULL);
    return;
  }

  int yes = 1;
  setsockopt(listen_sock, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));

  struct sockaddr_in addr;
  memset(&addr, 0, sizeof(addr));
  addr.sin_family = AF_INET;
  addr.sin_port = htons(template_ctx->cfg.port);
  addr.sin_addr.s_addr = htonl(INADDR_ANY);

  if (bind(listen_sock, (struct sockaddr*)&addr, sizeof(addr)) != 0) {
    ESP_LOGE(TAG, "bind() failed: errno=%d", errno);
    close(listen_sock);
    free(template_ctx);
    vTaskDelete(NULL);
    return;
  }

  if (listen(listen_sock, template_ctx->cfg.max_clients) != 0) {
    ESP_LOGE(TAG, "listen() failed: errno=%d", errno);
    close(listen_sock);
    free(template_ctx);
    vTaskDelete(NULL);
    return;
  }

  ESP_LOGI(TAG, "SQLite TCP server listening on port %d", template_ctx->cfg.port);

  while (1) {
    struct sockaddr_in6 source_addr;
    socklen_t socklen = sizeof(source_addr);
    int sock = accept(listen_sock, (struct sockaddr*)&source_addr, &socklen);
    if (sock < 0) {
      ESP_LOGW(TAG, "accept() failed: errno=%d", errno);
      vTaskDelay(pdMS_TO_TICKS(200));
      continue;
    }

    // create per-client context
    client_ctx_t* c = (client_ctx_t*)calloc(1, sizeof(client_ctx_t));
    if (!c) {
      ESP_LOGE(TAG, "No mem for client ctx");
      close(sock);
      continue;
    }

    *c = *template_ctx; // copy db/db_mutex/cfg
    c->sock = sock;
    c->next_stmt_id = 1;
    c->slots = (stmt_slot_t*)calloc(c->cfg.max_stmts_per_client, sizeof(stmt_slot_t));
    if (!c->slots) {
      ESP_LOGE(TAG, "No mem for stmt slots");
      close(sock);
      free(c);
      continue;
    }

    xTaskCreate(
      client_task,
      "sqlite_client",
      c->cfg.client_task_stack,
      c,
      c->cfg.client_task_prio,
      NULL
    );
  }

  // never reached
}

esp_err_t tcp_sqlite_server_start(sqlite3* db, SemaphoreHandle_t db_mutex, const tcp_sqlite_server_cfg_t* cfg) {
  if (!db || !db_mutex || !cfg) return ESP_ERR_INVALID_ARG;

  client_ctx_t* t = (client_ctx_t*)calloc(1, sizeof(client_ctx_t));
  if (!t) return ESP_ERR_NO_MEM;

  t->db = db;
  t->db_mutex = db_mutex;
  t->cfg = *cfg;

  if (t->cfg.max_clients <= 0) t->cfg.max_clients = 1;
  if (t->cfg.max_stmts_per_client <= 0) t->cfg.max_stmts_per_client = 8;
  if (t->cfg.rx_line_max <= 0) t->cfg.rx_line_max = 2048;
  if (t->cfg.tx_line_max <= 0) t->cfg.tx_line_max = 4096;
  if (t->cfg.client_task_stack <= 0) t->cfg.client_task_stack = 8192;
  if (t->cfg.client_task_prio <= 0) t->cfg.client_task_prio = 5;

  BaseType_t ok = xTaskCreate(server_task, "sqlite_tcp_srv", 4096, t, 5, NULL);
  if (ok != pdPASS) {
    free(t);
    return ESP_FAIL;
  }

  return ESP_OK;
}
