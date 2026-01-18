#include "db_psram.h"

#include <string.h>
#include "esp_log.h"
#include "esp_psram.h"
#include "esp_heap_caps.h"

static const char* TAG = "DB_PSRAM";

/* ---- SQLite custom allocator -> PSRAM ---- */
static void* ps_xMalloc(int n)
{
  if (n <= 0)
    n = 1;
  return heap_caps_malloc((size_t)n, MALLOC_CAP_SPIRAM | MALLOC_CAP_8BIT);
}

static void ps_xFree(void* p)
{
  if (p)
    heap_caps_free(p);
}

static void* ps_xRealloc(void* p, int n)
{
  if (n <= 0)
    n = 1;
  // heap_caps_realloc bestaat in ESP-IDF
  return heap_caps_realloc(p, (size_t)n, MALLOC_CAP_SPIRAM | MALLOC_CAP_8BIT);
}

static int ps_xSize(void* p)
{
  if (!p)
    return 0;
  return (int)heap_caps_get_allocated_size(p);
}

static int ps_xRoundup(int n)
{
  // simpele alignment naar 8 bytes
  return (n + 7) & ~7;
}

static int ps_xInit(void* pAppData)
{
  (void)pAppData;
  return 0;
}
static void ps_xShutdown(void* pAppData)
{
  (void)pAppData;
}

static sqlite3_mem_methods psram_mem = {
    .xMalloc = ps_xMalloc,
    .xFree = ps_xFree,
    .xRealloc = ps_xRealloc,
    .xSize = ps_xSize,
    .xRoundup = ps_xRoundup,
    .xInit = ps_xInit,
    .xShutdown = ps_xShutdown,
    .pAppData = NULL};

esp_err_t db_psram_open(sqlite3** out_db)
{
  if (!out_db)
    return ESP_ERR_INVALID_ARG;

  if (!esp_psram_is_initialized())
  {
    ESP_LOGE(TAG, "PSRAM not initialized. Enable it in menuconfig.");
    return ESP_FAIL;
  }

  // Configure SQLite to use PSRAM allocator.
  // Must happen before sqlite3_initialize/open.
  int rc = sqlite3_config(SQLITE_CONFIG_MALLOC, &psram_mem);
  if (rc != SQLITE_OK)
  {
    ESP_LOGE(TAG, "sqlite3_config(MALLOC) failed: %d", rc);
    return ESP_FAIL;
  }

  rc = sqlite3_initialize();
  if (rc != SQLITE_OK)
  {
    ESP_LOGE(TAG, "sqlite3_initialize failed: %d", rc);
    return ESP_FAIL;
  }

  sqlite3* db = NULL;

  // In-memory DB. (Volatile)
  rc = sqlite3_open(":memory:", &db);
  if (rc != SQLITE_OK)
  {
    ESP_LOGE(TAG, "sqlite3_open(:memory:) failed: %s", db ? sqlite3_errmsg(db) : "?");
    if (db)
      sqlite3_close(db);
    return ESP_FAIL;
  }

  // handigheid: iets betere default bij locks (ook al doen we 1 mutex in API)
  sqlite3_busy_timeout(db, 2000);

  *out_db = db;
  ESP_LOGI(TAG, "SQLite opened in memory using PSRAM allocator.");
  return ESP_OK;
}
