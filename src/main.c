#include "esp_log.h"
#include "nvs_flash.h"

#include "wifi_manager.h"
#include "db_psram.h"
#include "sql_api.h"

static const char *TAG = "MAIN";

void app_main(void) {
  ESP_ERROR_CHECK(nvs_flash_init());

  ESP_LOGI(TAG, "1) WiFi manager start");
  ESP_ERROR_CHECK(wifi_manager_start());

  ESP_LOGI(TAG, "2) Open SQLite database in PSRAM (in-memory)");
  sqlite3 *db = NULL;
  ESP_ERROR_CHECK(db_psram_open(&db));

  ESP_LOGI(TAG, "3) Start SQL JSON REST API");
  ESP_ERROR_CHECK(sql_api_start(db));

  ESP_LOGI(TAG, "Ready. POST /sql with JSON.");
}