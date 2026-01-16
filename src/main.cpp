#include "nvs_flash.h"
#include "esp_log.h"
#include "sqlite3.h"

#include "wifi_manager.h"
#include "sdcard.h"
#include "sql_api.h"

static const char *TAG = "MAIN";

void app_main(void) {
  ESP_ERROR_CHECK(nvs_flash_init());

  // 1) WiFi manager (STA proberen, anders AP portal)
  ESP_ERROR_CHECK(wifi_manager_start());

  // 2) SD card mount
  ESP_ERROR_CHECK(sdcard_mount());

  // 3) Open SQLite DB
  sqlite3 *db = NULL;
  ESP_ERROR_CHECK(sdcard_open_db("/sdcard/app.db", &db));

  // 4) Start REST API: POST /sql JSON
  ESP_ERROR_CHECK(sql_api_start(db));

  ESP_LOGI(TAG, "Ready.");
}
