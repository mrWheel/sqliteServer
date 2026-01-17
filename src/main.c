#include "esp_log.h"
#include "nvs_flash.h"

#include "wifi_manager.h"
#include "sdcard.h"
#include "sql_api.h"

static const char *TAG = "MAIN";

void app_main(void)
{
    ESP_ERROR_CHECK(nvs_flash_init());

    ESP_LOGI(TAG, "1) WiFi manager start");
    ESP_ERROR_CHECK(wifi_manager_start());

    ESP_LOGI(TAG, "2) Mount SD card");
    ESP_ERROR_CHECK(sdcard_mount());

    ESP_LOGI(TAG, "3) Open SQLite database on SD card (file-based)");
    sqlite3 *db = NULL;
    ESP_ERROR_CHECK(sdcard_open_db("/sdcard/app.db", &db));

    ESP_LOGI(TAG, "4) Start SQL JSON REST API");
    ESP_ERROR_CHECK(sql_api_start(db));

    ESP_LOGI(TAG, "Ready. POST /sql with JSON. DB=/sdcard/app.db");
}