#include <string.h>

#include "freertos/FreeRTOS.h"
#include "freertos/event_groups.h"

#include "esp_log.h"
#include "esp_err.h"
#include "esp_event.h"
#include "esp_netif.h"
#include "nvs_flash.h"

#include "esp_wifi.h"

#include "sqlite3.h"

#include "sdcard.h"
#include "sql_api.h"

#include "http_file_server.h"

// Include credentials (bestand staat in include/wifiCredentials.ini)
#include "wifiCredentials.ini"

static const char *TAG = "MAIN";

static EventGroupHandle_t s_wifi_event_group;
#define WIFI_CONNECTED_BIT BIT0
#define WIFI_FAIL_BIT      BIT1

static int s_retry_num = 0;
static const int WIFI_MAX_RETRY = 10;

static void wifi_event_handler(void *arg, esp_event_base_t event_base, int32_t event_id, void *event_data)
{
    if (event_base == WIFI_EVENT && event_id == WIFI_EVENT_STA_START) {
        esp_wifi_connect();
        ESP_LOGI(TAG, "WiFi STA start -> connecting...");
    } else if (event_base == WIFI_EVENT && event_id == WIFI_EVENT_STA_DISCONNECTED) {
        if (s_retry_num < WIFI_MAX_RETRY) {
            s_retry_num++;
            esp_wifi_connect();
            ESP_LOGW(TAG, "WiFi disconnected -> retry %d/%d", s_retry_num, WIFI_MAX_RETRY);
        } else {
            xEventGroupSetBits(s_wifi_event_group, WIFI_FAIL_BIT);
            ESP_LOGE(TAG, "WiFi connect failed (max retries reached)");
        }
    } else if (event_base == IP_EVENT && event_id == IP_EVENT_STA_GOT_IP) {
        ip_event_got_ip_t *event = (ip_event_got_ip_t *)event_data;
        ESP_LOGI(TAG, "WiFi got IP: " IPSTR, IP2STR(&event->ip_info.ip));
        s_retry_num = 0;
        xEventGroupSetBits(s_wifi_event_group, WIFI_CONNECTED_BIT);
    }
}

static esp_err_t wifi_connect_from_credentials(void)
{
    // Basic sanity: empty strings voorkomen
    if (strlen(WIFI_SSID) == 0) {
        ESP_LOGE(TAG, "WIFI_SSID is empty. Check include/wifiCredentials.ini");
        return ESP_ERR_INVALID_ARG;
    }

    s_wifi_event_group = xEventGroupCreate();
    if (!s_wifi_event_group) return ESP_ERR_NO_MEM;

    ESP_ERROR_CHECK(esp_netif_init());
    ESP_ERROR_CHECK(esp_event_loop_create_default());

    esp_netif_create_default_wifi_sta();

    wifi_init_config_t cfg = WIFI_INIT_CONFIG_DEFAULT();
    ESP_ERROR_CHECK(esp_wifi_init(&cfg));

    // Event handlers
    ESP_ERROR_CHECK(esp_event_handler_instance_register(
        WIFI_EVENT, ESP_EVENT_ANY_ID, &wifi_event_handler, NULL, NULL));
    ESP_ERROR_CHECK(esp_event_handler_instance_register(
        IP_EVENT, IP_EVENT_STA_GOT_IP, &wifi_event_handler, NULL, NULL));

    wifi_config_t wifi_config = { 0 };
    strncpy((char *)wifi_config.sta.ssid, WIFI_SSID, sizeof(wifi_config.sta.ssid) - 1);
    strncpy((char *)wifi_config.sta.password, WIFI_PASS, sizeof(wifi_config.sta.password) - 1);

    // Handig: voorkomt dat ESP32 blijft proberen met slechte settings
    wifi_config.sta.threshold.authmode = WIFI_AUTH_WPA2_PSK;
    wifi_config.sta.pmf_cfg.capable = true;
    wifi_config.sta.pmf_cfg.required = false;

    ESP_ERROR_CHECK(esp_wifi_set_mode(WIFI_MODE_STA));
    ESP_ERROR_CHECK(esp_wifi_set_config(WIFI_IF_STA, &wifi_config));
    ESP_ERROR_CHECK(esp_wifi_start());

    // Wacht tot connected of fail
    EventBits_t bits = xEventGroupWaitBits(
        s_wifi_event_group,
        WIFI_CONNECTED_BIT | WIFI_FAIL_BIT,
        pdFALSE,
        pdFALSE,
        pdMS_TO_TICKS(20000)
    );

    if (bits & WIFI_CONNECTED_BIT) {
        ESP_LOGI(TAG, "Connected to WiFi SSID='%s'", WIFI_SSID);
        return ESP_OK;
    }
    if (bits & WIFI_FAIL_BIT) {
        ESP_LOGE(TAG, "Failed to connect to WiFi SSID='%s'", WIFI_SSID);
        return ESP_FAIL;
    }

    ESP_LOGE(TAG, "WiFi connect timed out");
    return ESP_ERR_TIMEOUT;
}

void app_main(void)
{
    ESP_ERROR_CHECK(nvs_flash_init());

    ESP_LOGI(TAG, "1) Connect WiFi using include/wifiCredentials.ini");
    ESP_ERROR_CHECK(wifi_connect_from_credentials());

    ESP_LOGI(TAG, "2) Mount SD card");
    ESP_ERROR_CHECK(sdcard_mount());

    // === SQLite crash fix ===
    // Als SQLite met OMIT_AUTOINIT is gebouwd, moet je dit expliciet doen.
    ESP_LOGI(TAG, "3) sqlite3_initialize()");
    int irc = sqlite3_initialize();
    if (irc != SQLITE_OK) {
        ESP_LOGE(TAG, "sqlite3_initialize failed: %d", irc);
        abort();
    }

    ESP_LOGI(TAG, "4) Open SQLite database on SD card");
    sqlite3 *db = NULL;
    ESP_ERROR_CHECK(sdcard_open_db("/sdcard/app.db", &db));

    ESP_LOGI(TAG, "5) Start SQL JSON REST API");
    ESP_ERROR_CHECK(sql_api_start(db));

    ESP_LOGI(TAG, "Ready. POST /sql with JSON. DB=/sdcard/app.db");
}