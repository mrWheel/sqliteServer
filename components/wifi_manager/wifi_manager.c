#include "wifi_manager.h"

#include <string.h>
#include <stdio.h>
#include <ctype.h>

#include "freertos/FreeRTOS.h"
#include "freertos/event_groups.h"

#include "esp_wifi.h"
#include "esp_event.h"
#include "esp_log.h"
#include "esp_netif.h"
#include "nvs.h"
#include "esp_http_server.h"
#include "esp_system.h"

static const char *TAG = "WIFIMGR";

#define WIFI_CONNECTED_BIT BIT0

static EventGroupHandle_t s_wifi_events;
static httpd_handle_t s_portal = NULL;

/* -------- NVS creds -------- */
static bool load_wifi_creds(char *ssid, size_t ssid_len, char *pass, size_t pass_len) {
  nvs_handle_t nvs;
  if (nvs_open("wifi", NVS_READONLY, &nvs) != ESP_OK) return false;

  esp_err_t err;
  err = nvs_get_str(nvs, "ssid", ssid, &ssid_len);
  if (err != ESP_OK) { nvs_close(nvs); return false; }

  err = nvs_get_str(nvs, "pass", pass, &pass_len);
  if (err != ESP_OK) { nvs_close(nvs); return false; }

  nvs_close(nvs);
  return true;
}

static void save_wifi_creds(const char *ssid, const char *pass) {
  nvs_handle_t nvs;
  nvs_open("wifi", NVS_READWRITE, &nvs);
  nvs_set_str(nvs, "ssid", ssid);
  nvs_set_str(nvs, "pass", pass);
  nvs_commit(nvs);
  nvs_close(nvs);
}

/* -------- Events -------- */
static void wifi_event_handler(void *arg, esp_event_base_t base, int32_t id, void *data) {
  if (base == WIFI_EVENT && id == WIFI_EVENT_STA_START) {
    esp_wifi_connect();
  } else if (base == IP_EVENT && id == IP_EVENT_STA_GOT_IP) {
    xEventGroupSetBits(s_wifi_events, WIFI_CONNECTED_BIT);
  }
}

/* -------- Portal (simple) -------- */
static const char *HTML_FORM =
  "<html><body>"
  "<h3>WiFi setup</h3>"
  "<form method='POST' action='/save'>"
  "SSID:<br><input name='s'><br>"
  "Password:<br><input name='p' type='password'><br><br>"
  "<input type='submit' value='Save'>"
  "</form></body></html>";

static esp_err_t portal_root(httpd_req_t *req) {
  httpd_resp_set_type(req, "text/html");
  httpd_resp_send(req, HTML_FORM, HTTPD_RESP_USE_STRLEN);
  return ESP_OK;
}

static void url_decode_inplace(char *s) {
  // minimale decode: + => space, %HH
  char *o = s;
  while (*s) {
    if (*s == '+') { *o++ = ' '; s++; }
    else if (*s == '%' && isxdigit((unsigned char)s[1]) && isxdigit((unsigned char)s[2])) {
      char hex[3] = {s[1], s[2], 0};
      *o++ = (char)strtol(hex, NULL, 16);
      s += 3;
    } else {
      *o++ = *s++;
    }
  }
  *o = 0;
}

static esp_err_t portal_save(httpd_req_t *req) {
  char buf[256] = {0};
  int r = httpd_req_recv(req, buf, sizeof(buf) - 1);
  if (r <= 0) {
    httpd_resp_send_err(req, HTTPD_400_BAD_REQUEST, "bad form");
    return ESP_FAIL;
  }

  // verwacht: s=SSID&p=PASS
  char ssid[64] = {0};
  char pass[96] = {0};

  // simpele parsing (goed genoeg voor start)
  char *s = strstr(buf, "s=");
  char *p = strstr(buf, "&p=");
  if (!s || !p) {
    httpd_resp_send_err(req, HTTPD_400_BAD_REQUEST, "missing fields");
    return ESP_FAIL;
  }
  s += 2;
  *p = 0;
  p += 3;

  strncpy(ssid, s, sizeof(ssid)-1);
  strncpy(pass, p, sizeof(pass)-1);

  url_decode_inplace(ssid);
  url_decode_inplace(pass);

  save_wifi_creds(ssid, pass);

  httpd_resp_sendstr(req, "Saved. Rebooting...");
  vTaskDelay(pdMS_TO_TICKS(800));
  esp_restart();
  return ESP_OK;
}

static void start_ap_portal(void) {
  esp_netif_create_default_wifi_ap();

  wifi_config_t ap = {0};
  snprintf((char*)ap.ap.ssid, sizeof(ap.ap.ssid), "SQLite-Setup");
  ap.ap.channel = 1;
  ap.ap.max_connection = 4;
  ap.ap.authmode = WIFI_AUTH_OPEN;

  esp_wifi_set_mode(WIFI_MODE_AP);
  esp_wifi_set_config(WIFI_IF_AP, &ap);
  esp_wifi_start();

  httpd_config_t cfg = HTTPD_DEFAULT_CONFIG();
  httpd_start(&s_portal, &cfg);

  httpd_uri_t root = {.uri="/", .method=HTTP_GET, .handler=portal_root};
  httpd_uri_t save = {.uri="/save", .method=HTTP_POST, .handler=portal_save};
  httpd_register_uri_handler(s_portal, &root);
  httpd_register_uri_handler(s_portal, &save);

  ESP_LOGW(TAG, "AP portal started: connect to SSID 'SQLite-Setup' then open http://192.168.4.1/");
}

esp_err_t wifi_manager_start(void) {
  s_wifi_events = xEventGroupCreate();

  ESP_ERROR_CHECK(esp_netif_init());
  ESP_ERROR_CHECK(esp_event_loop_create_default());
  esp_netif_create_default_wifi_sta();

  wifi_init_config_t cfg = WIFI_INIT_CONFIG_DEFAULT();
  ESP_ERROR_CHECK(esp_wifi_init(&cfg));

  ESP_ERROR_CHECK(esp_event_handler_register(WIFI_EVENT, ESP_EVENT_ANY_ID, &wifi_event_handler, NULL));
  ESP_ERROR_CHECK(esp_event_handler_register(IP_EVENT, IP_EVENT_STA_GOT_IP, &wifi_event_handler, NULL));

  char ssid[32] = {0};
  char pass[64] = {0};

  bool have = load_wifi_creds(ssid, sizeof(ssid), pass, sizeof(pass));
  if (have) {
    ESP_LOGI(TAG, "Trying saved WiFi: %s", ssid);

    wifi_config_t sta = {0};
    strncpy((char*)sta.sta.ssid, ssid, sizeof(sta.sta.ssid)-1);
    strncpy((char*)sta.sta.password, pass, sizeof(sta.sta.password)-1);

    ESP_ERROR_CHECK(esp_wifi_set_mode(WIFI_MODE_STA));
    ESP_ERROR_CHECK(esp_wifi_set_config(WIFI_IF_STA, &sta));
    ESP_ERROR_CHECK(esp_wifi_start());

    EventBits_t bits = xEventGroupWaitBits(
      s_wifi_events, WIFI_CONNECTED_BIT, pdFALSE, pdFALSE, pdMS_TO_TICKS(8000)
    );
    if (bits & WIFI_CONNECTED_BIT) {
      ESP_LOGI(TAG, "WiFi connected.");
      return ESP_OK;
    }
  }

  ESP_LOGW(TAG, "WiFi not connected -> starting AP portal");
  start_ap_portal();
  return ESP_OK;
}
