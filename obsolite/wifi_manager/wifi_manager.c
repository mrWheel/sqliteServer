#include "wifi_manager.h"

#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <ctype.h>

#include "freertos/FreeRTOS.h"
#include "freertos/event_groups.h"

#include "esp_wifi.h"
#include "esp_event.h"
#include "esp_log.h"
#include "esp_netif.h"
#include "esp_netif_ip_addr.h"
#include "nvs.h"
#include "esp_http_server.h"
#include "esp_system.h"

static const char* TAG = "WIFIMGR";

#define WIFI_CONNECTED_BIT BIT0

static EventGroupHandle_t s_wifi_events;
static httpd_handle_t s_portal = NULL;
static bool s_event_loop_ready = false;

/* ---------------- NVS creds ---------------- */
static bool load_wifi_creds(char* ssid, size_t ssid_len, char* pass, size_t pass_len)
{
  nvs_handle_t nvs;
  if (nvs_open("wifi", NVS_READONLY, &nvs) != ESP_OK)
    return false;

  esp_err_t err;
  err = nvs_get_str(nvs, "ssid", ssid, &ssid_len);
  if (err != ESP_OK)
  {
    nvs_close(nvs);
    return false;
  }

  err = nvs_get_str(nvs, "pass", pass, &pass_len);
  if (err != ESP_OK)
  {
    nvs_close(nvs);
    return false;
  }

  nvs_close(nvs);
  return true;
}

static void save_wifi_creds(const char* ssid, const char* pass)
{
  nvs_handle_t nvs;
  nvs_open("wifi", NVS_READWRITE, &nvs);
  nvs_set_str(nvs, "ssid", ssid);
  nvs_set_str(nvs, "pass", pass);
  nvs_commit(nvs);
  nvs_close(nvs);
}

/* ---------------- URL decode ---------------- */
static void url_decode_inplace(char* s)
{
  // minimale decode: + => space, %HH
  char* o = s;
  while (*s)
  {
    if (*s == '+')
    {
      *o++ = ' ';
      s++;
    }
    else if (*s == '%' && isxdigit((unsigned char)s[1]) && isxdigit((unsigned char)s[2]))
    {
      char hex[3] = {s[1], s[2], 0};
      *o++ = (char)strtol(hex, NULL, 16);
      s += 3;
    }
    else
    {
      *o++ = *s++;
    }
  }
  *o = 0;
}

/* ---------------- WiFi event handler ---------------- */
static void wifi_event_handler(void* arg, esp_event_base_t base, int32_t id, void* data)
{
  (void)arg;

  if (base == WIFI_EVENT && id == WIFI_EVENT_STA_START)
  {
    esp_wifi_connect();
  }
  else if (base == WIFI_EVENT && id == WIFI_EVENT_STA_DISCONNECTED)
  {
    ESP_LOGW(TAG, "STA disconnected");
    // geen infinite reconnect loop hier; we doen reconnect attempt bij boot
  }
  else if (base == IP_EVENT && id == IP_EVENT_STA_GOT_IP)
  {
    ip_event_got_ip_t* event = (ip_event_got_ip_t*)data;
    ESP_LOGI(TAG, "Got IP: " IPSTR, IP2STR(&event->ip_info.ip));
    xEventGroupSetBits(s_wifi_events, WIFI_CONNECTED_BIT);
  }
}

/* ---------------- Captive portal HTML ---------------- */
static const char* HTML_FORM =
    "<!doctype html><html><head><meta charset='utf-8'/>"
    "<meta name='viewport' content='width=device-width,initial-scale=1'/>"
    "<title>WiFi Setup</title></head><body>"
    "<h3>WiFi setup</h3>"
    "<form method='POST' action='/save'>"
    "SSID:<br><input name='s' maxlength='32' style='width: 260px;'><br><br>"
    "Password:<br><input name='p' type='password' maxlength='64' style='width: 260px;'><br><br>"
    "<input type='submit' value='Save & Reboot'>"
    "</form>"
    "<p style='margin-top:20px;color:#666'>Na reboot: kijk in serial monitor voor het IP.</p>"
    "</body></html>";

static esp_err_t portal_root(httpd_req_t* req)
{
  httpd_resp_set_type(req, "text/html");
  httpd_resp_send(req, HTML_FORM, HTTPD_RESP_USE_STRLEN);
  return ESP_OK;
}

static esp_err_t portal_favicon(httpd_req_t* req)
{
  httpd_resp_set_status(req, "204 No Content");
  httpd_resp_send(req, NULL, 0);
  return ESP_OK;
}

static esp_err_t portal_save(httpd_req_t* req)
{
  int total = req->content_len;
  if (total <= 0 || total > 1024)
  {
    httpd_resp_send_err(req, HTTPD_400_BAD_REQUEST, "bad form size");
    return ESP_FAIL;
  }

  char* buf = (char*)calloc(1, total + 1);
  if (!buf)
  {
    httpd_resp_send_err(req, HTTPD_500_INTERNAL_SERVER_ERROR, "oom");
    return ESP_FAIL;
  }

  int received = 0;
  while (received < total)
  {
    int r = httpd_req_recv(req, buf + received, total - received);
    if (r <= 0)
    {
      free(buf);
      httpd_resp_send_err(req, HTTPD_400_BAD_REQUEST, "recv failed");
      return ESP_FAIL;
    }
    received += r;
  }
  buf[total] = 0;

  ESP_LOGI(TAG, "POST /save body (%d bytes): %s", total, buf);

  // verwacht: s=SSID&p=PASS
  char* s = strstr(buf, "s=");
  char* p = strstr(buf, "&p=");

  if (!s)
  {
    free(buf);
    httpd_resp_send_err(req, HTTPD_400_BAD_REQUEST, "missing ssid");
    return ESP_FAIL;
  }

  char ssid[33] = {0};
  char pass[65] = {0};

  s += 2;
  if (p)
  {
    *p = 0;
    p += 3;
    strncpy(pass, p, sizeof(pass) - 1);
  }
  strncpy(ssid, s, sizeof(ssid) - 1);

  url_decode_inplace(ssid);
  url_decode_inplace(pass);

  // strip CR/LF (sommige clients)
  for (char* x = ssid; *x; x++)
    if (*x == '\r' || *x == '\n')
      *x = 0;
  for (char* x = pass; *x; x++)
    if (*x == '\r' || *x == '\n')
      *x = 0;

  ESP_LOGI(TAG, "Saving WiFi SSID='%s' (pass len=%u)", ssid, (unsigned)strlen(pass));
  save_wifi_creds(ssid, pass);

  free(buf);

  httpd_resp_set_type(req, "text/plain");
  httpd_resp_sendstr(req, "Saved. Rebooting...");
  vTaskDelay(pdMS_TO_TICKS(700));
  esp_restart();
  return ESP_OK;
}

static void start_ap_portal(void)
{
  ESP_LOGW(TAG, "WiFi not connected -> starting AP portal");

  // AP netif
  esp_netif_create_default_wifi_ap();

  wifi_config_t ap = {0};
  snprintf((char*)ap.ap.ssid, sizeof(ap.ap.ssid), "SQLite-Setup");
  ap.ap.channel = 1;
  ap.ap.max_connection = 4;
  ap.ap.authmode = WIFI_AUTH_OPEN;

  ESP_ERROR_CHECK(esp_wifi_set_mode(WIFI_MODE_AP));
  ESP_ERROR_CHECK(esp_wifi_set_config(WIFI_IF_AP, &ap));
  ESP_ERROR_CHECK(esp_wifi_start());

  httpd_config_t cfg = HTTPD_DEFAULT_CONFIG();
  cfg.max_uri_handlers = 16;
  cfg.stack_size = 8192;

  ESP_ERROR_CHECK(httpd_start(&s_portal, &cfg));

  httpd_uri_t root = {.uri = "/", .method = HTTP_GET, .handler = portal_root};
  httpd_uri_t save = {.uri = "/save", .method = HTTP_POST, .handler = portal_save};
  httpd_uri_t fav = {.uri = "/favicon.ico", .method = HTTP_GET, .handler = portal_favicon};

  httpd_register_uri_handler(s_portal, &root);
  httpd_register_uri_handler(s_portal, &save);
  httpd_register_uri_handler(s_portal, &fav);

  ESP_LOGW(TAG, "AP portal started: connect to SSID 'SQLite-Setup' then open http://192.168.4.1/");
}

/* ---------------- Public API ---------------- */
esp_err_t wifi_manager_start(void)
{
  if (!s_wifi_events)
    s_wifi_events = xEventGroupCreate();

  ESP_ERROR_CHECK(esp_netif_init());

  // Event loop mag maar 1x; bij herstart in dezelfde run kan ESP_ERR_INVALID_STATE komen.
  if (!s_event_loop_ready)
  {
    esp_err_t err = esp_event_loop_create_default();
    if (err != ESP_OK && err != ESP_ERR_INVALID_STATE)
      return err;
    s_event_loop_ready = true;
  }

  // STA netif
  esp_netif_create_default_wifi_sta();

  wifi_init_config_t cfg = WIFI_INIT_CONFIG_DEFAULT();
  ESP_ERROR_CHECK(esp_wifi_init(&cfg));

  ESP_ERROR_CHECK(esp_event_handler_register(WIFI_EVENT, ESP_EVENT_ANY_ID, &wifi_event_handler, NULL));
  ESP_ERROR_CHECK(esp_event_handler_register(IP_EVENT, IP_EVENT_STA_GOT_IP, &wifi_event_handler, NULL));

  char ssid[33] = {0};
  char pass[65] = {0};

  bool have = load_wifi_creds(ssid, sizeof(ssid), pass, sizeof(pass));
  if (have)
  {
    ESP_LOGI(TAG, "Trying saved WiFi SSID='%s'", ssid);

    wifi_config_t sta = {0};
    strncpy((char*)sta.sta.ssid, ssid, sizeof(sta.sta.ssid) - 1);
    strncpy((char*)sta.sta.password, pass, sizeof(sta.sta.password) - 1);

    ESP_ERROR_CHECK(esp_wifi_set_mode(WIFI_MODE_STA));
    ESP_ERROR_CHECK(esp_wifi_set_config(WIFI_IF_STA, &sta));
    ESP_ERROR_CHECK(esp_wifi_start());

    // wacht op IP (10s)
    EventBits_t bits = xEventGroupWaitBits(
        s_wifi_events,
        WIFI_CONNECTED_BIT,
        pdFALSE,
        pdFALSE,
        pdMS_TO_TICKS(10000));

    if (bits & WIFI_CONNECTED_BIT)
    {
      ESP_LOGI(TAG, "WiFi connected (STA).");
      return ESP_OK;
    }

    ESP_LOGW(TAG, "STA connect timed out");
    // Stop STA voordat we AP starten (netjes)
    esp_wifi_stop();
  }

  // Geen creds of connect faalt -> AP portal
  start_ap_portal();
  return ESP_OK;
}
