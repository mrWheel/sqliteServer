#include "sdcard.h"

#include <string.h>
#include "esp_log.h"
#include "esp_vfs_fat.h"

#include "driver/spi_common.h"
#include "driver/sdspi_host.h"
#include "sdmmc_cmd.h"

static const char* TAG = "SDCARD";

/* TTGO-T8 (ESP32) microSD via SPI (pinnen uit jouw config):
   CS=13, MOSI=15, MISO=2, SCK=14
*/
#define PIN_SD_CS 13
#define PIN_SD_MOSI 15
#define PIN_SD_MISO 2
#define PIN_SD_SCLK 14

static bool s_mounted = false;
static sdmmc_card_t* s_card = NULL;
static spi_host_device_t s_host_slot = SDSPI_DEFAULT_HOST;

esp_err_t sdcard_mount(void)
{
  if (s_mounted)
    return ESP_OK;

  esp_vfs_fat_sdmmc_mount_config_t mount_config = {
      .format_if_mount_failed = false,
      .max_files = 5,
      .allocation_unit_size = 16 * 1024};

  sdmmc_host_t host = SDSPI_HOST_DEFAULT();
  s_host_slot = host.slot;

  spi_bus_config_t bus_cfg = {
      .mosi_io_num = PIN_SD_MOSI,
      .miso_io_num = PIN_SD_MISO,
      .sclk_io_num = PIN_SD_SCLK,
      .quadwp_io_num = -1,
      .quadhd_io_num = -1,
      .max_transfer_sz = 16 * 1024};

  esp_err_t err = spi_bus_initialize(host.slot, &bus_cfg, SPI_DMA_CH_AUTO);
  if (err != ESP_OK && err != ESP_ERR_INVALID_STATE)
  {
    ESP_LOGE(TAG, "spi_bus_initialize failed: %s", esp_err_to_name(err));
    return err;
  }

  sdspi_device_config_t slot_config = SDSPI_DEVICE_CONFIG_DEFAULT();
  slot_config.gpio_cs = PIN_SD_CS;
  slot_config.host_id = host.slot;

  err = esp_vfs_fat_sdspi_mount("/sdcard", &host, &slot_config, &mount_config, &s_card);
  if (err != ESP_OK)
  {
    ESP_LOGE(TAG, "SD mount failed: %s", esp_err_to_name(err));
    // Als mount faalt en we hebben de bus net geinit, ruim hem op:
    // (Alleen doen als jij zeker weet dat niets anders op dezelfde bus zit.)
    // spi_bus_free(host.slot);
    return err;
  }

  // Optioneel: kaartinfo
  sdmmc_card_print_info(stdout, s_card);

  ESP_LOGI(TAG, "SD mounted at /sdcard");
  s_mounted = true;
  return ESP_OK;
}

esp_err_t sdcard_open_db(const char* full_path, sqlite3** out_db)
{
  if (!out_db || !full_path || full_path[0] == '\0')
    return ESP_ERR_INVALID_ARG;
  if (!s_mounted)
  {
    esp_err_t err = sdcard_mount();
    if (err != ESP_OK)
      return err;
  }

  // Veiligheidscheck: forceer dat je echt naar SD wijst
  if (strncmp(full_path, "/sdcard/", 8) != 0)
  {
    ESP_LOGE(TAG, "Refusing to open DB outside /sdcard: %s", full_path);
    return ESP_ERR_INVALID_ARG;
  }

  sqlite3* db = NULL;
  int rc = sqlite3_open(full_path, &db);
  if (rc != SQLITE_OK)
  {
    ESP_LOGE(TAG, "sqlite3_open failed (%d): %s", rc, db ? sqlite3_errmsg(db) : "no db");
    if (db)
      sqlite3_close(db);
    return ESP_FAIL;
  }

  // Handig op embedded
  sqlite3_busy_timeout(db, 2000);

  *out_db = db;
  ESP_LOGI(TAG, "DB open: %s", full_path);
  return ESP_OK;
}
