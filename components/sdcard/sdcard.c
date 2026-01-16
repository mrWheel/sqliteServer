#include "sdcard.h"

#include "esp_log.h"
#include "driver/spi_master.h"
#include "driver/sdspi_host.h"
#include "sdmmc_cmd.h"
#include "esp_vfs_fat.h"

static const char *TAG = "SDCARD";

/* LilyGO T8-S3 (veel gezien):
   CS=IO10, MOSI=IO11, SCLK=IO12, MISO=IO13
   Als het bij jou anders is: pas deze defines aan.
*/
#define PIN_SD_CS    10
#define PIN_SD_MOSI  11
#define PIN_SD_SCLK  12
#define PIN_SD_MISO  13

static bool s_mounted = false;

esp_err_t sdcard_mount(void) {
  if (s_mounted) return ESP_OK;

  esp_vfs_fat_sdmmc_mount_config_t mount_config = {
    .format_if_mount_failed = false,
    .max_files = 5,
    .allocation_unit_size = 16 * 1024
  };

  sdmmc_card_t *card = NULL;

  sdmmc_host_t host = SDSPI_HOST_DEFAULT();

  spi_bus_config_t bus_cfg = {
    .mosi_io_num = PIN_SD_MOSI,
    .miso_io_num = PIN_SD_MISO,
    .sclk_io_num = PIN_SD_SCLK,
    .quadwp_io_num = -1,
    .quadhd_io_num = -1,
    .max_transfer_sz = 16 * 1024
  };

  esp_err_t err = spi_bus_initialize(host.slot, &bus_cfg, SPI_DMA_CH_AUTO);
  if (err != ESP_OK) {
    ESP_LOGE(TAG, "spi_bus_initialize failed: %s", esp_err_to_name(err));
    return err;
  }

  sdspi_device_config_t slot_config = SDSPI_DEVICE_CONFIG_DEFAULT();
  slot_config.gpio_cs = PIN_SD_CS;
  slot_config.host_id = host.slot;

  err = esp_vfs_fat_sdspi_mount("/sdcard", &host, &slot_config, &mount_config, &card);
  if (err != ESP_OK) {
    ESP_LOGE(TAG, "SD mount failed: %s", esp_err_to_name(err));
    return err;
  }

  ESP_LOGI(TAG, "SD mounted at /sdcard");
  s_mounted = true;
  return ESP_OK;
}

esp_err_t sdcard_open_db(const char *path, sqlite3 **out_db) {
  if (!out_db) return ESP_ERR_INVALID_ARG;

  sqlite3 *db = NULL;
  int rc = sqlite3_open(path, &db);
  if (rc != SQLITE_OK) {
    ESP_LOGE(TAG, "sqlite3_open failed: %s", db ? sqlite3_errmsg(db) : "no db");
    if (db) sqlite3_close(db);
    return ESP_FAIL;
  }

  sqlite3_busy_timeout(db, 2000);
  *out_db = db;

  ESP_LOGI(TAG, "DB open: %s", path);
  return ESP_OK;
}