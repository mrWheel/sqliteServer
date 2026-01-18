#pragma once

#include "esp_err.h"
#include "sqlite3.h"

#ifdef __cplusplus
extern "C"
{
#endif

  // Mount SD kaart op /sdcard (FATFS)
  esp_err_t sdcard_mount(void);

  // Open SQLite DB file op SD kaart (bijv. "/sdcard/app.db")
  esp_err_t sdcard_open_db(const char* full_path, sqlite3** out_db);

#ifdef __cplusplus
}
#endif
