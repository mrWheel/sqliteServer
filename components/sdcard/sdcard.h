#pragma once
#include "esp_err.h"
#include "sqlite3.h"

esp_err_t sdcard_mount(void);
esp_err_t sdcard_open_db(const char *path, sqlite3 **out_db);