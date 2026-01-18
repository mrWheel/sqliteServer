#pragma once
#include "esp_err.h"
#include "sqlite3.h"

esp_err_t db_psram_open(sqlite3** out_db);
