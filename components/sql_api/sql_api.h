#pragma once
#include "esp_err.h"
#include "sqlite3.h"

esp_err_t sql_api_start(sqlite3 *db);
