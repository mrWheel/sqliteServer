#pragma once
#include "esp_err.h"
#include "sqlite3.h"
#include "freertos/FreeRTOS.h"
#include "freertos/semphr.h"

esp_err_t telnet_sqlite_console_start(sqlite3* db, SemaphoreHandle_t db_mutex, int port);