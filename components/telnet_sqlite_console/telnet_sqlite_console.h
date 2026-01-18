#pragma once

#include "esp_err.h"
#include "freertos/FreeRTOS.h"
#include "freertos/semphr.h"
#include "sqlite3.h"

#ifdef __cplusplus
extern "C" {
#endif

// Start telnet sqlite console op TCP port (aanrader: 2323)
// Last client wins: nieuwe connect => bestaande client wordt afgesloten.
esp_err_t telnet_sqlite_console_start(sqlite3 *db, SemaphoreHandle_t db_mutex, int port);

#ifdef __cplusplus
}
#endif
