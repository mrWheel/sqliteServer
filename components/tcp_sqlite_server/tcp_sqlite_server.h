#pragma once

#include "esp_err.h"
#include "freertos/FreeRTOS.h"
#include "freertos/semphr.h"
#include "sqlite3.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
  int port;                    // TCP port
  int max_clients;             // meestal 1 is prima
  int max_stmts_per_client;    // bijv. 8
  int rx_line_max;             // bijv. 2048
  int tx_line_max;             // bijv. 4096
  int client_task_stack;       // bytes
  int client_task_prio;
} tcp_sqlite_server_cfg_t;

// Start TCP server task (luistert en accepteert clients)
esp_err_t tcp_sqlite_server_start(sqlite3* db, SemaphoreHandle_t db_mutex, const tcp_sqlite_server_cfg_t* cfg);

#ifdef __cplusplus
}
#endif
