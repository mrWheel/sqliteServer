#include "telnet_sqlite_console.h"

#include <stdio.h>
#include <string.h>
#include <ctype.h>
#include <stdlib.h>
#include <errno.h>
#include <stdarg.h>

#include <dirent.h>
#include <sys/stat.h>
#include <unistd.h>

#include "esp_log.h"
#include "freertos/FreeRTOS.h"
#include "freertos/task.h"

#include "lwip/sockets.h"
#include "lwip/netdb.h"

// We gebruiken libtelnet uit de esp_telnet component.
// Pad klopt als je esp_telnet/src als include dir toevoegt.
#include "libtelnet.h"

// ---- telnet option numbers (als jouw libtelnet.h ze niet definieert) ----
#ifndef TELOPT_ECHO
  #define TELOPT_ECHO 1
#endif
#ifndef TELOPT_SGA
  #define TELOPT_SGA 3
#endif
#ifndef TELOPT_TTYPE
  #define TELOPT_TTYPE 24
#endif
#ifndef TELOPT_NAWS
  #define TELOPT_NAWS 31
#endif

static const char* TAG = "TELNETSQL";

static sqlite3* s_db = NULL;
static SemaphoreHandle_t s_db_mutex = NULL;

static int s_listen_fd = -1;
static int s_client_fd = -1;

/* ---------- Console options (sqlite-ish) ---------- */
typedef enum {
  MODE_LIST = 0,
  MODE_CSV  = 1,
  MODE_TABS = 2,
} out_mode_t;

static struct {
  bool headers;
  bool echo;
  out_mode_t mode;
  char sep[8];
  char nullvalue[16];
} s_opt = {
  .headers   = true,
  .echo      = true,
  .mode      = MODE_LIST,
  .sep       = "|",
  .nullvalue = "NULL",
};

/* ---------- telnet session state (1 client) ---------- */
typedef struct {
  telnet_t* telnet;
  char line[512];
  int  llen;
} session_t;

static session_t s_sess = {0};

/* ---------- forward decls ---------- */
static void close_client_locked(void);

static void send_str_tel(const char* s);
static void sendf_tel(const char* fmt, ...);
static void prompt_tel(void);

static void trim(char* s);
static void split_first_token(char* line, char** tok, char** rest);

static void exec_sql_text(const char* sql_in);

static void dot_read(const char* path);
static void dot_import(char* args);

static void dot_help(void);
static void dot_tables(void);
static void dot_schema(const char* table);
static void dot_dbinfo(void);

// fs dot commands
static void dot_ls(const char* path);
static void dot_cat(const char* path);
static void dot_rm(const char* path);
static void dot_mv(const char* args);

static bool handle_dot_command(char* line);

/* ---------- telnet send plumbing ---------- */
// libtelnet vraagt ons om bytes te versturen via TELNET_EV_SEND.
// Dus send_str_tel() schrijft naar telnet_send(), en telnet_event handler doet send().

static void telnet_event_handler(telnet_t* telnet, telnet_event_t* ev, void* user_data)
{
  (void)telnet;
  (void)user_data;

  switch (ev->type)
  {
    case TELNET_EV_SEND:
      if (s_client_fd >= 0 && ev->data.buffer && ev->data.size > 0)
      {
        // best-effort; bij errors sluiten we de client later in recv loop
        (void)send(s_client_fd, ev->data.buffer, ev->data.size, 0);
      }
      break;

    case TELNET_EV_ERROR:
      ESP_LOGW(TAG, "telnet error: %s", ev->error.msg ? ev->error.msg : "(null)");
      break;

    default:
      // DATA verwerken we via telnet_recv() -> TELNET_EV_DATA events (zie hieronder)
      break;
  }
}

static const telnet_telopt_t s_telopts[] = {
  // server WILL ECHO + SGA
  { TELOPT_ECHO, TELNET_WILL, TELNET_DONT },
  { TELOPT_SGA,  TELNET_WILL, TELNET_DONT },

  // we ask client for NAWS + TTYPE (client WILL)
  { TELOPT_NAWS,  TELNET_DO,   TELNET_DONT },
  { TELOPT_TTYPE, TELNET_DO,   TELNET_DONT },

  // netjes afsluiten
  { -1, 0, 0 }
};

static void send_str_tel(const char* s)
{
  if (!s || !s_sess.telnet) return;
  telnet_send(s_sess.telnet, s, (unsigned)strlen(s));
}

static void sendf_tel(const char* fmt, ...)
{
  if (!fmt) return;
  char buf[512];
  va_list ap;
  va_start(ap, fmt);
  vsnprintf(buf, sizeof(buf), fmt, ap);
  va_end(ap);
  send_str_tel(buf);
}

static void prompt_tel(void)
{
  send_str_tel("sqlite> ");
}

/* ---------- connection mgmt ---------- */
static void close_client_locked(void)
{
  if (s_client_fd >= 0)
  {
    shutdown(s_client_fd, SHUT_RDWR);
    close(s_client_fd);
    s_client_fd = -1;
  }

  if (s_sess.telnet)
  {
    telnet_free(s_sess.telnet);
    s_sess.telnet = NULL;
  }

  memset(&s_sess, 0, sizeof(s_sess));
}

/* ---------- helpers ---------- */
static void trim(char* s)
{
  if (!s) return;
  char* p = s;
  while (*p && isspace((unsigned char)*p)) p++;
  if (p != s) memmove(s, p, strlen(p) + 1);

  size_t n = strlen(s);
  while (n > 0 && isspace((unsigned char)s[n - 1]))
  {
    s[n - 1] = 0;
    n--;
  }
}

static void split_first_token(char* line, char** tok, char** rest)
{
  *tok = line;
  while (**tok && isspace((unsigned char)**tok)) (*tok)++;
  char* p = *tok;
  while (*p && !isspace((unsigned char)*p)) p++;
  if (*p) { *p = 0; p++; }
  while (*p && isspace((unsigned char)*p)) p++;
  *rest = p;
}

/* ---------- sqlite printing ---------- */
static void print_value(int t, sqlite3_stmt* stmt, int col)
{
  if (t == SQLITE_NULL)
  {
    send_str_tel(s_opt.nullvalue);
  }
  else if (t == SQLITE_INTEGER)
  {
    char buf[32];
    snprintf(buf, sizeof(buf), "%lld", (long long)sqlite3_column_int64(stmt, col));
    send_str_tel(buf);
  }
  else if (t == SQLITE_FLOAT)
  {
    char buf[64];
    snprintf(buf, sizeof(buf), "%.17g", sqlite3_column_double(stmt, col));
    send_str_tel(buf);
  }
  else
  {
    const unsigned char* txt = sqlite3_column_text(stmt, col);
    send_str_tel(txt ? (const char*)txt : "");
  }
}

static void print_row_sep(void)
{
  if (s_opt.mode == MODE_TABS) send_str_tel("\t");
  else                        send_str_tel(s_opt.sep);
}

/* ---------- SQL execution (multi-statement) ---------- */
static void exec_sql_text(const char* sql_in)
{
  if (!sql_in || !*sql_in) return;

  xSemaphoreTake(s_db_mutex, portMAX_DELAY);

  const char* sql = sql_in;
  sqlite3_stmt* stmt = NULL;

  while (1)
  {
    int rc = sqlite3_prepare_v2(s_db, sql, -1, &stmt, &sql);
    if (rc != SQLITE_OK)
    {
      sendf_tel("ERR: %s\r\n", sqlite3_errmsg(s_db));
      if (stmt) sqlite3_finalize(stmt);
      break;
    }
    if (!stmt) break; // klaar

    int cols = sqlite3_column_count(stmt);
    if (cols > 0)
    {
      if (s_opt.headers)
      {
        for (int c = 0; c < cols; c++)
        {
          if (c) print_row_sep();
          send_str_tel(sqlite3_column_name(stmt, c));
        }
        send_str_tel("\r\n");
      }

      int s;
      while ((s = sqlite3_step(stmt)) == SQLITE_ROW)
      {
        for (int c = 0; c < cols; c++)
        {
          if (c) print_row_sep();
          int t = sqlite3_column_type(stmt, c);
          print_value(t, stmt, c);
        }
        send_str_tel("\r\n");
      }
      if (s != SQLITE_DONE)
      {
        sendf_tel("ERR: %s\r\n", sqlite3_errmsg(s_db));
        sqlite3_finalize(stmt);
        break;
      }
    }
    else
    {
      int s = sqlite3_step(stmt);
      if (s != SQLITE_DONE)
      {
        sendf_tel("ERR: %s\r\n", sqlite3_errmsg(s_db));
        sqlite3_finalize(stmt);
        break;
      }
      sendf_tel("OK (changes=%d last_id=%lld)\r\n",
                sqlite3_changes(s_db),
                (long long)sqlite3_last_insert_rowid(s_db));
    }

    sqlite3_finalize(stmt);
    stmt = NULL;
  }

  xSemaphoreGive(s_db_mutex);
}

/* ---------- .read ---------- */
#define READ_MAX_BYTES (256 * 1024)

static void dot_read(const char* path)
{
  if (!path || !*path)
  {
    send_str_tel("Usage: .read /spiffs/init.sql\r\n");
    return;
  }

  FILE* f = fopen(path, "rb");
  if (!f)
  {
    sendf_tel("ERR: cannot open %s\r\n", path);
    return;
  }

  if (fseek(f, 0, SEEK_END) != 0)
  {
    fclose(f);
    send_str_tel("ERR: fseek failed\r\n");
    return;
  }
  long sz = ftell(f);
  if (sz < 0)
  {
    fclose(f);
    send_str_tel("ERR: ftell failed\r\n");
    return;
  }
  if (sz > READ_MAX_BYTES)
  {
    fclose(f);
    sendf_tel("ERR: file too large (%ld bytes, max %d)\r\n", sz, (int)READ_MAX_BYTES);
    return;
  }
  rewind(f);

  char* buf = (char*)malloc((size_t)sz + 1);
  if (!buf)
  {
    fclose(f);
    send_str_tel("ERR: OOM\r\n");
    return;
  }

  size_t n = fread(buf, 1, (size_t)sz, f);
  fclose(f);

  buf[n] = 0;

  sendf_tel("-- .read %s (%u bytes)\r\n", path, (unsigned)n);
  exec_sql_text(buf);
  free(buf);
}

/* ---------- .import (zelfde als jouw oude, maar output via telnet) ---------- */
static void sqlite_exec_simple(const char* sql)
{
  char* err = NULL;
  int rc = sqlite3_exec(s_db, sql, NULL, NULL, &err);
  if (rc != SQLITE_OK)
  {
    sendf_tel("ERR: %s\r\n", err ? err : sqlite3_errmsg(s_db));
    if (err) sqlite3_free(err);
  }
}

static int parse_csv_inplace(char* buf, char** out, int max_fields)
{
  int n = 0;
  char* p = buf;

  while (*p && n < max_fields)
  {
    char* start = p;
    if (*p == '"')
    {
      p++;
      start = p;
      char* w = p;
      while (*p)
      {
        if (*p == '"' && p[1] == '"')
        {
          *w++ = '"';
          p += 2;
          continue;
        }
        if (*p == '"')
        {
          p++;
          break;
        }
        *w++ = *p++;
      }
      *w = 0;

      while (*p && *p != ',') p++;
      if (*p == ',') { *p = 0; p++; }
      out[n++] = start;
    }
    else
    {
      while (*p && *p != ',') p++;
      if (*p == ',') { *p = 0; p++; }

      char* e = start + strlen(start);
      while (e > start && (e[-1] == '\r' || e[-1] == '\n')) e--;
      while (e > start && isspace((unsigned char)e[-1])) e--;
      *e = 0;
      out[n++] = start;
    }
  }

  if (n > 0)
  {
    char* s = out[n - 1];
    size_t L = strlen(s);
    while (L > 0 && (s[L - 1] == '\r' || s[L - 1] == '\n'))
    {
      s[L - 1] = 0;
      L--;
    }
  }
  return n;
}

static int split_sep_inplace(char* buf, char** out, int max_fields, char sep)
{
  int n = 0;
  char* p = buf;

  size_t L = strlen(p);
  while (L > 0 && (p[L - 1] == '\r' || p[L - 1] == '\n'))
  {
    p[L - 1] = 0;
    L--;
  }

  while (*p && n < max_fields)
  {
    out[n++] = p;
    char* q = strchr(p, sep);
    if (!q) break;
    *q = 0;
    p = q + 1;
  }
  return n;
}

static bool is_valid_identifier_like(const char* s)
{
  if (!s || !*s) return false;
  for (const char* p = s; *p; p++)
  {
    if (!(isalnum((unsigned char)*p) || *p == '_' || *p == '.'))
      return false;
  }
  return true;
}

static void dot_import(char* args)
{
  bool csv = false;
  char sep = (s_opt.mode == MODE_TABS) ? '\t' : s_opt.sep[0];
  int skip = 0;

  char* tokv[16];
  int tokc = 0;
  char* p = args;
  while (p && *p)
  {
    while (*p && isspace((unsigned char)*p)) p++;
    if (!*p) break;
    tokv[tokc++] = p;
    if (tokc >= (int)(sizeof(tokv) / sizeof(tokv[0]))) break;
    while (*p && !isspace((unsigned char)*p)) p++;
    if (*p) *p++ = 0;
  }

  int i = 0;
  for (; i < tokc; i++)
  {
    if (strcmp(tokv[i], "--csv") == 0) { csv = true; continue; }
    if (strcmp(tokv[i], "--tabs") == 0) { csv = false; sep = '\t'; continue; }
    if (strcmp(tokv[i], "--separator") == 0)
    {
      if (i + 1 >= tokc) { send_str_tel("Usage: .import --separator X <file> <table>\r\n"); return; }
      sep = tokv[++i][0];
      csv = false;
      continue;
    }
    if (strcmp(tokv[i], "--skip") == 0)
    {
      if (i + 1 >= tokc) { send_str_tel("Usage: .import --skip N <file> <table>\r\n"); return; }
      skip = atoi(tokv[++i]);
      if (skip < 0) skip = 0;
      continue;
    }
    break;
  }

  if (tokc - i < 2)
  {
    send_str_tel("Usage: .import [--csv] [--tabs] [--separator X] [--skip N] <file> <table>\r\n");
    return;
  }

  const char* file  = tokv[i];
  const char* table = tokv[i + 1];

  if (!is_valid_identifier_like(table))
  {
    send_str_tel("ERR: invalid table name (allowed: a-z A-Z 0-9 _ .)\r\n");
    return;
  }

  FILE* f = fopen(file, "rb");
  if (!f) { sendf_tel("ERR: cannot open %s\r\n", file); return; }

  char linebuf[1024];
  for (int s = 0; s < skip; s++)
  {
    if (!fgets(linebuf, sizeof(linebuf), f)) break;
  }

  if (!fgets(linebuf, sizeof(linebuf), f))
  {
    fclose(f);
    send_str_tel("ERR: empty file (after skip)\r\n");
    return;
  }

  char work[1024];
  strncpy(work, linebuf, sizeof(work) - 1);
  work[sizeof(work) - 1] = 0;

  char* fields[64];
  int nfields = csv ? parse_csv_inplace(work, fields, 64) : split_sep_inplace(work, fields, 64, sep);
  if (nfields <= 0)
  {
    fclose(f);
    send_str_tel("ERR: could not parse first row\r\n");
    return;
  }

  char sql[256];
  strcpy(sql, "INSERT INTO ");
  strncat(sql, table, sizeof(sql) - strlen(sql) - 1);
  strncat(sql, " VALUES(", sizeof(sql) - strlen(sql) - 1);
  for (int c = 0; c < nfields; c++)
  {
    strncat(sql, "?", sizeof(sql) - strlen(sql) - 1);
    if (c != nfields - 1) strncat(sql, ",", sizeof(sql) - strlen(sql) - 1);
  }
  strncat(sql, ");", sizeof(sql) - strlen(sql) - 1);

  xSemaphoreTake(s_db_mutex, portMAX_DELAY);

  sqlite_exec_simple("BEGIN;");
  sqlite3_stmt* stmt = NULL;
  int prc = sqlite3_prepare_v2(s_db, sql, -1, &stmt, NULL);
  if (prc != SQLITE_OK || !stmt)
  {
    sendf_tel("ERR: prepare failed: %s\r\n", sqlite3_errmsg(s_db));
    sqlite_exec_simple("ROLLBACK;");
    xSemaphoreGive(s_db_mutex);
    fclose(f);
    return;
  }

  long rows = 0;
  bool had_error = false;

  // first row (already read)
  {
    char w2[1024];
    strncpy(w2, linebuf, sizeof(w2) - 1);
    w2[sizeof(w2) - 1] = 0;

    char* f2[64];
    int nf2 = csv ? parse_csv_inplace(w2, f2, 64) : split_sep_inplace(w2, f2, 64, sep);
    if (nf2 == nfields)
    {
      sqlite3_reset(stmt);
      sqlite3_clear_bindings(stmt);
      for (int c = 0; c < nfields; c++)
        sqlite3_bind_text(stmt, c + 1, f2[c] ? f2[c] : "", -1, SQLITE_TRANSIENT);

      int s = sqlite3_step(stmt);
      if (s != SQLITE_DONE) { sendf_tel("ERR: step failed: %s\r\n", sqlite3_errmsg(s_db)); had_error = true; }
      else rows++;
    }
    else
    {
      sendf_tel("WARN: column count mismatch (got %d expected %d), skipping row\r\n", nf2, nfields);
    }
  }

  while (!had_error && fgets(linebuf, sizeof(linebuf), f))
  {
    char w2[1024];
    strncpy(w2, linebuf, sizeof(w2) - 1);
    w2[sizeof(w2) - 1] = 0;

    char* f2[64];
    int nf2 = csv ? parse_csv_inplace(w2, f2, 64) : split_sep_inplace(w2, f2, 64, sep);
    if (nf2 != nfields)
    {
      sendf_tel("WARN: column count mismatch (got %d expected %d), skipping row\r\n", nf2, nfields);
      continue;
    }

    sqlite3_reset(stmt);
    sqlite3_clear_bindings(stmt);
    for (int c = 0; c < nfields; c++)
      sqlite3_bind_text(stmt, c + 1, f2[c] ? f2[c] : "", -1, SQLITE_TRANSIENT);

    int s = sqlite3_step(stmt);
    if (s != SQLITE_DONE)
    {
      sendf_tel("ERR: step failed: %s\r\n", sqlite3_errmsg(s_db));
      had_error = true;
    }
    else
    {
      rows++;
    }

    if ((rows % 500) == 0) vTaskDelay(pdMS_TO_TICKS(1));
  }

  sqlite3_finalize(stmt);

  if (!had_error) sqlite_exec_simple("COMMIT;");
  else            sqlite_exec_simple("ROLLBACK;");

  xSemaphoreGive(s_db_mutex);
  fclose(f);

  if (!had_error) sendf_tel("Imported %ld rows into %s\r\n", rows, table);
  else            send_str_tel("Import failed (rolled back)\r\n");
}

/* ---------- dot commands ---------- */
static void dot_help(void)
{
  send_str_tel(
    "Dot commands:\r\n"
    "  .help\r\n"
    "  .quit | .exit\r\n"
    "  .tables\r\n"
    "  .schema [table]\r\n"
    "  .headers on|off\r\n"
    "  .mode list|csv|tabs\r\n"
    "  .separator <sep>\r\n"
    "  .nullvalue <text>\r\n"
    "  .timeout <ms>\r\n"
    "  .echo on|off\r\n"
    "  .dbinfo\r\n"
    "  .read <file.sql>\r\n"
    "  .import [--csv] [--tabs] [--separator X] [--skip N] <file> <table>\r\n"
    "\r\n"
    "Filesystem:\r\n"
    "  .ls [dir]             List directory (default /)\r\n"
    "  .cat <file>           Show file contents\r\n"
    "  .rm <file>            Remove file\r\n"
    "  .mv <src> <dst>       Rename or move file\r\n"
    "  .pwd                 Print current directory\r\n"
    "  .cd [dir]            Change directory (default /sdcard)\r\n"
    "  .df                  Show filesystem usage (not implementd)\r\n"
    "\r\n"
    "Notes:\r\n"
    "  - .read reads up to 256KB per file.\r\n"
    "  - .import binds all fields as TEXT.\r\n"
    "  - Use --skip 1 for CSV header lines.\r\n"
  );
}

static void dot_tables(void)
{
  exec_sql_text("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;");
}

static void dot_schema(const char* table)
{
  if (!table || !*table)
  {
    exec_sql_text("SELECT sql FROM sqlite_master WHERE sql IS NOT NULL ORDER BY name;");
    return;
  }
  char* q = sqlite3_mprintf("SELECT sql FROM sqlite_master WHERE sql IS NOT NULL AND name=%Q;", table);
  if (q)
  {
    exec_sql_text(q);
    sqlite3_free(q);
  }
}

static void dot_dbinfo(void)
{
  sendf_tel("SQLite version: %s\r\n", sqlite3_libversion());
  xSemaphoreTake(s_db_mutex, portMAX_DELAY);
  sendf_tel("changes=%d last_insert_rowid=%lld\r\n",
            sqlite3_changes(s_db),
            (long long)sqlite3_last_insert_rowid(s_db));
  xSemaphoreGive(s_db_mutex);
}

/* ---------- filesystem dot commands ---------- */
static void dot_ls(const char* path)
{
  const char* dirpath = (path && *path) ? path : "/sdcard";

  DIR* d = opendir(dirpath);
  if (!d)
  {
    sendf_tel("ERR: cannot open directory '%s' (errno=%d)\r\n", dirpath, errno);
    return;
  }

  sendf_tel("Listing %s\r\n", dirpath);

  struct dirent* e;
  while ((e = readdir(d)) != NULL)
  {
    char full[256];
    int n;

    if (strcmp(dirpath, "/") == 0)
      n = snprintf(full, sizeof(full), "/%s", e->d_name);
    else
      n = snprintf(full, sizeof(full), "%s/%s", dirpath, e->d_name);

    // snprintf protection: voorkomt -Wformat-truncation
    if (n < 0 || n >= (int)sizeof(full))
    {
      sendf_tel("[SKIP] path too long: %s/%s\r\n", dirpath, e->d_name);
      continue;
    }

    struct stat st;
    if (stat(full, &st) == 0)
    {
      if (S_ISDIR(st.st_mode))
        sendf_tel("[DIR ] %s\r\n", e->d_name);
      else
        sendf_tel("[FILE] %s (%ld bytes)\r\n", e->d_name, (long)st.st_size);
    }
    else
    {
      sendf_tel("[????] %s\r\n", e->d_name);
    }
  }

  closedir(d);
}

static void dot_cat(const char* path)
{
  if (!path || !*path)
  {
    send_str_tel("Usage: .cat <file>\r\n");
    return;
  }

  FILE* f = fopen(path, "rb");
  if (!f)
  {
    sendf_tel("ERR: cannot open '%s' (errno=%d)\r\n", path, errno);
    return;
  }

  char buf[256];
  size_t n;
  while ((n = fread(buf, 1, sizeof(buf), f)) > 0)
  {
    if (s_sess.telnet) telnet_send(s_sess.telnet, buf, (unsigned)n);
  }

  fclose(f);
  send_str_tel("\r\n");
}

static void dot_rm(const char* path)
{
  if (!path || !*path)
  {
    send_str_tel("Usage: .rm <file>\r\n");
    return;
  }

  if (unlink(path) == 0)
    sendf_tel("OK: removed '%s'\r\n", path);
  else
    sendf_tel("ERR: cannot remove '%s' (errno=%d)\r\n", path, errno);
}

static void dot_mv(const char* args)
{
  if (!args || !*args)
  {
    send_str_tel("Usage: .mv <src> <dst>\r\n");
    return;
  }

  char src[160] = {0}, dst[160] = {0};
  if (sscanf(args, "%159s %159s", src, dst) != 2)
  {
    send_str_tel("Usage: .mv <src> <dst>\r\n");
    return;
  }

  if (rename(src, dst) == 0)
    sendf_tel("OK: %s -> %s\r\n", src, dst);
  else
    sendf_tel("ERR: cannot rename %s -> %s (errno=%d)\r\n", src, dst, errno);
}

// ---- current working directory (per "console", single client) ----
static char s_cwd[256] = "/sdcard";   // start hier, past bij jouw .ls output

static void path_join(char* out, size_t out_sz, const char* base, const char* rel)
{
  if (!out || out_sz == 0) return;

  if (!rel || !*rel) {
    strncpy(out, base ? base : "/", out_sz - 1);
    out[out_sz - 1] = 0;
    return;
  }

  // absolute
  if (rel[0] == '/') {
    strncpy(out, rel, out_sz - 1);
    out[out_sz - 1] = 0;
    return;
  }

  // base "/" special
  if (!base || !*base || strcmp(base, "/") == 0) {
    snprintf(out, out_sz, "/%s", rel);
    out[out_sz - 1] = 0;
    return;
  }

  // normal
  snprintf(out, out_sz, "%s/%s", base, rel);
  out[out_sz - 1] = 0;
}

static void dot_pwd(void)
{
  sendf_tel("%s\r\n", s_cwd);
}

static void dot_cd( const char* path)
{
  if (!path || !*path) {
    // cd zonder args -> naar /sdcard
    strncpy(s_cwd, "/sdcard", sizeof(s_cwd) - 1);
    s_cwd[sizeof(s_cwd) - 1] = 0;
    sendf_tel("OK\r\n");
    return;
  }

  char target[256];
  path_join(target, sizeof(target), s_cwd, path);

  // Probeer echt te chdir'en (als jouw VFS dit ondersteunt)
  if (chdir(target) == 0) {
    // getcwd kan op ESP-IDF soms werken; zo niet, houden we target aan
    char* got = getcwd(NULL, 0);
    if (got) {
      strncpy(s_cwd, got, sizeof(s_cwd) - 1);
      s_cwd[sizeof(s_cwd) - 1] = 0;
      free(got);
    } else {
      strncpy(s_cwd, target, sizeof(s_cwd) - 1);
      s_cwd[sizeof(s_cwd) - 1] = 0;
    }
    sendf_tel("OK\r\n");
    return;
  }

  // Als chdir niet werkt (of niet supported), dan checken we of het een dir is
  struct stat st;
  if (stat(target, &st) == 0 && S_ISDIR(st.st_mode)) {
    strncpy(s_cwd, target, sizeof(s_cwd) - 1);
    s_cwd[sizeof(s_cwd) - 1] = 0;
    sendf_tel("OK (no chdir)\r\n");
    return;
  }

  sendf_tel("ERR: cannot cd to '%s' (errno=%d)\r\n", target, errno);
}

static bool handle_dot_command(char* line)
{
  trim(line);
  if (line[0] != '.') return false;

  char *tok = NULL, *rest = NULL;
  split_first_token(line, &tok, &rest);

  if (!strcmp(tok, ".help") || !strcmp(tok, ".?")) { dot_help(); return true; }
  if (!strcmp(tok, ".quit") || !strcmp(tok, ".exit"))
  {
    send_str_tel("bye\r\n");
    // we close from the receive loop by forcing socket shutdown
    shutdown(s_client_fd, SHUT_RDWR);
    return true;
  }
  if (!strcmp(tok, ".tables")) { dot_tables(); return true; }
  if (!strcmp(tok, ".schema")) { dot_schema((rest && *rest) ? rest : NULL); return true; }

  if (!strcmp(tok, ".headers"))
  {
    if (!rest || !*rest) { sendf_tel("headers %s\r\n", s_opt.headers ? "on" : "off"); return true; }
    if (!strcmp(rest, "on")) s_opt.headers = true;
    else if (!strcmp(rest, "off")) s_opt.headers = false;
    else send_str_tel("Usage: .headers on|off\r\n");
    return true;
  }

  if (!strcmp(tok, ".mode"))
  {
    if (!rest || !*rest)
    {
      const char* m = (s_opt.mode == MODE_LIST) ? "list" : (s_opt.mode == MODE_CSV) ? "csv" : "tabs";
      sendf_tel("mode %s\r\n", m);
      return true;
    }
    if (!strcmp(rest, "list")) s_opt.mode = MODE_LIST;
    else if (!strcmp(rest, "csv"))
    {
      s_opt.mode = MODE_CSV;
      strncpy(s_opt.sep, ",", sizeof(s_opt.sep) - 1);
      s_opt.sep[sizeof(s_opt.sep) - 1] = 0;
    }
    else if (!strcmp(rest, "tabs")) s_opt.mode = MODE_TABS;
    else send_str_tel("Usage: .mode list|csv|tabs\r\n");
    return true;
  }

  if (!strcmp(tok, ".separator"))
  {
    if (!rest || !*rest) { sendf_tel("separator '%s'\r\n", s_opt.sep); return true; }
    strncpy(s_opt.sep, rest, sizeof(s_opt.sep) - 1);
    s_opt.sep[sizeof(s_opt.sep) - 1] = 0;
    return true;
  }

  if (!strcmp(tok, ".nullvalue"))
  {
    if (!rest) rest = "";
    strncpy(s_opt.nullvalue, rest, sizeof(s_opt.nullvalue) - 1);
    s_opt.nullvalue[sizeof(s_opt.nullvalue) - 1] = 0;
    return true;
  }

  if (!strcmp(tok, ".timeout"))
  {
    if (!rest || !*rest) { send_str_tel("Usage: .timeout <ms>\r\n"); return true; }
    int ms = atoi(rest);
    if (ms < 0) ms = 0;
    xSemaphoreTake(s_db_mutex, portMAX_DELAY);
    sqlite3_busy_timeout(s_db, ms);
    xSemaphoreGive(s_db_mutex);
    sendf_tel("timeout %d ms\r\n", ms);
    return true;
  }

  if (!strcmp(tok, ".echo"))
  {
    if (!rest || !*rest) { sendf_tel("echo %s\r\n", s_opt.echo ? "on" : "off"); return true; }
    if (!strcmp(rest, "on")) s_opt.echo = true;
    else if (!strcmp(rest, "off")) s_opt.echo = false;
    else send_str_tel("Usage: .echo on|off\r\n");
    return true;
  }

  if (!strcmp(tok, ".dbinfo")) { dot_dbinfo(); return true; }

  if (!strcmp(tok, ".read")) { dot_read((rest && *rest) ? rest : NULL); return true; }

  if (!strcmp(tok, ".import"))
  {
    if (!rest || !*rest)
    {
      send_str_tel("Usage: .import [--csv] [--skip N] [--separator X] <file> <table>\r\n");
      return true;
    }
    dot_import(rest);
    return true;
  }

  // filesystem commands
  if (!strcmp(tok, ".ls"))  { dot_ls((rest && *rest) ? rest : NULL); return true; }
  if (!strcmp(tok, ".cat")) { dot_cat((rest && *rest) ? rest : NULL); return true; }
  if (!strcmp(tok, ".rm"))  { dot_rm((rest && *rest) ? rest : NULL); return true; }
  if (!strcmp(tok, ".mv"))  { dot_mv((rest && *rest) ? rest : NULL); return true; }
  if (!strcmp(tok, ".pwd")) { dot_pwd(); return true; }
  if (!strcmp(tok, ".cd"))  { dot_cd((rest && *rest) ? rest : NULL); return true; }
  //-?-if (!strcmp(tok, ".df"))  { dot_df(); return true; }
  send_str_tel("Unknown dot-command. Try .help\r\n");
  return true;
}

/* ---------- RX processing: bytes die al "data" zijn (geen IAC) ---------- */
static void process_rx_data_bytes(const uint8_t* data, size_t len)
{
  for (size_t i = 0; i < len; i++)
  {
    unsigned char ch = data[i];
    
    // Telnet Enter komt vaak als CRLF of CRNUL
    if (ch == '\n' || ch == 0) {
      continue;                 // negeer LF en NUL
    }

    if (ch == '\r' || ch == '\n')
    {
      s_sess.line[s_sess.llen] = 0;
      send_str_tel("\r\n");

      trim(s_sess.line);

      if (s_sess.line[0] == '.')
      {
        char tmp[512];
        strncpy(tmp, s_sess.line, sizeof(tmp) - 1);
        tmp[sizeof(tmp) - 1] = 0;
        (void)handle_dot_command(tmp);
      }
      else if (s_sess.line[0] != 0)
      {
        exec_sql_text(s_sess.line);
      }

      s_sess.llen = 0;
      prompt_tel();
      continue;
    }

    if (ch == 0x08 || ch == 0x7F)
    {
      if (s_sess.llen > 0)
      {
        s_sess.llen--;
        if (s_opt.echo) send_str_tel("\b \b");
      }
      continue;
    }

    if (isprint(ch) && s_sess.llen < (int)sizeof(s_sess.line) - 1)
    {
      s_sess.line[s_sess.llen++] = (char)ch;
      if (s_opt.echo)
      {
        char c = (char)ch;
        telnet_send(s_sess.telnet, &c, 1);
      }
    }
  }
}

/* libtelnet geeft TELNET_EV_DATA events; die zijn "user input" */
static void telnet_data_event_handler(telnet_t* telnet, telnet_event_t* ev, void* user_data)
{
  (void)telnet;
  (void)user_data;

  if (ev->type == TELNET_EV_DATA)
  {
    if (ev->data.buffer && ev->data.size > 0)
      process_rx_data_bytes((const uint8_t*)ev->data.buffer, ev->data.size);
  }
  else
  {
    // de rest (SEND/ERROR) gaat via telnet_event_handler, maar libtelnet staat maar 1 handler toe.
    // daarom forwarden we de relevante types hier ook:
    telnet_event_handler(telnet, ev, user_data);
  }
}

/* ---------- connection loop (zoals je oude, maar telnet via libtelnet) ---------- */
static void telnet_task(void* arg)
{
  const int port = (int)(intptr_t)arg;

  struct sockaddr_in addr;
  s_listen_fd = socket(AF_INET, SOCK_STREAM, IPPROTO_IP);
  if (s_listen_fd < 0)
  {
    ESP_LOGE(TAG, "socket failed: %d", errno);
    vTaskDelete(NULL);
    return;
  }

  int yes = 1;
  setsockopt(s_listen_fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));

  memset(&addr, 0, sizeof(addr));
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_ANY);
  addr.sin_port = htons(port);

  if (bind(s_listen_fd, (struct sockaddr*)&addr, sizeof(addr)) != 0)
  {
    ESP_LOGE(TAG, "bind failed: %d", errno);
    close(s_listen_fd);
    s_listen_fd = -1;
    vTaskDelete(NULL);
    return;
  }

  if (listen(s_listen_fd, 1) != 0)
  {
    ESP_LOGE(TAG, "listen failed: %d", errno);
    close(s_listen_fd);
    s_listen_fd = -1;
    vTaskDelete(NULL);
    return;
  }

  ESP_LOGI(TAG, "Telnet SQLite console listening on port %d", port);

  while (1)
  {
    struct sockaddr_in6 src_addr;
    socklen_t socklen = sizeof(src_addr);
    int fd = accept(s_listen_fd, (struct sockaddr*)&src_addr, &socklen);
    if (fd < 0)
    {
      ESP_LOGE(TAG, "accept failed: %d", errno);
      vTaskDelay(pdMS_TO_TICKS(200));
      continue;
    }

    // last client wins
    close_client_locked();
    s_client_fd = fd;

    // telnet init
    s_sess.telnet = telnet_init(s_telopts, telnet_data_event_handler, 0, NULL);
    s_sess.llen = 0;

    send_str_tel("\r\nESP32 SQLite console (telnet)\r\n");
    send_str_tel("Dot commands: .help  | SQL: type statements directly\r\n");
    send_str_tel("Files: .read /spiffs/init.sql  |  .import --csv --skip 1 /spiffs/data.csv mytable\r\n\r\n");
    prompt_tel();

    while (1)
    {
      unsigned char buf[256];
      int r = recv(fd, buf, sizeof(buf), 0);
      if (r <= 0)
      {
        ESP_LOGW(TAG, "recv=%d errno=%d (closing client)", r, errno);
        break;
      }

      // voed raw bytes aan telnet parser; die maakt TELNET_EV_DATA events voor user data
      telnet_recv(s_sess.telnet, (const char*)buf, (unsigned)r);
    }

    if (fd == s_client_fd)
    {
      ESP_LOGI(TAG, "client disconnected/closing");
      close_client_locked();
    }
  }
}

/* ---------- public start ---------- */
esp_err_t telnet_sqlite_console_start(sqlite3* db, SemaphoreHandle_t db_mutex, int port)
{
  if (!db || !db_mutex) return ESP_ERR_INVALID_ARG;
  if (port <= 0) port = 23;

  s_db = db;
  s_db_mutex = db_mutex;

  xTaskCreate(telnet_task, "telnet_sqlite", 16384, (void*)(intptr_t)port, 5, NULL);
  return ESP_OK;
}
