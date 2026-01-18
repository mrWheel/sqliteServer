#include "telnet_sqlite_console.h"

#include <stdio.h>
#include <string.h>
#include <ctype.h>
#include <stdlib.h>
#include <errno.h>
#include <stdarg.h>

#include "esp_log.h"
#include "freertos/FreeRTOS.h"
#include "freertos/task.h"

#include "lwip/sockets.h"
#include "lwip/netdb.h"

static const char* TAG = "TELNETSQL";

static sqlite3* s_db = NULL;
static SemaphoreHandle_t s_db_mutex = NULL;

static int s_listen_fd = -1;
static int s_client_fd = -1;

/* ---------- Console options (sqlite-ish) ---------- */
typedef enum
{
  MODE_LIST = 0,
  MODE_CSV = 1,
  MODE_TABS = 2,
} out_mode_t;

static struct
{
  bool headers;
  bool echo;
  out_mode_t mode;
  char sep[8];        // separator for list/csv
  char nullvalue[16]; // replacement for NULL
} s_opt = {
    .headers = true,
    .echo = true,
    .mode = MODE_LIST,
    .sep = "|",
    .nullvalue = "NULL",
};

static void send_str(int fd, const char* s)
{
  if (!s)
    return;
  send(fd, s, strlen(s), 0);
}

static void sendf(int fd, const char* fmt, ...)
{
  char buf[512];
  va_list ap;
  va_start(ap, fmt);
  vsnprintf(buf, sizeof(buf), fmt, ap);
  va_end(ap);
  send_str(fd, buf);
}

static void close_client_locked(void)
{
  if (s_client_fd >= 0)
  {
    shutdown(s_client_fd, SHUT_RDWR);
    close(s_client_fd);
    s_client_fd = -1;
  }
}

// Telnet constants
#define IAC 255
#define DONT 254
#define DO 253
#define WONT 252
#define WILL 251
#define SB 250
#define SE 240

// Telnet options
#define TELOPT_ECHO 1
#define TELOPT_SGA 3
#define TELOPT_TTYPE 24
#define TELOPT_NAWS 31
#define TELOPT_LINEMODE 34
#define TELOPT_AUTH 37
#define TELOPT_ENCRYPT 38

static void telnet_send_cmd(int fd, unsigned char cmd, unsigned char opt)
{
  unsigned char b[3] = {IAC, cmd, opt};
  (void)send(fd, b, sizeof(b), 0);
}

/****
static bool server_wants_to_will(unsigned char opt) {
    // options we are willing to perform as server
    return (opt == TELOPT_ECHO || opt == TELOPT_SGA);
}
****/

static bool server_wants_client_to_will(unsigned char opt)
{
  // options we want client to perform
  return (opt == TELOPT_NAWS || opt == TELOPT_TTYPE);
}

static void telnet_reply_nego(int fd, unsigned char cmd, unsigned char opt)
{
  // Hard refuse AUTH/ENCRYPT always
  if (opt == TELOPT_AUTH || opt == TELOPT_ENCRYPT)
  {
    // ESP_LOGI(TAG, "telnet: refuse opt=%u cmd=%u", opt, cmd);
    if (cmd == DO)
    {
      telnet_send_cmd(fd, WONT, opt);
      return;
    }
    if (cmd == WILL)
    {
      telnet_send_cmd(fd, DONT, opt);
      return;
    }
    if (cmd == DONT)
    {
      telnet_send_cmd(fd, WONT, opt);
      return;
    }
    if (cmd == WONT)
    {
      telnet_send_cmd(fd, DONT, opt);
      return;
    }
  }

  // Client says server WILL do something: WILL opt
  // If we want client to do it: DO opt else DONT opt
  if (cmd == WILL)
  {
    telnet_send_cmd(fd, server_wants_client_to_will(opt) ? DO : DONT, opt);
    return;
  }

  // Polite acknowledgements
  if (cmd == DONT)
  {
    telnet_send_cmd(fd, WONT, opt);
    return;
  }
  if (cmd == WONT)
  {
    telnet_send_cmd(fd, DONT, opt);
    return;
  }
}

// A more “real telnet server” init for macOS telnet
static void telnet_init(int fd)
{
  // Refuse auth/encrypt up front (macOS telnet otherwise bails)
  telnet_send_cmd(fd, DONT, TELOPT_AUTH);
  telnet_send_cmd(fd, DONT, TELOPT_ENCRYPT);

  // We will echo and suppress go-ahead
  telnet_send_cmd(fd, WILL, TELOPT_ECHO);
  telnet_send_cmd(fd, WILL, TELOPT_SGA);

  // We want client to send NAWS and TTYPE
  telnet_send_cmd(fd, DO, TELOPT_NAWS);
  telnet_send_cmd(fd, DO, TELOPT_TTYPE);

  // We don't do linemode
  telnet_send_cmd(fd, DONT, TELOPT_LINEMODE);
}

static void telnet_drain_negotiation(int fd)
{
  // korte receive timeout zodat we niet blokkeren
  struct timeval tv;
  tv.tv_sec = 0;
  tv.tv_usec = 200 * 1000; // 200ms
  setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

  unsigned char b[256];

  for (int tries = 0; tries < 5; tries++)
  { // max ~1s totaal
    int r = recv(fd, b, sizeof(b), 0);
    if (r <= 0)
      break;

    // Alleen telnet commands verwerken; user-data weggooien (komt pas na de handshake)
    for (int i = 0; i < r; i++)
    {
      if (b[i] != IAC)
        continue;
      if (i + 1 >= r)
        break;
      unsigned char cmd = b[i + 1];

      if (cmd == SB)
      {
        // skip subnegotiation until IAC SE
        i += 2;
        while (i < r)
        {
          if (b[i] == IAC && (i + 1) < r && b[i + 1] == SE)
          {
            i += 1;
            break;
          }
          i++;
        }
        continue;
      }

      if (cmd == DO || cmd == DONT || cmd == WILL || cmd == WONT)
      {
        if (i + 2 >= r)
          break;
        unsigned char opt = b[i + 2];
        telnet_reply_nego(fd, cmd, opt);
        i += 2;
        continue;
      }

      // other 2-byte IAC commands
      i += 1;
    }
  }

  // timeout weer uit (optioneel)
  tv.tv_sec = 0;
  tv.tv_usec = 0;
  setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
}

/* telnet-aware IAC filter + negotiation replies (macOS telnet friendly) */
static int recv_filtered(int fd, unsigned char* out, int outlen)
{
  unsigned char b[256];
  int r = recv(fd, b, sizeof(b), 0);
  if (r <= 0)
    return r;

  // debug (optioneel): alleen loggen als er minimaal 4 bytes zijn
  if (r >= 4)
  {
    ESP_LOGI(TAG, "rx %d bytes, first=%02X %02X %02X %02X",
             r, b[0], b[1], b[2], b[3]);
  }
  else
  {
    ESP_LOGI(TAG, "rx %d bytes, first=%02X", r, b[0]);
  }
  int w = 0;

  for (int i = 0; i < r && w < outlen; i++)
  {
    unsigned char ch = b[i];

    if (ch != IAC)
    {
      out[w++] = ch;
      continue;
    }

    if (i + 1 >= r)
      break;
    unsigned char cmd = b[i + 1];

    // IAC IAC => literal 0xFF
    if (cmd == IAC)
    {
      out[w++] = IAC;
      i += 1;
      continue;
    }

    // IAC SB ... IAC SE
    if (cmd == SB)
    {
      i += 2;
      while (i < r)
      {
        if (b[i] == IAC && (i + 1) < r && b[i + 1] == SE)
        {
          i += 1; // consume SE
          break;
        }
        i++;
      }
      continue;
    }

    // IAC DO/DONT/WILL/WONT <opt>
    if (cmd == DO || cmd == DONT || cmd == WILL || cmd == WONT)
    {
      if (i + 2 >= r)
        break;
      unsigned char opt = b[i + 2];

      telnet_reply_nego(fd, cmd, opt);

      i += 2; // skip cmd+opt
      continue;
    }

    // Other 2-byte IAC commands
    i += 1;
  }

  return w;
}

static void prompt(int fd)
{
  send_str(fd, "sqlite> ");
}

/* ---------- helpers ---------- */
static void trim(char* s)
{
  if (!s)
    return;
  char* p = s;
  while (*p && isspace((unsigned char)*p))
    p++;
  if (p != s)
    memmove(s, p, strlen(p) + 1);

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
  while (**tok && isspace((unsigned char)**tok))
    (*tok)++;
  char* p = *tok;
  while (*p && !isspace((unsigned char)*p))
    p++;
  if (*p)
  {
    *p = 0;
    p++;
  }
  while (*p && isspace((unsigned char)*p))
    p++;
  *rest = p;
}

static void print_value(int fd, int t, sqlite3_stmt* stmt, int col)
{
  if (t == SQLITE_NULL)
  {
    send_str(fd, s_opt.nullvalue);
  }
  else if (t == SQLITE_INTEGER)
  {
    char buf[32];
    snprintf(buf, sizeof(buf), "%lld", (long long)sqlite3_column_int64(stmt, col));
    send_str(fd, buf);
  }
  else if (t == SQLITE_FLOAT)
  {
    char buf[64];
    snprintf(buf, sizeof(buf), "%.17g", sqlite3_column_double(stmt, col));
    send_str(fd, buf);
  }
  else
  {
    const unsigned char* txt = sqlite3_column_text(stmt, col);
    send_str(fd, txt ? (const char*)txt : "");
  }
}

static void print_row_sep(int fd)
{
  if (s_opt.mode == MODE_TABS)
    send_str(fd, "\t");
  else
    send_str(fd, s_opt.sep);
}

/* ---------- SQL execution (multi-statement) ---------- */
static void exec_sql_text(int fd, const char* sql_in)
{
  if (!sql_in || !*sql_in)
    return;

  xSemaphoreTake(s_db_mutex, portMAX_DELAY);

  const char* sql = sql_in;
  sqlite3_stmt* stmt = NULL;

  while (1)
  {
    int rc = sqlite3_prepare_v2(s_db, sql, -1, &stmt, &sql);
    if (rc != SQLITE_OK)
    {
      sendf(fd, "ERR: %s\r\n", sqlite3_errmsg(s_db));
      if (stmt)
        sqlite3_finalize(stmt);
      break;
    }
    if (!stmt)
      break; // done

    int cols = sqlite3_column_count(stmt);
    if (cols > 0)
    {
      if (s_opt.headers)
      {
        for (int c = 0; c < cols; c++)
        {
          if (c)
            print_row_sep(fd);
          send_str(fd, sqlite3_column_name(stmt, c));
        }
        send_str(fd, "\r\n");
      }

      int s;
      while ((s = sqlite3_step(stmt)) == SQLITE_ROW)
      {
        for (int c = 0; c < cols; c++)
        {
          if (c)
            print_row_sep(fd);
          int t = sqlite3_column_type(stmt, c);
          print_value(fd, t, stmt, c);
        }
        send_str(fd, "\r\n");
      }
      if (s != SQLITE_DONE)
      {
        sendf(fd, "ERR: %s\r\n", sqlite3_errmsg(s_db));
        sqlite3_finalize(stmt);
        break;
      }
    }
    else
    {
      int s = sqlite3_step(stmt);
      if (s != SQLITE_DONE)
      {
        sendf(fd, "ERR: %s\r\n", sqlite3_errmsg(s_db));
        sqlite3_finalize(stmt);
        break;
      }
      sendf(fd, "OK (changes=%d last_id=%lld)\r\n",
            sqlite3_changes(s_db),
            (long long)sqlite3_last_insert_rowid(s_db));
    }

    sqlite3_finalize(stmt);
    stmt = NULL;
  }

  xSemaphoreGive(s_db_mutex);
}

/* ---------- .read implementation ---------- */
#define READ_MAX_BYTES (256 * 1024)

static void dot_read(int fd, const char* path)
{
  if (!path || !*path)
  {
    send_str(fd, "Usage: .read /spiffs/init.sql\r\n");
    return;
  }

  FILE* f = fopen(path, "rb");
  if (!f)
  {
    sendf(fd, "ERR: cannot open %s\r\n", path);
    return;
  }

  if (fseek(f, 0, SEEK_END) != 0)
  {
    fclose(f);
    send_str(fd, "ERR: fseek failed\r\n");
    return;
  }
  long sz = ftell(f);
  if (sz < 0)
  {
    fclose(f);
    send_str(fd, "ERR: ftell failed\r\n");
    return;
  }
  if (sz > READ_MAX_BYTES)
  {
    fclose(f);
    sendf(fd, "ERR: file too large (%ld bytes, max %d)\r\n", sz, (int)READ_MAX_BYTES);
    return;
  }
  rewind(f);

  char* buf = (char*)malloc((size_t)sz + 1);
  if (!buf)
  {
    fclose(f);
    send_str(fd, "ERR: OOM\r\n");
    return;
  }

  size_t n = fread(buf, 1, (size_t)sz, f);
  fclose(f);

  buf[n] = 0;

  sendf(fd, "-- .read %s (%u bytes)\r\n", path, (unsigned)n);
  exec_sql_text(fd, buf);
  free(buf);
}

/* ---------- .import implementation ---------- */

static void sqlite_exec_simple(int fd, const char* sql)
{
  char* err = NULL;
  int rc = sqlite3_exec(s_db, sql, NULL, NULL, &err);
  if (rc != SQLITE_OK)
  {
    sendf(fd, "ERR: %s\r\n", err ? err : sqlite3_errmsg(s_db));
    if (err)
      sqlite3_free(err);
  }
}

/* Basic CSV parser:
   - comma separated
   - quoted fields with "..."
   - escaped quote inside quotes: ""
   Returns number of fields parsed into out[] (pointers into buf).
   Modifies buf in-place by inserting NULs.
*/
static int parse_csv_inplace(char* buf, char** out, int max_fields)
{
  int n = 0;
  char* p = buf;

  while (*p && n < max_fields)
  {
    // skip leading spaces (optional)
    // while (*p == ' ') p++;

    char* start = p;
    if (*p == '"')
    {
      // quoted
      p++; // skip first quote
      start = p;
      char* w = p;
      while (*p)
      {
        if (*p == '"' && p[1] == '"')
        {
          // escaped quote -> write one quote
          *w++ = '"';
          p += 2;
          continue;
        }
        if (*p == '"')
        {
          // end quote
          p++;
          break;
        }
        *w++ = *p++;
      }
      *w = 0;

      // consume until comma or end
      while (*p && *p != ',')
        p++;
      if (*p == ',')
      {
        *p = 0;
        p++;
      }
      out[n++] = start;
    }
    else
    {
      // unquoted until comma
      while (*p && *p != ',')
        p++;
      if (*p == ',')
      {
        *p = 0;
        p++;
      }
      // trim trailing spaces
      char* e = start + strlen(start);
      while (e > start && (e[-1] == '\r' || e[-1] == '\n'))
        e--;
      while (e > start && isspace((unsigned char)e[-1]))
        e--;
      *e = 0;
      out[n++] = start;
    }
  }

  // trim CRLF on last field if not handled
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

  // strip CRLF
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
    if (!q)
      break;
    *q = 0;
    p = q + 1;
  }
  return n;
}

static bool is_valid_identifier_like(const char* s)
{
  // minimal check to reduce injection risk in table name
  // allows letters, digits, underscore, and dot
  if (!s || !*s)
    return false;
  for (const char* p = s; *p; p++)
  {
    if (!(isalnum((unsigned char)*p) || *p == '_' || *p == '.'))
      return false;
  }
  return true;
}

static void dot_import(int fd, char* args)
{
  // Syntax:
  // .import [--csv] [--tabs] [--separator X] [--skip N] <file> <table>
  bool csv = false;
  char sep = (s_opt.mode == MODE_TABS) ? '\t' : s_opt.sep[0];
  int skip = 0;

  // tokenise args (space separated)
  char* tokv[16];
  int tokc = 0;
  char* p = args;
  while (p && *p)
  {
    while (*p && isspace((unsigned char)*p))
      p++;
    if (!*p)
      break;
    tokv[tokc++] = p;
    if (tokc >= (int)(sizeof(tokv) / sizeof(tokv[0])))
      break;
    while (*p && !isspace((unsigned char)*p))
      p++;
    if (*p)
      *p++ = 0;
  }

  // parse options
  int i = 0;
  for (; i < tokc; i++)
  {
    if (strcmp(tokv[i], "--csv") == 0)
    {
      csv = true;
      continue;
    }
    if (strcmp(tokv[i], "--tabs") == 0)
    {
      csv = false;
      sep = '\t';
      continue;
    }
    if (strcmp(tokv[i], "--separator") == 0)
    {
      if (i + 1 >= tokc)
      {
        send_str(fd, "Usage: .import --separator X <file> <table>\r\n");
        return;
      }
      sep = tokv[++i][0];
      csv = false;
      continue;
    }
    if (strcmp(tokv[i], "--skip") == 0)
    {
      if (i + 1 >= tokc)
      {
        send_str(fd, "Usage: .import --skip N <file> <table>\r\n");
        return;
      }
      skip = atoi(tokv[++i]);
      if (skip < 0)
        skip = 0;
      continue;
    }
    // first non-option token => file
    break;
  }

  if (tokc - i < 2)
  {
    send_str(fd, "Usage: .import [--csv] [--skip N] [--separator X] <file> <table>\r\n");
    return;
  }

  const char* file = tokv[i];
  const char* table = tokv[i + 1];

  if (!is_valid_identifier_like(table))
  {
    send_str(fd, "ERR: invalid table name (allowed: a-z A-Z 0-9 _ .)\r\n");
    return;
  }

  FILE* f = fopen(file, "rb");
  if (!f)
  {
    sendf(fd, "ERR: cannot open %s\r\n", file);
    return;
  }

  // skip N lines
  char linebuf[1024];
  for (int s = 0; s < skip; s++)
  {
    if (!fgets(linebuf, sizeof(linebuf), f))
      break;
  }

  // read first data line to determine column count
  if (!fgets(linebuf, sizeof(linebuf), f))
  {
    fclose(f);
    send_str(fd, "ERR: empty file (after skip)\r\n");
    return;
  }

  // Make a working copy we can modify
  char work[1024];
  strncpy(work, linebuf, sizeof(work) - 1);
  work[sizeof(work) - 1] = 0;

  char* fields[64];
  int nfields = 0;
  if (csv)
  {
    nfields = parse_csv_inplace(work, fields, 64);
  }
  else
  {
    nfields = split_sep_inplace(work, fields, 64, sep);
  }

  if (nfields <= 0)
  {
    fclose(f);
    send_str(fd, "ERR: could not parse first row\r\n");
    return;
  }

  // prepare INSERT statement
  char sql[256];
  // "INSERT INTO table VALUES(?,?,?)"
  strcpy(sql, "INSERT INTO ");
  strncat(sql, table, sizeof(sql) - strlen(sql) - 1);
  strncat(sql, " VALUES(", sizeof(sql) - strlen(sql) - 1);
  for (int c = 0; c < nfields; c++)
  {
    strncat(sql, "?", sizeof(sql) - strlen(sql) - 1);
    if (c != nfields - 1)
      strncat(sql, ",", sizeof(sql) - strlen(sql) - 1);
  }
  strncat(sql, ");", sizeof(sql) - strlen(sql) - 1);

  xSemaphoreTake(s_db_mutex, portMAX_DELAY);

  sqlite_exec_simple(fd, "BEGIN;");
  sqlite3_stmt* stmt = NULL;
  int prc = sqlite3_prepare_v2(s_db, sql, -1, &stmt, NULL);
  if (prc != SQLITE_OK || !stmt)
  {
    sendf(fd, "ERR: prepare failed: %s\r\n", sqlite3_errmsg(s_db));
    sqlite_exec_simple(fd, "ROLLBACK;");
    xSemaphoreGive(s_db_mutex);
    fclose(f);
    return;
  }

  long rows = 0;
  bool had_error = false;

  // Import first data row already read (linebuf)
  {
    char w2[1024];
    strncpy(w2, linebuf, sizeof(w2) - 1);
    w2[sizeof(w2) - 1] = 0;

    char* f2[64];
    int nf2 = csv ? parse_csv_inplace(w2, f2, 64) : split_sep_inplace(w2, f2, 64, sep);
    if (nf2 != nfields)
    {
      sendf(fd, "WARN: column count mismatch (got %d expected %d), skipping row\r\n", nf2, nfields);
    }
    else
    {
      sqlite3_reset(stmt);
      sqlite3_clear_bindings(stmt);

      for (int c = 0; c < nfields; c++)
      {
        const char* val = f2[c];
        sqlite3_bind_text(stmt, c + 1, val ? val : "", -1, SQLITE_TRANSIENT);
      }

      int s = sqlite3_step(stmt);
      if (s != SQLITE_DONE)
      {
        sendf(fd, "ERR: step failed: %s\r\n", sqlite3_errmsg(s_db));
        had_error = true;
      }
      else
      {
        rows++;
      }
    }
  }

  // Rest of file
  while (!had_error && fgets(linebuf, sizeof(linebuf), f))
  {
    char w2[1024];
    strncpy(w2, linebuf, sizeof(w2) - 1);
    w2[sizeof(w2) - 1] = 0;

    char* f2[64];
    int nf2 = csv ? parse_csv_inplace(w2, f2, 64) : split_sep_inplace(w2, f2, 64, sep);
    if (nf2 != nfields)
    {
      sendf(fd, "WARN: column count mismatch (got %d expected %d), skipping row\r\n", nf2, nfields);
      continue;
    }

    sqlite3_reset(stmt);
    sqlite3_clear_bindings(stmt);

    for (int c = 0; c < nfields; c++)
    {
      const char* val = f2[c];
      sqlite3_bind_text(stmt, c + 1, val ? val : "", -1, SQLITE_TRANSIENT);
    }

    int s = sqlite3_step(stmt);
    if (s != SQLITE_DONE)
    {
      sendf(fd, "ERR: step failed: %s\r\n", sqlite3_errmsg(s_db));
      had_error = true;
    }
    else
    {
      rows++;
    }

    if ((rows % 500) == 0)
      vTaskDelay(pdMS_TO_TICKS(1));
  }

  sqlite3_finalize(stmt);

  if (!had_error)
    sqlite_exec_simple(fd, "COMMIT;");
  else
    sqlite_exec_simple(fd, "ROLLBACK;");

  xSemaphoreGive(s_db_mutex);

  fclose(f);

  if (!had_error)
  {
    sendf(fd, "Imported %ld rows into %s\r\n", rows, table);
  }
  else
  {
    send_str(fd, "Import failed (rolled back)\r\n");
  }
}

/* ---------- dot commands ---------- */
static void dot_help(int fd)
{
  send_str(fd,
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
           "Notes:\r\n"
           "  - .read reads up to 256KB per file.\r\n"
           "  - .import uses VALUES(...) and binds all fields as TEXT.\r\n"
           "  - Use --skip 1 for CSV header lines.\r\n");
}

static void dot_tables(int fd)
{
  exec_sql_text(fd, "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;");
}

static void dot_schema(int fd, const char* table)
{
  if (!table || !*table)
  {
    exec_sql_text(fd, "SELECT sql FROM sqlite_master WHERE sql IS NOT NULL ORDER BY name;");
    return;
  }
  char* q = sqlite3_mprintf("SELECT sql FROM sqlite_master WHERE sql IS NOT NULL AND name=%Q;", table);
  if (q)
  {
    exec_sql_text(fd, q);
    sqlite3_free(q);
  }
}

static void dot_dbinfo(int fd)
{
  sendf(fd, "SQLite version: %s\r\n", sqlite3_libversion());
  xSemaphoreTake(s_db_mutex, portMAX_DELAY);
  sendf(fd, "changes=%d last_insert_rowid=%lld\r\n",
        sqlite3_changes(s_db),
        (long long)sqlite3_last_insert_rowid(s_db));
  xSemaphoreGive(s_db_mutex);
}

static bool handle_dot_command(int fd, char* line)
{
  trim(line);
  if (line[0] != '.')
    return false;

  char *tok = NULL, *rest = NULL;
  split_first_token(line, &tok, &rest);

  if (!strcmp(tok, ".help") || !strcmp(tok, ".?"))
  {
    dot_help(fd);
    return true;
  }
  if (!strcmp(tok, ".quit") || !strcmp(tok, ".exit"))
  {
    send_str(fd, "bye\r\n");
    shutdown(fd, SHUT_RDWR);
    return true;
  }
  if (!strcmp(tok, ".tables"))
  {
    dot_tables(fd);
    return true;
  }
  if (!strcmp(tok, ".schema"))
  {
    dot_schema(fd, (rest && *rest) ? rest : NULL);
    return true;
  }

  if (!strcmp(tok, ".headers"))
  {
    if (!rest || !*rest)
    {
      sendf(fd, "headers %s\r\n", s_opt.headers ? "on" : "off");
      return true;
    }
    if (!strcmp(rest, "on"))
      s_opt.headers = true;
    else if (!strcmp(rest, "off"))
      s_opt.headers = false;
    else
      send_str(fd, "Usage: .headers on|off\r\n");
    return true;
  }

  if (!strcmp(tok, ".mode"))
  {
    if (!rest || !*rest)
    {
      const char* m = (s_opt.mode == MODE_LIST) ? "list" : (s_opt.mode == MODE_CSV) ? "csv"
                                                                                    : "tabs";
      sendf(fd, "mode %s\r\n", m);
      return true;
    }
    if (!strcmp(rest, "list"))
      s_opt.mode = MODE_LIST;
    else if (!strcmp(rest, "csv"))
    {
      s_opt.mode = MODE_CSV;
      strncpy(s_opt.sep, ",", sizeof(s_opt.sep) - 1);
      s_opt.sep[sizeof(s_opt.sep) - 1] = 0;
    }
    else if (!strcmp(rest, "tabs"))
      s_opt.mode = MODE_TABS;
    else
      send_str(fd, "Usage: .mode list|csv|tabs\r\n");
    return true;
  }

  if (!strcmp(tok, ".separator"))
  {
    if (!rest || !*rest)
    {
      sendf(fd, "separator '%s'\r\n", s_opt.sep);
      return true;
    }
    strncpy(s_opt.sep, rest, sizeof(s_opt.sep) - 1);
    s_opt.sep[sizeof(s_opt.sep) - 1] = 0;
    return true;
  }

  if (!strcmp(tok, ".nullvalue"))
  {
    if (!rest)
      rest = "";
    strncpy(s_opt.nullvalue, rest, sizeof(s_opt.nullvalue) - 1);
    s_opt.nullvalue[sizeof(s_opt.nullvalue) - 1] = 0;
    return true;
  }

  if (!strcmp(tok, ".timeout"))
  {
    if (!rest || !*rest)
    {
      send_str(fd, "Usage: .timeout <ms>\r\n");
      return true;
    }
    int ms = atoi(rest);
    if (ms < 0)
      ms = 0;
    xSemaphoreTake(s_db_mutex, portMAX_DELAY);
    sqlite3_busy_timeout(s_db, ms);
    xSemaphoreGive(s_db_mutex);
    sendf(fd, "timeout %d ms\r\n", ms);
    return true;
  }

  if (!strcmp(tok, ".echo"))
  {
    if (!rest || !*rest)
    {
      sendf(fd, "echo %s\r\n", s_opt.echo ? "on" : "off");
      return true;
    }
    if (!strcmp(rest, "on"))
      s_opt.echo = true;
    else if (!strcmp(rest, "off"))
      s_opt.echo = false;
    else
      send_str(fd, "Usage: .echo on|off\r\n");
    return true;
  }

  if (!strcmp(tok, ".dbinfo"))
  {
    dot_dbinfo(fd);
    return true;
  }

  if (!strcmp(tok, ".read"))
  {
    dot_read(fd, (rest && *rest) ? rest : NULL);
    return true;
  }

  if (!strcmp(tok, ".import"))
  {
    if (!rest || !*rest)
    {
      send_str(fd, "Usage: .import [--csv] [--skip N] [--separator X] <file> <table>\r\n");
      return true;
    }
    // dot_import modifies args in-place; safe to pass rest
    dot_import(fd, rest);
    return true;
  }

  send_str(fd, "Unknown dot-command. Try .help\r\n");
  return true;
}

/* ---------- connection loop ---------- */
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

    // last client wins (sluit oude client, niet de nieuwe)
    close_client_locked();
    s_client_fd = fd;

    telnet_init(fd);
    telnet_drain_negotiation(fd);

    send_str(fd, "\r\nESP32 SQLite console (telnet)\r\n");
    send_str(fd, "Dot commands: .help  | SQL: type statements directly\r\n");
    send_str(fd, "Files: .read /spiffs/init.sql  |  .import --csv --skip 1 /spiffs/data.csv mytable\r\n\r\n");
    prompt(fd);
    char line[512];
    int llen = 0;

    while (1)
    {
      unsigned char buf[128];
      int r = recv_filtered(fd, buf, sizeof(buf));
      if (r <= 0)
      {
        ESP_LOGW(TAG, "recv_filtered=%d errno=%d (closing client)", r, errno);
        break;
      }
      for (int i = 0; i < r; i++)
      {
        unsigned char ch = buf[i];

        // Treat CR or LF as "Enter" (telnet often sends CR)
        if (ch == '\r' || ch == '\n')
        {
          line[llen] = 0;
          send_str(fd, "\r\n");

          trim(line);

          if (line[0] == '.')
          {
            char tmp[512];
            strncpy(tmp, line, sizeof(tmp) - 1);
            tmp[sizeof(tmp) - 1] = 0;
            (void)handle_dot_command(fd, tmp);

            if (fd != s_client_fd || s_client_fd < 0)
              goto client_done;
          }
          else if (line[0] != 0)
          {
            exec_sql_text(fd, line);
          }

          llen = 0;
          if (fd == s_client_fd)
            prompt(fd);
          continue;
        }
        // backspace
        if (ch == 0x08 || ch == 0x7F)
        {
          if (llen > 0)
          {
            llen--;
            if (s_opt.echo)
              send_str(fd, "\b \b");
          }
          continue;
        }

        if (isprint(ch) && llen < (int)sizeof(line) - 1)
        {
          line[llen++] = (char)ch;
          if (s_opt.echo)
            send(fd, &ch, 1, 0);
        }
      }
    }

  client_done:
    if (fd == s_client_fd)
    {
      shutdown(fd, SHUT_RDWR);
      ESP_LOGI(TAG, "client disconnected/closing");
      close(fd);
      s_client_fd = -1;
    }
  }
}

esp_err_t telnet_sqlite_console_start(sqlite3* db, SemaphoreHandle_t db_mutex, int port)
{
  if (!db || !db_mutex)
    return ESP_ERR_INVALID_ARG;
  if (port <= 0)
    port = 23;

  s_db = db;
  s_db_mutex = db_mutex;

  xTaskCreate(telnet_task, "telnet_sqlite", 16384, (void*)(intptr_t)port, 5, NULL);
  return ESP_OK;
}
