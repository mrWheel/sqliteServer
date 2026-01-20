# ESP32 SQLite TCP Protocol (NDJSON v1)

## Transport
- TCP
- 1 request per regel JSON (newline-delimited JSON / NDJSON)
- Server antwoordt ook met 1 JSON object per regel
- UTF-8
- Handle/stmt IDs zijn geldig binnen dezelfde TCP verbinding (session scoped)

## Conventies
- Elk request heeft: { "op": "<name>", ... }
- Responses:
  - Succes: { "ok": true, ... }
  - Fout:   { "ok": false, "error": { "code": <int>, "message": "<text>" } }

## Operaties

### 1) exec (voor INSERT/UPDATE/DDL of simpele queries zonder row-by-row)
Request:
{
  "op": "exec",
  "sql": "CREATE TABLE ...; INSERT ...; UPDATE ...;"
}

Response (succes):
{
  "ok": true,
  "changes": <int>,          // sqlite3_changes()
  "total_changes": <int>,    // sqlite3_total_changes()
  "last_insert_rowid": <int> // sqlite3_last_insert_rowid()
}

Let op:
- exec gebruikt sqlite3_exec() (handig voor statements zonder results)
- Voor SELECT gebruik je liever prepare/step/finalize

### 2) prepare (maak een statement-handle)
Request:
{
  "op": "prepare",
  "sql": "SELECT id,name FROM mytable WHERE id > ? ORDER BY id LIMIT ?"
}

Response (succes):
{
  "ok": true,
  "stmt": <int>,             // handle
  "cols": <int>,
  "col_names": ["id","name"]
}

### 3) bind (bind parameters aan een prepared statement)
Je kunt meerdere binds doen, of alles in 1 request.

Request (bind 1 parameter):
{
  "op": "bind",
  "stmt": 3,
  "index": 1,                // 1-based zoals SQLite
  "type": "int",             // "null" | "int" | "double" | "text" | "blob_b64"
  "value": 42                // bij text: string, bij blob_b64: base64 string
}

Response:
{ "ok": true }

### 4) step (haal de volgende row op)
Request:
{
  "op": "step",
  "stmt": 3
}

Response wanneer er een row is:
{
  "ok": true,
  "row": ["123","Alice"],    // alles als string of null
  "types": ["int","text"]    // optioneel maar handig
}

Response wanneer klaar:
{
  "ok": true,
  "done": true
}

### 5) reset (hergebruik statement met nieuwe binds)
Request:
{
  "op": "reset",
  "stmt": 3,
  "clear_binds": true
}

Response:
{ "ok": true }

### 6) finalize (ruim statement op)
Request:
{
  "op": "finalize",
  "stmt": 3
}

Response:
{ "ok": true }

### 7) pragma (handig voor dot-achtige commando's)
Request:
{ "op": "pragma", "sql": "PRAGMA table_info(mytable)" }

Response (succes):
{
  "ok": true,
  "cols": <int>,
  "col_names": [...],
  "rows": [ [...], [...], ... ]
}

Let op: pragma geeft alles in 1 response (geen cursor). Gebruik prepare/step als je streaming wilt.

### 8) ping
Request:
{ "op": "ping" }
Response:
{ "ok": true, "pong": true }

## Foutcodes
- 400: bad request (JSON ontbreekt/velden fout)
- 404: stmt handle onbekend
- 409: stmt slot vol (teveel open statements)
- 500: sqlite error (met sqlite rc in message)
- 501: unknown op

## Praktische limieten
- Max request line length: 2048 bytes (configureerbaar)
- Max open statements per verbinding: 8 (configureerbaar)