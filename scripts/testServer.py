#!/usr/bin/env python3
import argparse
import json
import socket
import sys
import time
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Tuple

PROTO = "sqlite-tcp-v1"

SQL_PERSONEN = (
    "SELECT id,voornaam,achternaam,geboortedatum,woonplaats,opleiding,functie,ervaring_sinds "
    "FROM persoon ORDER BY achternaam,voornaam"
)

# ---------- logging ----------
LOG_ERROR = 0
LOG_INFO = 1
LOG_DEBUG = 2

LOG_LEVEL = LOG_INFO

def ts() -> str:
    return time.strftime("%H:%M:%S")

def log_error(msg: str) -> None:
    if LOG_LEVEL >= LOG_ERROR:
        print(f"[{ts()}] ERROR {msg}", file=sys.stderr, flush=True)

def log_info(msg: str) -> None:
    if LOG_LEVEL >= LOG_INFO:
        print(f"[{ts()}] INFO  {msg}", file=sys.stderr, flush=True)

def log_debug(msg: str) -> None:
    if LOG_LEVEL >= LOG_DEBUG:
        print(f"[{ts()}] DEBUG {msg}", file=sys.stderr, flush=True)

def jshort(obj: Any, limit: int = 260) -> str:
    s = json.dumps(obj, ensure_ascii=False)
    return s if len(s) <= limit else (s[:limit] + "...")

# ---------- protocol helpers ----------
def json_send(sock: socket.socket, obj: Dict[str, Any]) -> None:
    line = (json.dumps(obj, ensure_ascii=False) + "\n").encode("utf-8")
    log_debug(f"TX {len(line)}b: {line.decode('utf-8', errors='ignore').rstrip()}")
    sock.sendall(line)

def json_recv_line(f, timeout_sec: float, sock: socket.socket) -> Dict[str, Any]:
    sock.settimeout(timeout_sec)
    try:
        line = f.readline()
    except socket.timeout:
        raise TimeoutError(f"Timeout waiting for server response after {timeout_sec}s")
    finally:
        sock.settimeout(None)

    if not line:
        raise ConnectionError("Server closed connection (EOF)")

    txt = line.decode("utf-8", errors="strict").rstrip("\n")
    log_debug(f"RX {len(line)}b: {txt}")
    return json.loads(txt)

def expect_ok(resp: Dict[str, Any]) -> Dict[str, Any]:
    if not resp.get("ok", False):
        raise RuntimeError(f"Server error: {resp}")
    return resp

def prepare(sock: socket.socket, f, sql: str, timeout: float = 5.0) -> Tuple[int, List[str]]:
    log_debug(f"PREPARE: {sql}")
    json_send(sock, {"op": "prepare", "sql": sql})
    resp = expect_ok(json_recv_line(f, timeout, sock))
    stmt = int(resp["stmt"])
    cols = list(resp.get("col_names", []))
    log_debug(f"PREPARED stmt={stmt} cols={resp.get('cols')} names={cols}")
    return stmt, cols

def step(sock: socket.socket, f, stmt: int, timeout: float = 5.0) -> Optional[List[Any]]:
    json_send(sock, {"op": "step", "stmt": stmt})
    resp = expect_ok(json_recv_line(f, timeout, sock))
    if resp.get("done"):
        log_debug(f"STEP stmt={stmt}: done")
        return None
    row = resp.get("row")
    log_debug(f"STEP stmt={stmt}: row={jshort(row)}")
    return row

def finalize(sock: socket.socket, f, stmt: int, timeout: float = 5.0) -> None:
    log_debug(f"FINALIZE stmt={stmt}")
    json_send(sock, {"op": "finalize", "stmt": stmt})
    expect_ok(json_recv_line(f, timeout, sock))
    log_debug(f"FINALIZED stmt={stmt}")

def sql_quote(s: str) -> str:
    return "'" + s.replace("'", "''") + "'"

def fetch_single_text(sock: socket.socket, f, sql: str, timeout: float = 5.0) -> Optional[str]:
    log_debug(f"FETCH_ONE: {sql}")
    stmt, _ = prepare(sock, f, sql, timeout=timeout)
    try:
        row = step(sock, f, stmt, timeout=timeout)
        if row is None or len(row) == 0:
            log_debug("FETCH_ONE: no row")
            return None
        v = row[0]
        log_debug(f"FETCH_ONE: value={v!r}")
        return None if v is None else str(v)
    finally:
        finalize(sock, f, stmt, timeout=timeout)

# ---------- pretty output (per person) ----------
def print_person_card(person: Dict[str, str]) -> None:
    items = [
        ("ID", person.get("id", "")),
        ("Naam", f'{person.get("achternaam","")}, {person.get("voornaam","")}'.strip() or person.get("naam","")),
        ("Geboorte", f'{person.get("geboorte_fmt","")} (leeftijd {person.get("leeftijd","")})'.strip()),
        ("Woonplaats", person.get("woonplaats", "")),
        ("Opleiding", person.get("opleiding", "")),
        ("Functie", person.get("functie", "")),
        ("Categorie", person.get("categorie", "")),
        ("Ervaring", f'{person.get("ervaring_jaren","")} jaar'.strip()),
    ]
    width = max(len(k) for k, _ in items)
    print("+" + "-" * (width + 2) + "+" + "-" * 62 + "+")
    for k, v in items:
        left = f" {k.ljust(width)} "
        right = f" {v}"
        if len(right) > 62:
            right = right[:59] + "..."
        print("|" + left + "|" + right.ljust(62) + "|")
    print("+" + "-" * (width + 2) + "+" + "-" * 62 + "+")
    print("", flush=True)


def clip(s: str, width: int) -> str:
    s = "" if s is None else str(s)
    if width <= 1:
        return s[:width]
    return s if len(s) <= width else (s[: max(0, width - 3)] + "...")

def print_person_list_table(rows: List[Dict[str, str]]) -> None:
    # Kolommen + max breedtes (pas gerust aan)
    cols = [
      # ("id", "id", 4),
        ("naam", "naam", 24),
        ("geboortedatum", "geboortedatum", 10),
        ("woonplaats", "woonplaats", 14),
        ("opleiding", "opleiding", 9),
        ("functie", "functie", 28),
        ("categorie", "categorie", 22),
        ("ervaring_sinds", "ervaring_sinds", 10),
    ]

    # widths: minimaal header breedte, maximaal opgegeven max
    widths = []
    for key, header, maxw in cols:
        widths.append(min(maxw, max(len(header), maxw)))

    def fmt_row(values: List[str]) -> str:
        out = []
        for i, v in enumerate(values):
            out.append(clip(v, widths[i]).ljust(widths[i]))
        return "| " + " | ".join(out) + " |"

    header_vals = [h for _, h, _ in cols]
    print(fmt_row(header_vals))
    print("|-" + "-|-".join("-" * w for w in widths) + "-|")

    for r in rows:
        vals = []
        for key, _, _ in cols:
            vals.append(r.get(key, ""))
        print(fmt_row(vals))

def main() -> None:
    global LOG_LEVEL

    ap = argparse.ArgumentParser(description="Read persons from sqlite-tcp-v1 server and print persons.")
    ap.add_argument("--host", default="192.168.12.14", help="Server IP/hostname")
    ap.add_argument("--port", type=int, default=5555, help="Server port")
    ap.add_argument("--log-level", choices=["error", "info", "debug"], default="info", help="Logging verbosity")
    ap.add_argument("--timeout", type=float, default=5.0, help="Per request timeout seconds")
    ap.add_argument("--limit", type=int, default=0, help="Optional limit number of persons (0 = all)")
    ap.add_argument("--wide", type=int, default=80, help="Table width in columns (default: 80)")
    args = ap.parse_args()

    LOG_LEVEL = {"error": LOG_ERROR, "info": LOG_INFO, "debug": LOG_DEBUG}[args.log_level]

    table_width = args.wide 
    host, port, timeout = args.host, args.port, args.timeout

    # -------- date helpers --------
    def parse_ymd(s: str) -> Optional[date]:
        if not s:
            return None
        try:
            return datetime.strptime(str(s), "%Y-%m-%d").date()
        except ValueError:
            return None

    def fmt_ddmmyyyy(d: Optional[date]) -> str:
        return d.strftime("%d-%m-%Y") if d else ""

    def years_between(d: Optional[date], today: date) -> str:
        if not d:
            return ""
        y = today.year - d.year - ((today.month, today.day) < (d.month, d.day))
        return str(y)

    # -------- output helpers (streaming, fixed widths) --------
    def clip(s: str, width: int) -> str:
        s = "" if s is None else str(s)
        if len(s) <= width:
            return s
        if width <= 3:
            return s[:width]
        return s[: width - 3] + "..."

    if table_width >= 100:
        COLS = [
            ("naam", "naam", 28, "l"),
            ("geboorte_fmt", "geboorte", 10, "l"),
            ("leeftijd", "leeftijd", 8, "r"),
            ("woonplaats", "woonplaats", 16, "l"),
            ("opleiding", "opl", 10, "c"),
            ("functie", "functie", 36, "l"),
            ("categorie", "categorie", 28, "l"),
            ("ervaring_jaren", "ervaring", 8, "r"),
        ]
    else:
        COLS = [
            ("naam", "naam", 24, "l"),
            ("geboorte_fmt", "geboorte", 10, "l"),
            ("leeftijd", "leeftijd", 8, "r"),
            ("woonplaats", "woonplaats", 14, "l"),
            ("opleiding", "opl", 5, "c"),
            ("functie", "functie", 28, "l"),
            ("categorie", "categorie", 22, "l"),
            ("ervaring_jaren", "ervaring", 8, "r"),
        ]
    WIDTHS = [w for _, _, w, _ in COLS]
    ALIGNS = [a for _, _, _, a in COLS]

    def fmt_line(values: List[str]) -> str:
        parts = []
        for i, v in enumerate(values):
            txt = clip(v, WIDTHS[i])
            a = ALIGNS[i]
            if a == "r":
                parts.append(txt.rjust(WIDTHS[i]))
            elif a == "c":
                parts.append(txt.center(WIDTHS[i]))
            else:
                parts.append(txt.ljust(WIDTHS[i]))
        return "| " + " | ".join(parts) + " |"


    def print_header_once() -> None:
        headers = [h for _, h, _, _ in COLS]
        header_parts = []
        for i, h in enumerate(headers):
            w = WIDTHS[i]
            a = ALIGNS[i]
            if a == "c":
                header_parts.append(h.center(w))
            else:
                header_parts.append(h.ljust(w))
        print("| " + " | ".join(header_parts) + " |")
        print("|-" + "-|-".join("-" * w for w in WIDTHS) + "-|")
        sys.stdout.flush()

    def print_row(person: Dict[str, str]) -> None:
        values = [person.get(k, "") for k, _, _, _ in COLS]
        print(fmt_line(values))
        sys.stdout.flush()

    # -------- connect & query --------
    log_info(f"Connecting to {host}:{port} ...")
    t_start = time.perf_counter()

    with socket.create_connection((host, port), timeout=5) as sock:
        f = sock.makefile("rb")

        hello = json_recv_line(f, timeout_sec=timeout, sock=sock)
        expect_ok(hello)
        if hello.get("hello") != PROTO:
            raise RuntimeError(f"Unexpected protocol: {hello.get('hello')}")

        json_send(sock, {"op": "ping"})
        expect_ok(json_recv_line(f, timeout_sec=timeout, sock=sock))

        stmt_p, _ = prepare(sock, f, SQL_PERSONEN, timeout=timeout)

        count = 0
        header_printed = False
        today = date.today()

        try:
            while True:
                prow = step(sock, f, stmt_p, timeout=timeout)
                if prow is None:
                    break

                if len(prow) < 8:
                    log_error(f"Row has {len(prow)} cols, expected 8. raw={jshort(prow)}")
                    continue

                count += 1
                pid, voornaam, achternaam, geboortedatum, woonplaats, opleiding, functie_code, ervaring_sinds = prow
                functie_code_str = "" if functie_code is None else str(functie_code)

                # Lookups without JOIN
                f_oms = fetch_single_text(
                    sock, f,
                    "SELECT omschrijving FROM functie WHERE code=" + sql_quote(functie_code_str) + " LIMIT 1",
                    timeout=timeout
                ) or ""

                cat_code = fetch_single_text(
                    sock, f,
                    "SELECT categorie_code FROM functie WHERE code=" + sql_quote(functie_code_str) + " LIMIT 1",
                    timeout=timeout
                ) or ""

                cat_oms = ""
                if cat_code:
                    cat_oms = fetch_single_text(
                        sock, f,
                        "SELECT omschrijving FROM functie_categorie WHERE code=" + sql_quote(cat_code) + " LIMIT 1",
                        timeout=timeout
                    ) or ""

                gb_date = parse_ymd(str(geboortedatum))
                erv_date = parse_ymd(str(ervaring_sinds))
                opl_code = str(opleiding)
                opl_oms = fetch_single_text(
                    sock, f,
                    "SELECT omschrijving FROM onderwijsniveau WHERE code=" + sql_quote(opl_code) + " LIMIT 1",
                    timeout=timeout
                ) or ""

                opleiding_str = f"{opl_code} - {opl_oms}" if opl_oms else opl_code
                person = {
                    "voornaam": str(voornaam),
                    "achternaam": str(achternaam),
                    "naam": f"{achternaam}, {voornaam}",
                    "geboorte_fmt": fmt_ddmmyyyy(gb_date),
                    "leeftijd": years_between(gb_date, today),
                    "woonplaats": str(woonplaats),
                    "opleiding": opleiding_str,
                    "functie": f"{functie_code_str} - {f_oms}",
                    "categorie": f"{cat_code} - {cat_oms}" if (cat_code or cat_oms) else "",
                    "ervaring_jaren": years_between(erv_date, today),
                }

                # Output mode:
                if LOG_LEVEL >= LOG_DEBUG:
                    print_person_card(person)
                else:
                    if not header_printed:
                        print_header_once()
                        header_printed = True
                    print_row(person)

                if args.limit and count >= args.limit:
                    break

        finally:
            finalize(sock, f, stmt_p, timeout=timeout)

    total_sec = time.perf_counter() - t_start
    log_info(f"Done. Total persons processed: {count} in {total_sec:.3f} seconds")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log_error(f"{type(e).__name__}: {e}")
        sys.exit(1)   
