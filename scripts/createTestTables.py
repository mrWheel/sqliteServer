#!/usr/bin/env python3
import argparse
import json
import socket
import sys
import time
from typing import Any, Dict, List, Tuple, Optional

PROTO = "sqlite-tcp-v1"

# ---------------- logging ----------------
LOG_ERROR = 0
LOG_INFO = 1
LOG_DEBUG = 2
LOG_LEVEL = LOG_INFO

def ts() -> str:
    return time.strftime("%H:%M:%S")

def ms(dt_sec: float) -> int:
    return int(dt_sec * 1000.0)

def log_error(msg: str) -> None:
    if LOG_LEVEL >= LOG_ERROR:
        print(f"[{ts()}] ERROR {msg}", file=sys.stderr, flush=True)

def log_info(msg: str) -> None:
    if LOG_LEVEL >= LOG_INFO:
        print(f"[{ts()}] INFO  {msg}", file=sys.stderr, flush=True)

def log_debug(msg: str) -> None:
    if LOG_LEVEL >= LOG_DEBUG:
        print(f"[{ts()}] DEBUG {msg}", file=sys.stderr, flush=True)

# ---------------- protocol helpers ----------------
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

def exec_sql(sock: socket.socket, f, sql: str, timeout: float) -> Tuple[Dict[str, Any], int]:
    t0 = time.perf_counter()
    json_send(sock, {"op": "exec", "sql": sql})
    resp = expect_ok(json_recv_line(f, timeout, sock))
    return resp, ms(time.perf_counter() - t0)

def sql_quote(s: str) -> str:
    return "'" + s.replace("'", "''") + "'"

def insert_sql(prefix: str, table: str, cols: str, values: str) -> str:
    return f"{prefix} INTO {table} ({cols}) VALUES ({values});"

# ---------------- seed data ----------------
ONDERWIJSNIVEAU: List[Tuple[str, str]] = [
    ("-", "geen"),
    ("BO", "Basis Onderwijs"),
    ("LBO", "Lager Beroeps Onderwijs"),
    ("MA", "MAVO"),
    ("HA", "HAVO"),
    ("VWO", "Voorbereidend Wetenschappelijk Onderwijs"),
    ("MBO", "Middelbaar Beroeps Onderwijs"),
    ("HBO", "Hoger Beroeps Onderwijs"),
    ("GYM", "Gymnasium"),
    ("WO", "Wetenschappelijk Onderwijs"),
]

FUNCTIE_CATEGORIE: List[Tuple[str, str]] = [
    ("ADM", "Administratief & secretariaat"),
    ("COM", "Commercieel & sales"),
    ("ICT", "ICT"),
    ("ZRG", "Zorg & welzijn"),
    ("TEC", "Techniek & onderhoud"),
    ("LOG", "Logistiek & transport"),
    ("BOU", "Bouw & installatie"),
    ("FIN", "Financieel"),
    ("JUR", "Juridisch"),
    ("HRM", "HR & recruitment"),
    ("MKT", "Marketing & communicatie"),
    ("OND", "Onderwijs"),
    ("HOS", "Horeca & dienstverlening"),
    ("VEI", "Veiligheid & overheid"),
]

FUNCTIES: List[Tuple[str, str, str]] = [
    ("ADM", "Administratief medewerker", "ADM"),
    ("SEC", "Secretaresse / managementassistent", "ADM"),
    ("BAL", "Baliemedewerker / receptioniste", "ADM"),
    ("PLA", "Planner", "LOG"),
    ("INK", "Inkoper", "COM"),
    ("VER", "Verkoper (retail)", "COM"),
    ("ACM", "Accountmanager", "COM"),
    ("KSD", "Klantenservice medewerker", "COM"),
    ("MKT", "Marketing medewerker", "MKT"),
    ("COM", "Communicatiemedewerker", "MKT"),
    ("BOE", "Boekhouder", "FIN"),
    ("FIN", "Financieel medewerker", "FIN"),
    ("CTL", "Controller", "FIN"),
    ("JUR", "Jurist", "JUR"),
    ("HRM", "HR medewerker", "HRM"),
    ("REC", "Recruiter", "HRM"),
    ("MNG", "Teamleider / manager", "ADM"),
    ("PRJ", "Projectleider", "ADM"),
    ("ICT", "ICT medewerker (support)", "ICT"),
    ("SAD", "Systeembeheerder", "ICT"),
    ("DEV", "Software developer", "ICT"),
    ("DAT", "Data-analist", "ICT"),
    ("LOG", "Logistiek medewerker", "LOG"),
    ("MAG", "Magazijnmedewerker", "LOG"),
    ("CHD", "Chauffeur", "LOG"),
    ("OPR", "Productie operator", "TEC"),
    ("MNT", "Monteur", "TEC"),
    ("TCH", "Technisch medewerker", "TEC"),
    ("ELK", "Elektricien", "BOU"),
    ("TIM", "Timmerman", "BOU"),
    ("INS", "Installatiemonteur", "BOU"),
    ("DOC", "Docent", "OND"),
    ("ONB", "Onderwijsassistent", "OND"),
    ("VPK", "Verpleegkundige", "ZRG"),
    ("VIG", "Verzorgende IG", "ZRG"),
    ("ZAH", "Zorgassistent", "ZRG"),
    ("SPH", "Sociaal pedagogisch hulpverlener", "ZRG"),
    ("KOK", "Kok", "HOS"),
    ("BED", "Bedieningsmedewerker", "HOS"),
    ("SCH", "Schoonmaakmedewerker", "HOS"),
    ("BEV", "Beveiliger", "VEI"),
    ("BOA", "BOA (handhaving)", "VEI"),
]

PERSONEN: List[Tuple[str, str, str, str, str, str, str]] = [
    ("Jan","de Vries","1986-03-14","Amsterdam","HBO","PRJ","2010-09-01"),
    ("Sanne","Jansen","1992-11-02","Rotterdam","MBO","ADM","2013-07-01"),
    ("Daan","van Dijk","1989-06-21","Den Haag","WO","DEV","2012-02-01"),
    ("Lisa","Bakker","1996-01-09","Utrecht","HBO","HRM","2017-06-01"),
    ("Mohamed","Visser","1984-09-30","Eindhoven","BO","CHD","2006-05-01"),
    ("Noa","Smit","1999-04-18","Tilburg","MBO","KSD","2019-09-01"),
    ("Thomas","Meijer","1979-12-07","Groningen","HBO","CTL","2003-03-01"),
    ("Eva","de Jong","1991-08-25","Almere","HBO","MKT","2014-01-01"),
    ("Ruben","Mulder","1987-02-11","Nijmegen","MBO","MNT","2008-08-01"),
    ("Fleur","de Boer","1994-10-05","Arnhem","HBO","REC","2016-04-01"),
    ("Jesse","van Leeuwen","1988-08-14","Haarlem","HBO","SAD","2011-04-01"),
    ("Lotte","Dekker","1997-12-22","Amersfoort","LBO","BAL","2018-08-01"),
    ("Ahmed","van der Meer","1990-06-02","Breda","HBO","ACM","2012-09-01"),
    ("Iris","van den Berg","1983-10-11","Apeldoorn","WO","JUR","2008-01-01"),
    ("Mark","Vos","1985-01-16","Enschede","MBO","OPR","2007-09-01"),
    ("Nina","Bos","1993-03-03","Zwolle","HBO","ICT","2015-01-01"),
    ("Tim","Hendriks","2000-02-09","Leiden","LBO","MAG","2020-09-01"),
    ("Maud","Kuipers","1995-05-27","Zoetermeer","HBO","VIG","2016-09-01"),
    ("Anouk","Dijkstra","1986-11-18","Maastricht","HBO","FIN","2009-01-01"),
    ("Bas","Schouten","1992-02-20","Dordrecht","BO","BEV","2013-10-01"),
    ("Sara","van der Heijden","1999-09-12","Amsterdam","WO","DAT","2021-01-01"),
    ("Hugo","Kok","1984-04-08","Rotterdam","MBO","ELK","2006-09-01"),
    ("Roos","Peeters","1991-01-29","Den Haag","HBO","PRJ","2014-09-01"),
    ("Milan","Martens","1996-06-16","Utrecht","MBO","SCH","2016-10-01"),
    ("Eline","Willems","1989-09-01","Eindhoven","HBO","DOC","2012-08-01"),
    ("Stijn","Jacobs","1980-03-26","Tilburg","LBO","TIM","2001-07-01"),
    ("Britt","Koster","1993-07-07","Groningen","HBO","ONB","2014-09-01"),
    ("Olivier","Verhoeven","1990-12-23","Almere","WO","PRJ","2013-01-01"),
    ("Maaike","Gerritsen","1985-06-10","Nijmegen","HBO","BOE","2007-09-01"),
    ("Brian","van Loon","1997-11-05","Arnhem","MBO","KOK","2017-06-01"),
    ("Femke","van Dam","2001-01-30","Haarlem","MBO","BED","2019-05-01"),
    ("Johan","Post","1978-08-19","Amersfoort","HBO","MNG","2002-01-01"),
    ("Kim","Smits","1994-09-27","Breda","HBO","HRM","2016-02-01"),
    ("Ray","van Beek","1982-02-03","Apeldoorn","BO","BOA","2006-04-01"),
    ("Esmee","van den Bosch","1999-06-24","Enschede","HBO","VPK","2021-09-01"),
    ("Koen","van den Heuvel","1988-10-09","Zwolle","MBO","MNT","2010-03-01"),
    ("Naomi","van der Wal","1992-04-30","Leiden","HBO","ACM","2014-06-01"),
    ("Sjoerd","van der Veen","1986-02-17","Zoetermeer","WO","JUR","2010-01-01"),
    ("Ilse","van der Pol","1996-08-08","Maastricht","HBO","DEV","2018-07-01"),
    ("Niek","van der Linden","1983-01-12","Dordrecht","MBO","CHD","2005-09-01"),
    ("Aylin","de Vries","1997-04-04","Amsterdam","HBO","KSD","2018-09-01"),
    ("Sem","Jansen","2000-09-20","Rotterdam","BO","LOG","2020-10-01"),
    ("Nora","van Dijk","1991-12-01","Den Haag","HBO","FIN","2013-09-01"),
    ("Kasper","Bakker","1989-05-23","Utrecht","MBO","TCH","2011-02-01"),
    ("Sofia","Visser","1998-02-14","Eindhoven","HBO","ADM","2019-03-01"),
    ("Rick","Smit","1984-11-09","Tilburg","MBO","INS","2007-01-01"),
    ("Amber","Meijer","1995-07-19","Groningen","HBO","COM","2017-02-01"),
    ("Bart","de Jong","1977-04-28","Almere","MBO","ELK","1999-09-01"),
    ("Lynn","Mulder","2002-06-06","Nijmegen","LBO","MAG","2021-07-01"),
]

# ---------------- seeding helpers ----------------
def run_batch(sock: socket.socket, f, timeout: float, label: str, sql_list: List[str], progress_every: int = 10) -> None:
    t_batch0 = time.perf_counter()
    t_chunk0 = time.perf_counter()
    for i, sql in enumerate(sql_list, start=1):
        _, dt_ms = exec_sql(sock, f, sql, timeout)
        log_debug(f"{label} item#{i} dt={dt_ms}ms")
        if LOG_LEVEL >= LOG_INFO and progress_every > 0 and (i % progress_every == 0):
            chunk_ms = ms(time.perf_counter() - t_chunk0)
            log_info(f"{label}: processed {i}/{len(sql_list)} in {chunk_ms} ms")
            t_chunk0 = time.perf_counter()
    total_ms = ms(time.perf_counter() - t_batch0)
    log_info(f"{label}: done ({len(sql_list)} rows) in {total_ms} ms")

def make_sql_lists(insert_prefix: str) -> Dict[str, List[str]]:
    sqls: Dict[str, List[str]] = {}

    sqls["onderwijsniveau"] = [
        insert_sql(insert_prefix, "onderwijsniveau", "code, omschrijving", f"{sql_quote(code)},{sql_quote(oms)}")
        for code, oms in ONDERWIJSNIVEAU
    ]

    sqls["functie_categorie"] = [
        insert_sql(insert_prefix, "functie_categorie", "code, omschrijving", f"{sql_quote(code)},{sql_quote(oms)}")
        for code, oms in FUNCTIE_CATEGORIE
    ]

    sqls["functie"] = [
        insert_sql(insert_prefix, "functie", "code, omschrijving, categorie_code", f"{sql_quote(code)},{sql_quote(oms)},{sql_quote(cat)}")
        for code, oms, cat in FUNCTIES
    ]

    sqls["persoon"] = [
        insert_sql(
            insert_prefix,
            "persoon",
            "voornaam, achternaam, geboortedatum, woonplaats, opleiding, functie, ervaring_sinds",
            f"{sql_quote(vn)},{sql_quote(an)},{sql_quote(gb)},{sql_quote(wp)},{sql_quote(opl)},{sql_quote(fnc)},{sql_quote(erv)}",
        )
        for (vn, an, gb, wp, opl, fnc, erv) in PERSONEN
    ]

    return sqls

def main() -> int:
    global LOG_LEVEL

    ap = argparse.ArgumentParser(description="Create/seed ESP32 sqlite-tcp-v1 tables.")
    ap.add_argument("--host", default="192.168.12.14", help="Server IP/hostname")
    ap.add_argument("--port", type=int, default=5555, help="Server port")
    ap.add_argument("--timeout", type=float, default=5.0, help="Per request timeout seconds")
    ap.add_argument("--log-level", choices=["error", "info", "debug"], default="info", help="Logging verbosity")
    ap.add_argument("--no-drop", action="store_true", help="Do not DROP TABLEs first (idempotent seeding)")
    ap.add_argument("--upsert", action="store_true", help="Use INSERT OR REPLACE instead of INSERT OR IGNORE when --no-drop")
    ap.add_argument("--tables", default="all", help="Comma separated: all,persoon,functie,functie_categorie,onderwijsniveau")
    args = ap.parse_args()

    LOG_LEVEL = {"error": LOG_ERROR, "info": LOG_INFO, "debug": LOG_DEBUG}[args.log_level]
    insert_prefix = "INSERT" if not args.no_drop else ("INSERT OR REPLACE" if args.upsert else "INSERT OR IGNORE")

    selected = [t.strip() for t in args.tables.split(",") if t.strip()]
    allowed = {"all", "persoon", "functie", "functie_categorie", "onderwijsniveau"}
    for t in selected:
        if t not in allowed:
            log_error(f"Invalid --tables value: {t}. Allowed: all,persoon,functie,functie_categorie,onderwijsniveau")
            return 1
    if "all" in selected:
        selected = ["onderwijsniveau", "functie_categorie", "functie", "persoon"]

    log_info(f"Connecting to {args.host}:{args.port} ...")
    t_conn0 = time.perf_counter()

    with socket.create_connection((args.host, args.port), timeout=5) as sock:
        f = sock.makefile("rb")

        log_info("Waiting for handshake ...")
        hello = expect_ok(json_recv_line(f, args.timeout, sock))
        if hello.get("hello") != PROTO:
            log_error(f"Unexpected protocol. Expected {PROTO}, got {hello.get('hello')}")
            return 1
        log_info(f"Handshake OK in {ms(time.perf_counter()-t_conn0)} ms")

        t_ping0 = time.perf_counter()
        json_send(sock, {"op": "ping"})
        expect_ok(json_recv_line(f, args.timeout, sock))
        log_info(f"Ping OK in {ms(time.perf_counter()-t_ping0)} ms")

        # Drop (optional)
        if not args.no_drop:
            log_info("Dropping tables (if exist) ...")
            t_drop0 = time.perf_counter()
            exec_sql(sock, f, "DROP TABLE IF EXISTS persoon;", args.timeout*2)
            exec_sql(sock, f, "DROP TABLE IF EXISTS functie;", args.timeout*2)
            exec_sql(sock, f, "DROP TABLE IF EXISTS functie_categorie;", args.timeout*2)
            exec_sql(sock, f, "DROP TABLE IF EXISTS onderwijsniveau;", args.timeout*2)
            log_info(f"Drop done in {ms(time.perf_counter()-t_drop0)} ms")

        # Create tables
        log_info("Creating tables ...")
        t_create0 = time.perf_counter()
        exec_sql(sock, f, "CREATE TABLE IF NOT EXISTS onderwijsniveau (id INTEGER PRIMARY KEY, code TEXT NOT NULL, omschrijving TEXT NOT NULL);", args.timeout*2)
        exec_sql(sock, f, "CREATE UNIQUE INDEX IF NOT EXISTS idx_onderwijsniveau_code ON onderwijsniveau(code);", args.timeout*2)
        exec_sql(sock, f, "CREATE TABLE IF NOT EXISTS functie_categorie (id INTEGER PRIMARY KEY, code TEXT NOT NULL, omschrijving TEXT NOT NULL);", args.timeout*2)
        exec_sql(sock, f, "CREATE UNIQUE INDEX IF NOT EXISTS idx_functie_categorie_code ON functie_categorie(code);", args.timeout*2)
        exec_sql(sock, f, "CREATE TABLE IF NOT EXISTS functie (id INTEGER PRIMARY KEY, code TEXT NOT NULL, omschrijving TEXT NOT NULL, categorie_code TEXT NOT NULL);", args.timeout*2)
        exec_sql(sock, f, "CREATE UNIQUE INDEX IF NOT EXISTS idx_functie_code ON functie(code);", args.timeout*2)
        exec_sql(sock, f, "CREATE INDEX IF NOT EXISTS idx_functie_categorie_code ON functie(categorie_code);", args.timeout*2)
        exec_sql(sock, f, "CREATE TABLE IF NOT EXISTS persoon (id INTEGER PRIMARY KEY, voornaam TEXT NOT NULL, achternaam TEXT NOT NULL, geboortedatum TEXT NOT NULL, woonplaats TEXT NOT NULL, opleiding TEXT NOT NULL, functie TEXT NOT NULL, ervaring_sinds TEXT NOT NULL);", args.timeout*2)
        log_info(f"Create done in {ms(time.perf_counter()-t_create0)} ms")

        log_info(f"Seeding with '{insert_prefix}' for tables: {','.join(selected)}")

        sql_lists = make_sql_lists(insert_prefix)

        # Seed selected tables, with per-10 progress times
        for table in selected:
            run_batch(sock, f, args.timeout, f"Seed {table}", sql_lists[table], progress_every=10)

        log_info("All done.")
        return 0

if __name__ == "__main__":
    try:
        rc = main()
    except Exception as e:
        log_error(f"{type(e).__name__}: {e}")
        rc = 1
    raise SystemExit(rc)
