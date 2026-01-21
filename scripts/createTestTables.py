#!/usr/bin/env python3
# createTestTables.py — seed script for sqlite-tcp-v1 (ESP32 SQLite TCP Server)
# - Creates and seeds: onderwijsniveau, functie, functie_categorie, persoon
# - persoon has: functie (code), categorie (code), inkomen (integer)
# - Generates 60 persons, 50 unique last names, top-10 NL cities, education+income with (approx) normal distributions
# - No JOINs, no constraints (no FOREIGN KEY), only indexes for speed
# - Lots of log_info() for progress
#
# Usage examples:
#   ./createTestTables.py
#   ./createTestTables.py --no-drop
#   ./createTestTables.py --no-drop --no-refresh-personen
#   ./createTestTables.py --log-level debug
#   ./createTestTables.py --seed 123

import argparse
import json
import random
import socket
import sys
import time
from datetime import date, timedelta
from typing import Any, Dict, List, Tuple, Optional

PROTO = "sqlite-tcp-v1"

# ---------------- logging ----------------
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

def ms(dt_sec: float) -> int:
    return int(dt_sec * 1000.0)

def sec3(dt_sec: float) -> str:
    return f"{dt_sec:.3f}s"

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
        raise RuntimeError(resp.get("error", {}).get("message", f"Server error: {resp}"))
    return resp

def exec_sql(sock: socket.socket, f, sql: str, timeout: float) -> int:
    t0 = time.perf_counter()
    json_send(sock, {"op": "exec", "sql": sql})
    resp = expect_ok(json_recv_line(f, timeout, sock))
    _ = resp.get("changes", None)
    return ms(time.perf_counter() - t0)

def sql_quote(s: str) -> str:
    return "'" + s.replace("'", "''") + "'"

def insert_sql(prefix: str, table: str, cols: str, values: str) -> str:
    return f"{prefix} INTO {table} ({cols}) VALUES ({values});"

# ---------------- seed data (static tables) ----------------
# Education codes: include LO, MA (MAVO), HA (HAVO), GYM, VWO, etc.
ONDERWIJSNIVEAU: List[Tuple[str, str]] = [
    ("-", "Geen"),
    ("LO", "Lager Onderwijs"),
    ("BO", "Basis Onderwijs"),
    ("LBO", "Lager Beroeps Onderwijs"),
    ("MA", "MAVO"),
    ("HA", "HAVO"),
    ("VWO", "Voorbereidend Wetenschappelijk Onderwijs"),
    ("GYM", "Gymnasium"),
    ("MBO", "Middelbaar Beroeps Onderwijs"),
    ("HBO", "Hoger Beroeps Onderwijs"),
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

# functies: code + omschrijving (categorie is an attribute of persoon now)
FUNCTIES: List[Tuple[str, str]] = [
    ("ADM", "Administratief medewerker"),
    ("SEC", "Secretaresse / managementassistent"),
    ("BAL", "Baliemedewerker / receptioniste"),
    ("PLA", "Planner"),
    ("INK", "Inkoper"),
    ("VER", "Verkoper (retail)"),
    ("ACM", "Accountmanager"),
    ("KSD", "Klantenservice medewerker"),
    ("MKT", "Marketing medewerker"),
    ("COM", "Communicatiemedewerker"),
    ("BOE", "Boekhouder"),
    ("FIN", "Financieel medewerker"),
    ("CTL", "Controller"),
    ("JUR", "Jurist"),
    ("HRM", "HR medewerker"),
    ("REC", "Recruiter"),
    ("MNG", "Teamleider / manager"),
    ("PRJ", "Projectleider"),
    ("SUP", "ICT support medewerker"),
    ("SAD", "Systeembeheerder"),
    ("DEV", "Software developer"),
    ("DAT", "Data-analist"),
    ("LOG", "Logistiek medewerker"),
    ("MAG", "Magazijnmedewerker"),
    ("CHD", "Chauffeur"),
    ("OPR", "Productie operator"),
    ("MNT", "Monteur"),
    ("TCH", "Technisch medewerker"),
    ("ELK", "Elektricien"),
    ("TIM", "Timmerman"),
    ("INS", "Installatiemonteur"),
    ("DOC", "Docent"),
    ("ONB", "Onderwijsassistent"),
    ("VPK", "Verpleegkundige"),
    ("VIG", "Verzorgende IG"),
    ("ZAH", "Zorgassistent"),
    ("SPH", "Sociaal pedagogisch hulpverlener"),
    ("KOK", "Kok"),
    ("BED", "Bedieningsmedewerker"),
    ("SCH", "Schoonmaakmedewerker"),
    ("BEV", "Beveiliger"),
    ("BOA", "BOA (handhaving)"),
]

# For plausibility: "typical category" per function code.
# Person's categorie can differ; we will mostly follow this, with some variation.
TYPICAL_CAT: Dict[str, str] = {
    "ADM": "ADM", "SEC": "ADM", "BAL": "ADM", "PLA": "LOG", "INK": "COM", "VER": "COM",
    "ACM": "COM", "KSD": "COM", "MKT": "MKT", "COM": "MKT", "BOE": "FIN", "FIN": "FIN",
    "CTL": "FIN", "JUR": "JUR", "HRM": "HRM", "REC": "HRM", "MNG": "ADM", "PRJ": "ADM",
    "SUP": "ICT", "SAD": "ICT", "DEV": "ICT", "DAT": "ICT", "LOG": "LOG", "MAG": "LOG",
    "CHD": "LOG", "OPR": "TEC", "MNT": "TEC", "TCH": "TEC", "ELK": "BOU", "TIM": "BOU",
    "INS": "BOU", "DOC": "OND", "ONB": "OND", "VPK": "ZRG", "VIG": "ZRG", "ZAH": "ZRG",
    "SPH": "ZRG", "KOK": "HOS", "BED": "HOS", "SCH": "HOS", "BEV": "VEI", "BOA": "VEI",
}

TOP10_CITIES_NL = [
    "Amsterdam", "Rotterdam", "Den Haag", "Utrecht", "Eindhoven",
    "Groningen", "Tilburg", "Almere", "Breda", "Nijmegen"
]

FIRSTNAMES_M = ["Jan","Daan","Thomas","Ruben","Jesse","Ahmed","Mark","Tim","Bas","Hugo","Milan","Stijn","Olivier","Brian","Johan","Koen","Niek","Sem","Kasper","Rick","Bart","Sjoerd","Mohamed","Ray","Tariq"]
FIRSTNAMES_F = ["Sanne","Lisa","Eva","Fleur","Lotte","Iris","Nina","Maud","Anouk","Sara","Roos","Eline","Britt","Maaike","Femke","Kim","Naomi","Ilse","Esmee","Aylin","Nora","Sofia","Amber","Lynn","Noa"]

# 50 unique last names (simple mix, common NL-ish)
LASTNAMES_50 = [
    "de Vries","Jansen","van Dijk","Bakker","Visser","Smit","Meijer","de Jong","Mulder","de Boer",
    "van Leeuwen","Dekker","van der Meer","van den Berg","Vos","Bos","Hendriks","Kuipers","Dijkstra","Schouten",
    "van der Heijden","Kok","Peeters","Martens","Willems","Jacobs","Koster","Verhoeven","Gerritsen","van Loon",
    "van Dam","Post","Smits","van Beek","van den Bosch","van den Heuvel","van der Wal","van der Veen","van der Pol","van der Linden",
    "de Wit","van 't Hof","van der Plas","van de Ven","van der Steen","de Graaf","van der Horst","Wolters","Koning","van der Krogt"
]

# ---------------- generation helpers ----------------
def clamp_int(x: float, lo: int, hi: int) -> int:
    if x < lo:
        return lo
    if x > hi:
        return hi
    return int(round(x))

def rand_norm(rng: random.Random, mean: float, std: float) -> float:
    # Box-Muller transform
    u1 = max(1e-12, rng.random())
    u2 = max(1e-12, rng.random())
    z = ( (-2.0 * (u1).ln()) ** 0.5 )  # will fail: float has no ln
    return mean + std * z

def rand_norm_box_muller(rng: random.Random, mean: float, std: float) -> float:
    # Box-Muller, using math without importing heavy libs isn't possible; use math.
    import math
    u1 = max(1e-12, rng.random())
    u2 = max(1e-12, rng.random())
    z0 = math.sqrt(-2.0 * math.log(u1)) * math.cos(2.0 * math.pi * u2)
    return mean + std * z0

def choose_education_codes_60(rng: random.Random) -> List[str]:
    # Ensure each education appears at least once
    codes = [c for c, _ in ONDERWIJSNIVEAU]
    # We'll exclude "-" from "normal distribution"; but user asked all opleidingen min 1x,
    # so we keep "-" once as well.
    must = codes[:]  # includes "-"
    remaining = 60 - len(must)
    if remaining < 0:
        # shouldn't happen
        must = must[:60]
        remaining = 0

    # Define an approximate "normal-like" distribution centered around MBO/HBO.
    # We'll map integer score to codes.
    # score ~ N(mean=6.5, std=1.8) on 0..10 scale and bucket.
    buckets = ["-","LO","BO","LBO","MA","HA","VWO","GYM","MBO","HBO","WO"]  # index 0..10
    out = must[:]
    for _ in range(remaining):
        s = rand_norm_box_muller(rng, mean=8.0, std=1.7)  # centered near MBO/HBO
        idx = clamp_int(s, 0, 10)
        out.append(buckets[idx])
    rng.shuffle(out)
    return out

def income_for_education(rng: random.Random, edu: str) -> int:
    # Rough annual gross income ranges by education (EUR)
    # We'll sample from normal distributions and clamp to plausible bands.
    params = {
        "-":  (22000, 4000, 16000, 32000),
        "LO": (23000, 4500, 16000, 34000),
        "BO": (24000, 4500, 17000, 36000),
        "LBO":(26000, 5000, 18000, 40000),
        "MA": (28000, 5500, 20000, 45000),
        "HA": (30000, 6000, 22000, 50000),
        "VWO":(32000, 6500, 24000, 56000),
        "GYM":(33000, 7000, 25000, 60000),
        "MBO":(34000, 7000, 24000, 60000),
        "HBO":(42000, 9000, 28000, 80000),
        "WO": (52000, 12000, 32000, 120000),
    }
    mean, std, lo, hi = params.get(edu, (32000, 7000, 20000, 70000))
    x = rand_norm_box_muller(rng, mean, std)
    return clamp_int(x, lo, hi)

def random_birthdate(rng: random.Random) -> str:
    # Age 20..60
    today = date.today()
    age_years = rng.randint(20, 60)
    # +/- 180 days jitter
    jitter_days = rng.randint(-180, 180)
    d = today - timedelta(days=age_years * 365 + jitter_days)
    return d.isoformat()

def random_experience_since(rng: random.Random, birth_ymd: str) -> str:
    # Experience since age 16.. (now), but realistic: between 0 and 25 years
    today = date.today()
    b = date.fromisoformat(birth_ymd)
    earliest = b + timedelta(days=16 * 365)
    if earliest > today:
        earliest = today
    # choose a start date between max(earliest, today-25y) and today
    min_start = today - timedelta(days=25 * 365)
    if earliest > min_start:
        min_start = earliest
    if min_start > today:
        min_start = today
    span = (today - min_start).days
    start = min_start + timedelta(days=rng.randint(0, max(0, span)))
    return start.isoformat()

def generate_personen_60(rng: random.Random) -> List[Tuple[str,str,str,str,str,str,str,str,int]]:
    # Output tuple:
    # (voornaam, achternaam, geboortedatum, woonplaats, opleiding_code, functie_code, categorie_code, ervaring_sinds, inkomen)
    educations = choose_education_codes_60(rng)

    # Need 60 persons, 50 unique last names => pick 50 unique + repeat 10 randomly
    lastnames = LASTNAMES_50[:]
    repeats = [rng.choice(LASTNAMES_50) for _ in range(10)]
    lastnames60 = lastnames + repeats
    rng.shuffle(lastnames60)

    functie_codes = [c for c, _ in FUNCTIES]
    categorie_codes = [c for c, _ in FUNCTIE_CATEGORIE]

    personen: List[Tuple[str,str,str,str,str,str,str,str,int]] = []
    for i in range(60):
        gender = "F" if rng.random() < 0.5 else "M"
        vn = rng.choice(FIRSTNAMES_F if gender == "F" else FIRSTNAMES_M)
        an = lastnames60[i]
        gb = random_birthdate(rng)
        wp = rng.choice(TOP10_CITIES_NL)
        opl = educations[i]

        fnc = rng.choice(functie_codes)

        # categorie is a property of person; mostly typical, sometimes vary (esp PRJ)
        typical = TYPICAL_CAT.get(fnc, rng.choice(categorie_codes))
        if fnc == "PRJ":
            # allow PRJ in many domains
            if rng.random() < 0.65:
                cat = rng.choice(["ICT","ZRG","BOU","FIN","OND","ADM","TEC","LOG"])
            else:
                cat = typical
        else:
            # 85% typical, 15% random other
            if rng.random() < 0.85:
                cat = typical
            else:
                cat = rng.choice(categorie_codes)

        erv = random_experience_since(rng, gb)

        inc = income_for_education(rng, opl)

        personen.append((vn, an, gb, wp, opl, fnc, cat, erv, inc))
    return personen

# ---------------- seeding helpers ----------------
def run_batch(sock: socket.socket, f, timeout: float, label: str, sql_list: List[str], progress_every: int = 10) -> None:
    log_info(f"{label}: starting ({len(sql_list)} statements) ...")
    t0 = time.perf_counter()
    t_chunk0 = time.perf_counter()
    ok = 0
    for i, sql in enumerate(sql_list, start=1):
        try:
            dt_ms = exec_sql(sock, f, sql, timeout)
            ok += 1
            log_debug(f"{label} item#{i} dt={dt_ms}ms")
        except Exception as e:
            # keep going but report
            log_error(f"{label} item#{i} failed: {type(e).__name__}: {e}")
        if progress_every > 0 and (i % progress_every == 0):
            chunk = time.perf_counter() - t_chunk0
            log_info(f"{label}: processed {i}/{len(sql_list)} in {sec3(chunk)}")
            t_chunk0 = time.perf_counter()
    total = time.perf_counter() - t0
    log_info(f"{label}: done ok={ok}/{len(sql_list)} in {sec3(total)}")

def make_sql_lists(insert_prefix: str,
                   personen: List[Tuple[str,str,str,str,str,str,str,str,int]]) -> Dict[str, List[str]]:
    sqls: Dict[str, List[str]] = {}

    sqls["onderwijsniveau"] = [
        insert_sql(insert_prefix, "onderwijsniveau", "code,omschrijving",
                   f"{sql_quote(code)},{sql_quote(oms)}")
        for code, oms in ONDERWIJSNIVEAU
    ]

    sqls["functie_categorie"] = [
        insert_sql(insert_prefix, "functie_categorie", "code,omschrijving",
                   f"{sql_quote(code)},{sql_quote(oms)}")
        for code, oms in FUNCTIE_CATEGORIE
    ]

    sqls["functie"] = [
        insert_sql(insert_prefix, "functie", "code,omschrijving",
                   f"{sql_quote(code)},{sql_quote(oms)}")
        for code, oms in FUNCTIES
    ]

    sqls["persoon"] = [
        insert_sql(
            insert_prefix,
            "persoon",
            "voornaam,achternaam,geboortedatum,woonplaats,opleiding,functie,categorie,ervaring_sinds,inkomen",
            f"{sql_quote(vn)},{sql_quote(an)},{sql_quote(gb)},{sql_quote(wp)},"
            f"{sql_quote(opl)},{sql_quote(fnc)},{sql_quote(cat)},{sql_quote(erv)},{int(inc)}",
        )
        for (vn, an, gb, wp, opl, fnc, cat, erv, inc) in personen
    ]

    return sqls

def main() -> int:
    global LOG_LEVEL

    ap = argparse.ArgumentParser(description="Create/seed ESP32 sqlite-tcp-v1 tables (flat model, no constraints).")
    ap.add_argument("--host", default="192.168.12.14", help="Server IP/hostname")
    ap.add_argument("--port", type=int, default=5555, help="Server port")
    ap.add_argument("--timeout", type=float, default=10.0, help="Per request timeout seconds")
    ap.add_argument("--log-level", choices=["error","info","debug"], default="info", help="Logging verbosity")
    ap.add_argument("--no-drop", action="store_true", help="Do not DROP TABLEs first")
    ap.add_argument("--no-refresh-personen", action="store_true", help="When --no-drop: do NOT delete persoon before insert")
    ap.add_argument("--upsert", action="store_true", help="When --no-drop: use INSERT OR REPLACE (otherwise INSERT OR IGNORE)")
    ap.add_argument("--seed", type=int, default=42, help="Random seed for person generation (default: 42)")
    args = ap.parse_args()

    LOG_LEVEL = {"error":LOG_ERROR, "info":LOG_INFO, "debug":LOG_DEBUG}[args.log_level]

    rng = random.Random(args.seed)

    insert_prefix = "INSERT" if not args.no_drop else ("INSERT OR REPLACE" if args.upsert else "INSERT OR IGNORE")

    log_info(f"Config: host={args.host} port={args.port} timeout={args.timeout}s log={args.log_level} seed={args.seed}")
    log_info(f"Insert mode: {insert_prefix}")

    # generate persons now (so we can log distribution)
    log_info("Generating 60 persons (50 unique last names, top-10 NL cities, education+income ~ normal) ...")
    t_gen0 = time.perf_counter()
    personen = generate_personen_60(rng)
    log_info(f"Generated persons in {sec3(time.perf_counter()-t_gen0)}")

    # sanity: education coverage
    edu_counts: Dict[str, int] = {}
    for p in personen:
        edu_counts[p[4]] = edu_counts.get(p[4], 0) + 1
    missing = [c for c, _ in ONDERWIJSNIVEAU if edu_counts.get(c, 0) == 0]
    if missing:
        log_error(f"Education coverage error (missing): {missing}")
        return 1
    log_info("Education coverage OK (all education levels appear at least once).")
    log_info("Education counts: " + ", ".join([f"{k}={edu_counts[k]}" for k in sorted(edu_counts.keys())]))

    log_info(f"Connecting to {args.host}:{args.port} ...")
    t_conn0 = time.perf_counter()

    try:
        with socket.create_connection((args.host, args.port), timeout=5) as sock:
            f = sock.makefile("rb")

            log_info("Waiting for handshake ...")
            hello = expect_ok(json_recv_line(f, args.timeout, sock))
            if hello.get("hello") != PROTO:
                log_error(f"Unexpected protocol. Expected {PROTO}, got {hello.get('hello')}")
                return 1
            log_info(f"Handshake OK in {sec3(time.perf_counter()-t_conn0)}")

            t_ping0 = time.perf_counter()
            json_send(sock, {"op": "ping"})
            expect_ok(json_recv_line(f, args.timeout, sock))
            log_info(f"Ping OK in {sec3(time.perf_counter()-t_ping0)}")

            # Drop tables (optional)
            if not args.no_drop:
                log_info("Dropping tables (if exist) ...")
                t_drop0 = time.perf_counter()
                exec_sql(sock, f, "DROP TABLE IF EXISTS persoon;", args.timeout * 2)
                exec_sql(sock, f, "DROP TABLE IF EXISTS functie;", args.timeout * 2)
                exec_sql(sock, f, "DROP TABLE IF EXISTS functie_categorie;", args.timeout * 2)
                exec_sql(sock, f, "DROP TABLE IF EXISTS onderwijsniveau;", args.timeout * 2)
                log_info(f"Drop done in {sec3(time.perf_counter()-t_drop0)}")
            else:
                log_info("Skipping DROP (--no-drop).")

            # Create tables (no constraints)
            log_info("Creating tables (no constraints) ...")
            t_create0 = time.perf_counter()

            log_info("--> Creating onderwijsniveau table ...")
            exec_sql(sock, f, "CREATE TABLE IF NOT EXISTS onderwijsniveau (id INTEGER PRIMARY KEY, code TEXT NOT NULL, omschrijving TEXT NOT NULL);", args.timeout * 2)
            exec_sql(sock, f, "CREATE UNIQUE INDEX IF NOT EXISTS idx_onderwijsniveau_code ON onderwijsniveau(code);", args.timeout * 2)

            log_info("--> Creating functie_categorie table ...")
            exec_sql(sock, f, "CREATE TABLE IF NOT EXISTS functie_categorie (id INTEGER PRIMARY KEY, code TEXT NOT NULL, omschrijving TEXT NOT NULL);", args.timeout * 2)
            exec_sql(sock, f, "CREATE UNIQUE INDEX IF NOT EXISTS idx_functie_categorie_code ON functie_categorie(code);", args.timeout * 2)

            log_info("--> Creating functie table ...")
            exec_sql(sock, f, "CREATE TABLE IF NOT EXISTS functie (id INTEGER PRIMARY KEY, code TEXT NOT NULL, omschrijving TEXT NOT NULL);", args.timeout * 2)
            exec_sql(sock, f, "CREATE UNIQUE INDEX IF NOT EXISTS idx_functie_code ON functie(code);", args.timeout * 2)

            log_info("--> Creating persoon table ...")  
            # persoon: categorie is per person; functie is per person; inkomen added
            exec_sql(sock, f, "CREATE TABLE IF NOT EXISTS persoon (id INTEGER PRIMARY KEY, voornaam TEXT NOT NULL, achternaam TEXT NOT NULL, geboortedatum TEXT NOT NULL, woonplaats TEXT NOT NULL, opleiding TEXT NOT NULL, functie TEXT NOT NULL, categorie TEXT NOT NULL, ervaring_sinds TEXT NOT NULL, inkomen INTEGER NOT NULL);", args.timeout * 2)
            exec_sql(sock, f, "CREATE INDEX IF NOT EXISTS idx_persoon_achternaam ON persoon(achternaam);", args.timeout * 2)
            exec_sql(sock, f, "CREATE INDEX IF NOT EXISTS idx_persoon_woonplaats ON persoon(woonplaats);", args.timeout * 2)
            exec_sql(sock, f, "CREATE INDEX IF NOT EXISTS idx_persoon_functie ON persoon(functie);", args.timeout * 2)
            exec_sql(sock, f, "CREATE INDEX IF NOT EXISTS idx_persoon_categorie ON persoon(categorie);", args.timeout * 2)

            log_info(f"Create done in {sec3(time.perf_counter()-t_create0)}")

            # Make SQL lists
            log_info("Building SQL statement lists ...")
            t_build0 = time.perf_counter()
            sql_lists = make_sql_lists(insert_prefix, personen)
            log_info(f"SQL lists built in {sec3(time.perf_counter()-t_build0)}")

            # Refresh persons when no-drop (avoid duplicates)
            if args.no_drop and not args.no_refresh_personen:
                log_info("Refreshing persoon (DELETE FROM persoon) to avoid duplicates ...")
                t_ref0 = time.perf_counter()
                exec_sql(sock, f, "DELETE FROM persoon;", args.timeout * 2)
                log_info(f"Refresh persoon done in {sec3(time.perf_counter()-t_ref0)}")
            elif args.no_drop and args.no_refresh_personen:
                log_info("Not refreshing persoon (--no-refresh-personen). Duplicates may occur if you INSERT again.")

            # Seed in logical order
            log_info("Seeding tables ...")
            run_batch(sock, f, args.timeout, "Seed onderwijsniveau", sql_lists["onderwijsniveau"], progress_every=10)
            run_batch(sock, f, args.timeout, "Seed functie_categorie", sql_lists["functie_categorie"], progress_every=10)
            run_batch(sock, f, args.timeout, "Seed functie", sql_lists["functie"], progress_every=10)
            run_batch(sock, f, args.timeout, "Seed persoon", sql_lists["persoon"], progress_every=10)

            log_info("All done.")
            return 0

    except Exception as e:
        log_error(f"{type(e).__name__}: {e}")
        return 1

if __name__ == "__main__":
    raise SystemExit(main())
