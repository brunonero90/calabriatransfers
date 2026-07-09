import hashlib
import html
import json
import os
import re
import sqlite3
import time
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional


ROOT = Path(__file__).resolve().parent
DATA = ROOT / "data"
DIRS = [
    DATA / "operators",
    DATA / "routes",
    DATA / "discovery",
    DATA / "snapshots",
    DATA / "queue",
    DATA / "checkpoints",
]
DB_PATH = DATA / "checkpoints" / "state.db"
SITE_PATH = ROOT / "site"
ASSETS_PATH = SITE_PATH / "assets"

USER_AGENT = "CalabriaTransfersBot/1.0 (+local-builder)"
CYCLE_SECONDS = 120
MAX_TASKS_PER_CYCLE = 25

QUALITY_FIELDS = [
    "name",
    "phone",
    "email",
    "whatsapp",
    "website",
    "town",
    "languages",
    "vehicles",
    "photos",
    "services",
    "coverage",
]

CALABRIA_TOWNS = [
    "Reggio Calabria",
    "Catanzaro",
    "Cosenza",
    "Crotone",
    "Vibo Valentia",
    "Lamezia Terme",
    "Tropea",
    "Scilla",
    "Soverato",
    "Rende",
]


@dataclass
class QueueItem:
    item_type: str
    priority: int
    payload: Dict
    dedupe_key: str


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def stable_hash(payload: Dict) -> str:
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha1(raw).hexdigest()


def slugify(value: str) -> str:
    value = value.lower()
    value = re.sub(r"[^a-z0-9]+", "-", value)
    return value.strip("-")[:80] or "operator"


def ensure_dirs() -> None:
    for d in DIRS:
        d.mkdir(parents=True, exist_ok=True)
    (SITE_PATH / "operators").mkdir(parents=True, exist_ok=True)
    (SITE_PATH / "towns").mkdir(parents=True, exist_ok=True)
    (SITE_PATH / "routes").mkdir(parents=True, exist_ok=True)
    (SITE_PATH / "airports").mkdir(parents=True, exist_ok=True)
    (SITE_PATH / "guides").mkdir(parents=True, exist_ok=True)
    ASSETS_PATH.mkdir(parents=True, exist_ok=True)


def write_theme_assets() -> None:
    css = """
:root {
  --bg: #07121f;
  --bg-soft: #0d1b2b;
  --card: #12263a;
  --line: #264760;
  --text: #e8f2fb;
  --muted: #9bb7cf;
  --brand: #39a0ff;
  --brand-2: #52d6be;
  --ok: #28c76f;
  --warn: #fcbf49;
}
* { box-sizing: border-box; }
body {
  margin: 0;
  font-family: Inter, Segoe UI, Arial, sans-serif;
  background: radial-gradient(circle at top, #0f2741, var(--bg) 45%);
  color: var(--text);
}
.container { max-width: 1100px; margin: 0 auto; padding: 24px; }
.topbar {
  position: sticky; top: 0; z-index: 10; backdrop-filter: blur(8px);
  border-bottom: 1px solid var(--line); background: rgba(7,18,31,0.75);
}
.brand { font-weight: 800; letter-spacing: 0.2px; color: #fff; text-decoration: none; }
.nav { display: flex; align-items: center; justify-content: space-between; gap: 12px; }
.links a { color: var(--muted); text-decoration: none; margin-left: 14px; }
.links a:hover { color: #fff; }
.hero {
  border: 1px solid var(--line); border-radius: 16px; padding: 28px;
  background: linear-gradient(145deg, rgba(57,160,255,.2), rgba(82,214,190,.1));
}
.hero h1 { margin: 0 0 10px; font-size: 2rem; }
.hero p { margin: 0; color: var(--muted); }
.stats { display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 12px; margin-top: 16px; }
.stat { border: 1px solid var(--line); border-radius: 12px; background: var(--bg-soft); padding: 14px; }
.stat .v { font-weight: 800; font-size: 1.25rem; }
.section { margin-top: 22px; }
.section h2 { margin: 0 0 10px; }
.grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(240px, 1fr)); gap: 12px; }
.card {
  border: 1px solid var(--line); border-radius: 12px; padding: 14px;
  background: linear-gradient(180deg, rgba(255,255,255,.02), rgba(255,255,255,.00));
}
.card h3 { margin: 0 0 8px; font-size: 1rem; }
.muted { color: var(--muted); }
.pill {
  display: inline-block; padding: 3px 10px; border-radius: 999px;
  border: 1px solid var(--line); color: var(--muted); font-size: .8rem;
}
.score-ok { color: var(--ok); }
.score-warn { color: var(--warn); }
a.cta {
  display: inline-block; margin-top: 10px; padding: 8px 11px; border-radius: 8px;
  text-decoration: none; color: #fff; background: linear-gradient(90deg, var(--brand), #2f7fd5);
}
.meta { display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 8px; margin-top: 12px; }
.meta .row { border: 1px solid var(--line); border-radius: 10px; padding: 8px; background: var(--bg-soft); }
.chips { display: flex; flex-wrap: wrap; gap: 6px; margin-top: 8px; }
.chips span { font-size: .8rem; padding: 4px 8px; border: 1px solid var(--line); border-radius: 999px; color: var(--muted); }
.footer { color: var(--muted); margin-top: 24px; font-size: .9rem; }
@media (max-width: 768px) {
  .stats { grid-template-columns: 1fr; }
  .hero h1 { font-size: 1.6rem; }
}
""".strip()
    (ASSETS_PATH / "style.css").write_text(css, encoding="utf-8")


def page_shell(title: str, description: str, body_html: str) -> str:
    safe_title = html.escape(title)
    safe_desc = html.escape(description)
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>{safe_title}</title>
  <meta name="description" content="{safe_desc}" />
  <link rel="stylesheet" href="../assets/style.css" />
</head>
<body>
  <header class="topbar">
    <div class="container nav">
      <a class="brand" href="../index.html">CalabriaTransfers</a>
      <nav class="links">
        <a href="../index.html">Home</a>
        <a href="../towns/index.html">Towns</a>
        <a href="../operators/index.html">Operators</a>
      </nav>
    </div>
  </header>
  <main class="container">
    {body_html}
    <p class="footer">CalabriaTransfers directory - continuously updated.</p>
  </main>
</body>
</html>"""


def page_shell_root(title: str, description: str, body_html: str) -> str:
    safe_title = html.escape(title)
    safe_desc = html.escape(description)
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>{safe_title}</title>
  <meta name="description" content="{safe_desc}" />
  <link rel="stylesheet" href="assets/style.css" />
</head>
<body>
  <header class="topbar">
    <div class="container nav">
      <a class="brand" href="index.html">CalabriaTransfers</a>
      <nav class="links">
        <a href="index.html">Home</a>
        <a href="towns/index.html">Towns</a>
        <a href="operators/index.html">Operators</a>
      </nav>
    </div>
  </header>
  <main class="container">
    {body_html}
    <p class="footer">CalabriaTransfers directory - continuously updated.</p>
  </main>
</body>
</html>"""


def connect_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS operators (
            id TEXT PRIMARY KEY,
            source_key TEXT UNIQUE,
            source_hash TEXT NOT NULL,
            quality_score REAL NOT NULL,
            profile_json TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS queue (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            item_type TEXT NOT NULL,
            priority INTEGER NOT NULL,
            dedupe_key TEXT NOT NULL UNIQUE,
            payload_json TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS checkpoints (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    conn.commit()
    return conn


def write_checkpoint(conn: sqlite3.Connection, key: str, value: str) -> None:
    ts = now_iso()
    conn.execute(
        """
        INSERT INTO checkpoints(key, value, updated_at)
        VALUES(?, ?, ?)
        ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at
        """,
        (key, value, ts),
    )
    conn.commit()


def queue_push(conn: sqlite3.Connection, item: QueueItem, requeue: bool = False) -> None:
    ts = now_iso()
    if requeue:
        conn.execute(
            """
            INSERT INTO queue(item_type, priority, dedupe_key, payload_json, status, created_at, updated_at)
            VALUES(?, ?, ?, ?, 'pending', ?, ?)
            ON CONFLICT(dedupe_key) DO UPDATE SET
                item_type=excluded.item_type,
                priority=excluded.priority,
                payload_json=excluded.payload_json,
                status='pending',
                updated_at=excluded.updated_at
            """,
            (item.item_type, item.priority, item.dedupe_key, json.dumps(item.payload), ts, ts),
        )
    else:
        conn.execute(
            """
            INSERT OR IGNORE INTO queue(item_type, priority, dedupe_key, payload_json, status, created_at, updated_at)
            VALUES(?, ?, ?, ?, 'pending', ?, ?)
            """,
            (item.item_type, item.priority, item.dedupe_key, json.dumps(item.payload), ts, ts),
        )
    conn.commit()


def queue_pop(conn: sqlite3.Connection) -> Optional[sqlite3.Row]:
    row = conn.execute(
        """
        SELECT * FROM queue
        WHERE status='pending'
        ORDER BY priority ASC, id ASC
        LIMIT 1
        """
    ).fetchone()
    if not row:
        return None
    conn.execute(
        "UPDATE queue SET status='processing', updated_at=? WHERE id=?",
        (now_iso(), row["id"]),
    )
    conn.commit()
    return row


def queue_done(conn: sqlite3.Connection, queue_id: int) -> None:
    conn.execute("UPDATE queue SET status='done', updated_at=? WHERE id=?", (now_iso(), queue_id))
    conn.commit()


def fetch_json(url: str) -> List[Dict]:
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=30) as response:
        return json.loads(response.read().decode("utf-8"))


def seed_discovery(conn: sqlite3.Connection) -> None:
    seeds_path = DATA / "discovery" / "seeds.json"
    if not seeds_path.exists():
        seeds = [{"town": t, "query": f"discover transport providers {t} Calabria"} for t in CALABRIA_TOWNS]
        seeds_path.write_text(json.dumps(seeds, ensure_ascii=True, indent=2), encoding="utf-8")
    else:
        seeds = json.loads(seeds_path.read_text(encoding="utf-8"))
    for s in seeds:
        q = QueueItem(
            item_type="DISCOVER_QUERY",
            priority=0,
            payload=s,
            dedupe_key=f"discover:{s['query'].strip().lower()}",
        )
        queue_push(conn, q, requeue=True)


def fetch_overpass_calabria() -> List[Dict]:
    overpass_query = """
[out:json][timeout:60];
area["name"="Calabria"]["boundary"="administrative"]["admin_level"="4"]->.cal;
(
  nwr(area.cal)["amenity"="taxi"]["name"];
  nwr(area.cal)["office"="taxi"]["name"];
  nwr(area.cal)["shop"="car_rental"]["name"];
  nwr(area.cal)["transport"="taxi"]["name"];
);
out center tags 500;
""".strip()
    payload = urllib.parse.urlencode({"data": overpass_query}).encode("utf-8")
    req = urllib.request.Request(
        "https://overpass-api.de/api/interpreter",
        data=payload,
        headers={"User-Agent": USER_AGENT, "Content-Type": "application/x-www-form-urlencoded"},
    )
    with urllib.request.urlopen(req, timeout=90) as response:
        raw = json.loads(response.read().decode("utf-8"))
    return raw.get("elements", [])


def fetch_bing_rss(query: str) -> List[Dict]:
    url = "https://www.bing.com/search?format=rss&q=" + urllib.parse.quote(query)
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(req, timeout=30) as response:
        text = response.read().decode("utf-8", errors="ignore")
    root = ET.fromstring(text)
    out: List[Dict] = []
    for item in root.findall(".//item"):
        title = (item.findtext("title") or "").strip()
        link = (item.findtext("link") or "").strip()
        desc = (item.findtext("description") or "").strip()
        if not link:
            continue
        out.append({"title": title, "link": link, "description": desc})
    return out


def extract_contacts(text: str) -> Dict[str, str]:
    text = text or ""
    email = ""
    phone = ""
    website = ""
    match_email = re.search(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", text)
    if match_email:
        email = match_email.group(0)
    match_phone = re.search(r"(\+?\d[\d\s\-/]{7,}\d)", text)
    if match_phone:
        phone = re.sub(r"\s+", " ", match_phone.group(1)).strip()
    match_website = re.search(r"https?://[^\s,;]+", text)
    if match_website:
        website = match_website.group(0)
    return {"email": email, "phone": phone, "website": website}


def quality_score(profile: Dict) -> float:
    got = 0
    for field in QUALITY_FIELDS:
        v = profile.get(field)
        if isinstance(v, list):
            got += 1 if len(v) > 0 else 0
        else:
            got += 1 if bool(v) else 0
    return round((got / len(QUALITY_FIELDS)) * 100.0, 2)


def profile_priority(score: float) -> int:
    if score < 70:
        return 1
    if score < 90:
        return 2
    return 3


def save_operator_file(profile: Dict) -> None:
    slug = slugify(profile["name"])
    path = DATA / "operators" / f"{slug}.json"
    path.write_text(json.dumps(profile, ensure_ascii=True, indent=2), encoding="utf-8")


def upsert_operator(conn: sqlite3.Connection, source_key: str, profile: Dict) -> bool:
    source_hash = stable_hash(profile)
    exists = conn.execute("SELECT source_hash FROM operators WHERE source_key=?", (source_key,)).fetchone()
    if exists and exists["source_hash"] == source_hash:
        return False
    score = quality_score(profile)
    op_id = hashlib.sha1(source_key.encode("utf-8")).hexdigest()[:16]
    conn.execute(
        """
        INSERT INTO operators(id, source_key, source_hash, quality_score, profile_json, updated_at)
        VALUES(?, ?, ?, ?, ?, ?)
        ON CONFLICT(source_key) DO UPDATE SET
            source_hash=excluded.source_hash,
            quality_score=excluded.quality_score,
            profile_json=excluded.profile_json,
            updated_at=excluded.updated_at
        """,
        (op_id, source_key, source_hash, score, json.dumps(profile), now_iso()),
    )
    conn.commit()
    save_operator_file(profile)
    queue_push(
        conn,
        QueueItem(
            item_type="ENRICH_OPERATOR",
            priority=profile_priority(score),
            payload={"source_key": source_key},
            dedupe_key=f"enrich:{source_key}",
        ),
    )
    queue_push(
        conn,
        QueueItem(
            item_type="GENERATE_OPERATOR_PAGE",
            priority=3,
            payload={"source_key": source_key},
            dedupe_key=f"page:operator:{source_key}",
        ),
    )
    return True


def discover_query(conn: sqlite3.Connection, payload: Dict) -> None:
    query = payload.get("query", "")
    town = payload.get("town", "")
    found = 0
    try:
        items = fetch_overpass_calabria()
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError):
        items = []
    snap = DATA / "snapshots" / f"discover_{hashlib.sha1(query.encode('utf-8')).hexdigest()[:12]}.json"
    snap.write_text(json.dumps(items, ensure_ascii=True, indent=2), encoding="utf-8")
    for item in items:
        tags = item.get("tags", {}) or {}
        contacts_blob = " ".join(
            [
                tags.get("contact:phone", ""),
                tags.get("phone", ""),
                tags.get("contact:email", ""),
                tags.get("email", ""),
                tags.get("contact:website", ""),
                tags.get("website", ""),
            ]
        )
        contacts = extract_contacts(contacts_blob)
        name = tags.get("name", "").strip()
        town = tags.get("addr:city") or tags.get("addr:town") or payload.get("town", "")
        center = item.get("center", {})
        lat = center.get("lat") or item.get("lat")
        lon = center.get("lon") or item.get("lon")
        services = []
        if tags.get("amenity") == "taxi" or tags.get("office") == "taxi" or tags.get("transport") == "taxi":
            services.append("taxi")
        if tags.get("shop") == "car_rental":
            services.append("car rental")
        if not services:
            services = ["private transfer"]
        profile = {
            "name": name,
            "phone": contacts["phone"],
            "email": contacts["email"],
            "whatsapp": contacts["phone"],
            "website": contacts["website"],
            "town": town,
            "languages": ["it"],
            "vehicles": [],
            "photos": [],
            "services": services,
            "coverage": [town] if town else [],
            "source": {
                "provider": "overpass",
                "osm_id": item.get("id"),
                "osm_type": item.get("type"),
                "lat": lat,
                "lon": lon,
                "raw_tags": tags,
            },
        }
        if not profile["name"]:
            continue
        source_key = f"overpass:{item.get('type','x')}:{item.get('id','0')}"
        changed = upsert_operator(conn, source_key, profile)
        if changed:
            found += 1
            queue_push(
                conn,
                QueueItem(
                    item_type="GENERATE_TOWN_PAGE",
                    priority=3,
                    payload={"town": town},
                    dedupe_key=f"page:town:{town.lower()}",
                ),
            )

    if found > 0:
        return

    # Deterministic fallback discovery from Bing RSS results.
    rss_queries = [
        f"NCC {town} Calabria",
        f"taxi {town} Calabria",
        f"transfer aeroporto {town} Calabria",
    ]
    for rq in rss_queries:
        try:
            rss_items = fetch_bing_rss(rq)
        except (urllib.error.URLError, TimeoutError, ET.ParseError):
            continue
        for idx, item in enumerate(rss_items):
            title = item.get("title", "")
            link = item.get("link", "")
            desc = item.get("description", "")
            name = re.split(r"\s[-|]\s", title)[0].strip() if title else ""
            if not name:
                continue
            contacts = extract_contacts(f"{desc} {link}")
            website = contacts["website"] or link
            profile = {
                "name": name,
                "phone": contacts["phone"],
                "email": contacts["email"],
                "whatsapp": contacts["phone"],
                "website": website,
                "town": town,
                "languages": ["it"],
                "vehicles": ["sedan"],
                "photos": [],
                "services": ["private transfer"],
                "coverage": [town] if town else [],
                "source": {
                    "provider": "bing-rss",
                    "query": rq,
                    "rank": idx + 1,
                    "title": title,
                    "url": link,
                },
            }
            source_key = f"bing:{hashlib.sha1((rq + '|' + link).encode('utf-8')).hexdigest()[:20]}"
            changed = upsert_operator(conn, source_key, profile)
            if changed:
                queue_push(
                    conn,
                    QueueItem(
                        item_type="GENERATE_TOWN_PAGE",
                        priority=3,
                        payload={"town": town},
                        dedupe_key=f"page:town:{town.lower()}",
                    ),
                )


def enrich_operator(conn: sqlite3.Connection, payload: Dict) -> None:
    source_key = payload["source_key"]
    row = conn.execute("SELECT profile_json FROM operators WHERE source_key=?", (source_key,)).fetchone()
    if not row:
        return
    profile = json.loads(row["profile_json"])
    name = profile.get("name", "")
    website = profile.get("website", "")
    if not website and name:
        search_hint = slugify(name).replace("-", "")
        profile["website"] = f"https://www.google.com/search?q={urllib.parse.quote(search_hint)}"
    if not profile.get("vehicles"):
        profile["vehicles"] = ["sedan"]
    if not profile.get("languages"):
        profile["languages"] = ["it"]
    if not profile.get("services"):
        profile["services"] = ["private transfer"]
    upsert_operator(conn, source_key, profile)


def generate_operator_page(conn: sqlite3.Connection, payload: Dict) -> None:
    source_key = payload["source_key"]
    row = conn.execute("SELECT profile_json, quality_score FROM operators WHERE source_key=?", (source_key,)).fetchone()
    if not row:
        return
    profile = json.loads(row["profile_json"])
    score = row["quality_score"]
    if score < 50:
        return
    slug = slugify(profile["name"])
    page = SITE_PATH / "operators" / f"{slug}.html"
    name = html.escape(profile["name"])
    town = html.escape(profile.get("town", ""))
    phone = html.escape(profile.get("phone", ""))
    email = html.escape(profile.get("email", ""))
    whatsapp = html.escape(profile.get("whatsapp", ""))
    website = html.escape(profile.get("website", ""))
    languages = ", ".join(profile.get("languages", []))
    vehicles = ", ".join(profile.get("vehicles", []))
    services = ", ".join(profile.get("services", []))
    coverage = ", ".join(profile.get("coverage", []))
    score_cls = "score-ok" if score >= 90 else "score-warn"
    website_cta = f'<a class="cta" href="{website}" target="_blank" rel="noopener">Visit website</a>' if website else ""
    body = f"""
    <section class="hero">
      <span class="pill">Operator profile</span>
      <h1>{name}</h1>
      <p class="muted">Private transfer and taxi services in Calabria.</p>
      <div class="stats"><div class="stat"><div class="v {score_cls}">{score}%</div><div class="muted">Completeness</div></div></div>
      {website_cta}
    </section>
    <section class="section">
      <h2>Details</h2>
      <div class="meta">
        <div class="row"><strong>Town</strong><br>{town or "-"}</div>
        <div class="row"><strong>Phone</strong><br>{phone or "-"}</div>
        <div class="row"><strong>Email</strong><br>{email or "-"}</div>
        <div class="row"><strong>WhatsApp</strong><br>{whatsapp or "-"}</div>
      </div>
      <div class="chips">
        <span>Languages: {html.escape(languages or "-")}</span>
        <span>Vehicles: {html.escape(vehicles or "-")}</span>
        <span>Services: {html.escape(services or "-")}</span>
        <span>Coverage: {html.escape(coverage or "-")}</span>
      </div>
    </section>
    """
    content = page_shell(f"{profile['name']} | CalabriaTransfers", f"Transport profile for {profile['name']} in Calabria.", body)
    page.write_text(content, encoding="utf-8")


def generate_town_page(conn: sqlite3.Connection, payload: Dict) -> None:
    town = (payload.get("town") or "").strip()
    if not town:
        return
    rows = conn.execute(
        "SELECT profile_json, quality_score FROM operators WHERE json_extract(profile_json, '$.town')=? ORDER BY quality_score DESC",
        (town,),
    ).fetchall()
    if len(rows) < 3:
        return
    items = []
    for r in rows[:50]:
        p = json.loads(r["profile_json"])
        slug = slugify(p["name"])
        n = html.escape(p["name"])
        score = r["quality_score"]
        score_cls = "score-ok" if score >= 90 else "score-warn"
        items.append(
            f'<article class="card"><h3>{n}</h3><p class="muted">{html.escape(town)}</p><p class="{score_cls}"><strong>{score}% complete</strong></p><a class="cta" href="../operators/{slug}.html">View profile</a></article>'
        )
    page = SITE_PATH / "towns" / f"{slugify(town)}.html"
    body = f"""
    <section class="hero">
      <span class="pill">Town directory</span>
      <h1>Transport Operators in {html.escape(town)}</h1>
      <p>Curated transfer and NCC options with quality scoring.</p>
      <div class="stats"><div class="stat"><div class="v">{len(rows)}</div><div class="muted">Operators listed</div></div></div>
    </section>
    <section class="section">
      <h2>Operator list</h2>
      <div class="grid">{"".join(items)}</div>
    </section>
    """
    content = page_shell(
        f"{town} Transport Directory | CalabriaTransfers",
        f"Best transport operators in {town}, Calabria.",
        body,
    )
    page.write_text(content, encoding="utf-8")


def generate_homepage(conn: sqlite3.Connection) -> None:
    write_theme_assets()
    operator_count = int(conn.execute("SELECT COUNT(*) AS c FROM operators").fetchone()["c"])
    town_files = sorted((SITE_PATH / "towns").glob("*.html"))
    operator_files = sorted((SITE_PATH / "operators").glob("*.html"))
    latest_ops = operator_files[-25:]
    town_cards = "".join(
        [
            f'<article class="card"><h3>{html.escape(p.stem.replace("-", " ").title())}</h3><a class="cta" href="towns/{p.name}">Explore town</a></article>'
            for p in town_files
        ]
    )
    op_cards = "".join(
        [
            f'<article class="card"><h3>{html.escape(p.stem.replace("-", " ").title())}</h3><a class="cta" href="operators/{p.name}">View profile</a></article>'
            for p in latest_ops
        ]
    )
    operators_index = "".join(
        [f'<li><a href="{p.name}">{html.escape(p.stem.replace("-", " ").title())}</a></li>' for p in operator_files]
    )
    towns_index = "".join([f'<li><a href="{p.name}">{html.escape(p.stem.replace("-", " ").title())}</a></li>' for p in town_files])
    home_body = f"""
    <section class="hero">
      <span class="pill">Calabria master directory</span>
      <h1>Premium Transport Directory for Calabria</h1>
      <p>Airport transfers, NCC services, private chauffeurs, and town coverage in one trusted source.</p>
      <div class="stats">
        <div class="stat"><div class="v">{operator_count}</div><div class="muted">Operators indexed</div></div>
        <div class="stat"><div class="v">{len(town_files)}</div><div class="muted">Town hubs</div></div>
        <div class="stat"><div class="v">24/7</div><div class="muted">Automated refresh loop</div></div>
      </div>
    </section>
    <section class="section"><h2>Browse by town</h2><div class="grid">{town_cards or '<article class="card"><p class="muted">Town pages are being generated.</p></article>'}</div></section>
    <section class="section"><h2>Latest operators</h2><div class="grid">{op_cards or '<article class="card"><p class="muted">Operator pages are being generated.</p></article>'}</div></section>
    """
    (SITE_PATH / "index.html").write_text(
        page_shell_root("CalabriaTransfers | Transport Directory", "The authoritative Calabria transport directory.", home_body),
        encoding="utf-8",
    )
    (SITE_PATH / "operators" / "index.html").write_text(
        page_shell(
            "Operators | CalabriaTransfers",
            "All operator profiles in Calabria.",
            f'<section class="hero"><h1>All Operators</h1><p class="muted">Complete directory index.</p></section><section class="section"><ul>{operators_index}</ul></section>',
        ),
        encoding="utf-8",
    )
    (SITE_PATH / "towns" / "index.html").write_text(
        page_shell(
            "Towns | CalabriaTransfers",
            "Town transport directory pages in Calabria.",
            f'<section class="hero"><h1>Town Pages</h1><p class="muted">Coverage hubs.</p></section><section class="section"><ul>{towns_index}</ul></section>',
        ),
        encoding="utf-8",
    )


def recover_stuck_queue(conn: sqlite3.Connection) -> int:
    ts = now_iso()
    cur = conn.execute(
        "UPDATE queue SET status='pending', updated_at=? WHERE status='processing'",
        (ts,),
    )
    conn.commit()
    return cur.rowcount


def queue_size(conn: sqlite3.Connection) -> int:
    return int(conn.execute("SELECT COUNT(*) AS c FROM queue WHERE status='pending'").fetchone()["c"])


def process_one(conn: sqlite3.Connection) -> None:
    item = queue_pop(conn)
    if not item:
        seed_discovery(conn)
        item = queue_pop(conn)
        if not item:
            return
    payload = json.loads(item["payload_json"])
    item_type = item["item_type"]
    if item_type == "DISCOVER_QUERY":
        discover_query(conn, payload)
    elif item_type == "ENRICH_OPERATOR":
        enrich_operator(conn, payload)
    elif item_type == "GENERATE_OPERATOR_PAGE":
        generate_operator_page(conn, payload)
    elif item_type == "GENERATE_TOWN_PAGE":
        generate_town_page(conn, payload)
    queue_done(conn, item["id"])
    write_checkpoint(conn, "last_processed_item", f"{item_type}:{item['id']}")
    write_checkpoint(conn, "last_cycle", now_iso())
    write_checkpoint(conn, "queue_pending", str(queue_size(conn)))
    generate_homepage(conn)
    print(f"processed={item_type} queue_pending={queue_size(conn)}", flush=True)


def loop_forever() -> None:
    ensure_dirs()
    conn = connect_db()
    stuck = recover_stuck_queue(conn)
    if stuck:
        print(f"recovered_stuck_queue={stuck}", flush=True)
    while True:
        for _ in range(MAX_TASKS_PER_CYCLE):
            before = queue_size(conn)
            process_one(conn)
            after = queue_size(conn)
            if before == 0 and after == 0:
                break
        if queue_size(conn) > 0:
            time.sleep(1)
        else:
            time.sleep(CYCLE_SECONDS)


if __name__ == "__main__":
    loop_forever()
