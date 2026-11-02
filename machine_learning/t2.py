import requests, sqlite3, time

API = "https://en.wikipedia.org/w/api.php"
HEADERS = {"User-Agent": "DE-class-project/0.1 (your_email@example.com)"}

def fetch_revisions(title):
    params = {
        "action": "query", "format": "json", "prop": "revisions",
        "titles": title, "rvlimit": 500,
        "rvprop": "ids|timestamp|user|userid|comment|size|tags",
    }
    while True:
        resp = requests.get(API, params=params, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        for page in data["query"]["pages"].values():
            for rev in page.get("revisions", []):
                yield rev
        if "continue" not in data:
            break
        params.update(data["continue"])

con = sqlite3.connect("wiki.db")
con.execute("""CREATE TABLE IF NOT EXISTS revisions (
    revid INTEGER PRIMARY KEY, article TEXT, is_hoax INTEGER,
    timestamp TEXT, user TEXT, is_anon INTEGER,
    size INTEGER, comment TEXT, tags TEXT)""")

titles = con.execute("""
    SELECT title, is_hoax FROM titles
    WHERE exists_now = 1
      AND title NOT IN (SELECT DISTINCT article FROM revisions)
""").fetchall()
print(f"{len(titles)} articles to fetch")

start_i = len(titles)
max_rev = 10

for i, (title, label) in enumerate(titles, 1):
    start_i -= 1
    rev = 0
    print(f"{start_i} articles to fetch")
    try:
        for r in fetch_revisions(title):
            rev += 1
            if rev >= max_rev:
                break
            con.execute("INSERT OR IGNORE INTO revisions VALUES (?,?,?,?,?,?,?,?,?)",
                (r["revid"], title, label, r["timestamp"], r.get("user"),
                 int(r.get("userid") == 0),
                 r.get("size"), r.get("comment"), ",".join(r.get("tags", []))))
        con.commit()
    except Exception as e:
        con.rollback()
        print(f"Failed on {title!r}: {e}")
    print(f"[{i}/{len(titles)}] {title}")