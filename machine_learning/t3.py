import requests, sqlite3, time

API = "https://en.wikipedia.org/w/api.php"
HEADERS = {"User-Agent": "DE-class-project/0.1 (your_email@example.com)"}

con = sqlite3.connect("wiki.db")
con.execute("""CREATE TABLE IF NOT EXISTS titles (
    title TEXT PRIMARY KEY, is_hoax INTEGER,
    exists_now INTEGER, source TEXT)""")

# 1. Hoax candidates: all article-space links on the hoax list page
r = requests.get(API, headers=HEADERS, params={
    "action": "parse", "page": "Wikipedia:List of hoaxes on Wikipedia",
    "prop": "links", "format": "json", "formatversion": 2,
}).json()

hoaxes = {l["title"]: l["exists"] for l in r["parse"]["links"] if l["ns"] == 0}
print(f"Hoax candidates: {len(hoaxes)} "
      f"({sum(hoaxes.values())} still exist, {len(hoaxes) - sum(hoaxes.values())} deleted)")

for title, exists in hoaxes.items():
    con.execute("INSERT OR IGNORE INTO titles VALUES (?,?,?,?)",
                (title, 1, int(exists), "hoax_list_page"))

# 2. Controls: random articles (500 max per request)
n_controls = len(hoaxes) * 3          # 3 controls per hoax, adjust as needed
controls = set()
while len(controls) < n_controls:
    r = requests.get(API, headers=HEADERS, params={
        "action": "query", "list": "random", "rnnamespace": 0,
        "rnlimit": 500, "format": "json", "formatversion": 2,
    }).json()
    controls.update(p["title"] for p in r["query"]["random"])
    time.sleep(0.5)

for title in list(controls)[:n_controls]:
    con.execute("INSERT OR IGNORE INTO titles VALUES (?,?,?,?)",
                (title, 0, 1, "random"))

con.commit()
print(con.execute("SELECT is_hoax, exists_now, COUNT(*) FROM titles "
                  "GROUP BY 1, 2").fetchall())