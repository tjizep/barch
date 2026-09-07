#!/usr/bin/env python3
"""Turn the amazon-products.csv scrape into what the shop loads.

Two outputs, because the example shows both ways of holding a catalog:

  build/catalog/<Top>/<Sub>/<asin>.json   the file store tree, one file per
                                          product, the directories being the
                                          category tree
  build/index.json                        one compact summary per product, so the
                                          front page is one read rather than a
                                          thousand

Nothing here talks to barch: setup.sh does that with LOADFS and FS PUT.
"""
import csv, json, os, re, shutil, sys

csv.field_size_limit(10_000_000)

HERE = os.path.dirname(os.path.abspath(__file__))
SRC = os.path.join(HERE, "..", "shopping", "amazon-products.csv")
OUT = os.path.join(HERE, "build")


def safe(name):
    """a category as a directory name.

    Slugged, with no spaces in it, and that is not only tidiness: a key with a
    space in it is invisible to a range scan (TODO 260), and an fs listing is a
    range scan - so a directory called `Home & Kitchen` stores fine and cannot be
    listed. The display name is kept in the record and in categories.json.
    """
    slug = re.sub(r"[^a-z0-9]+", "-", (name or "").lower()).strip("-")
    return slug[:60] or "other"


def price(row):
    """final_price is a quoted string and initial_price is often the text null"""
    raw = (row.get("final_price") or "").strip().strip('"')
    try:
        return round(float(raw.replace(",", "")), 2)
    except ValueError:
        return None


def number(v, default=0):
    try:
        return float(str(v).strip().strip('"').replace(",", ""))
    except (TypeError, ValueError):
        return default


def main():
    if not os.path.exists(SRC):
        sys.exit("no %s" % SRC)
    rows = list(csv.DictReader(open(SRC, newline="", encoding="utf-8", errors="replace")))
    shutil.rmtree(OUT, ignore_errors=True)
    os.makedirs(OUT)

    index, written, skipped = [], 0, 0
    names = {}                       # slug -> the name to show
    for row in rows:
        asin = (row.get("asin") or "").strip()
        title = (row.get("title") or "").strip()
        image = (row.get("image_url") or "").strip()
        cost = price(row)
        # the same inclusion rule the whole way through: without these four there
        # is no product to show, and a card with a hole in it is worse than absent
        if not (asin and title and image.startswith("http") and cost):
            skipped += 1
            continue
        try:
            cats = [safe(c) for c in json.loads(row.get("categories") or "[]")]
        except json.JSONDecodeError:
            cats = []
        try:
            shown = json.loads(row.get("categories") or "[]") or ["Other"]
        except json.JSONDecodeError:
            shown = ["Other"]
        cats = cats or ["other"]
        top, sub = cats[0], (cats[1] if len(cats) > 1 else "general")
        names[top] = shown[0]
        names[sub] = shown[1] if len(shown) > 1 else "General"

        was = number(row.get("initial_price"), 0)
        record = {
            "asin": asin,
            "title": title,
            "brand": (row.get("brand") or "").strip(),
            "description": (row.get("description") or "").strip()[:1500],
            "price": cost,
            "was": round(was, 2) if was > cost else None,
            "currency": (row.get("currency") or "USD").strip(),
            "rating": number(row.get("rating")),
            "reviews": int(number(row.get("reviews_count"))),
            "availability": (row.get("availability") or "").strip(),
            "image_url": image,
            "categories": shown,
            "seller": (row.get("seller_name") or row.get("manufacturer") or "").strip(),
            "url": (row.get("url") or "").strip(),
        }
        where = os.path.join(OUT, "catalog", top, sub)
        os.makedirs(where, exist_ok=True)
        with open(os.path.join(where, asin + ".json"), "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False)
        written += 1
        index.append({"asin": asin, "title": title, "brand": record["brand"],
                      "price": cost, "was": record["was"], "currency": record["currency"],
                      "rating": record["rating"], "reviews": record["reviews"],
                      "image_url": image, "top": top, "sub": sub})

    index.sort(key=lambda p: (p["top"], p["sub"], p["title"]))
    # both go in through LOADFS with everything else: a 400KB value does not want
    # to travel as a shell argument
    os.makedirs(os.path.join(OUT, "meta"), exist_ok=True)
    with open(os.path.join(OUT, "meta", "index.json"), "w", encoding="utf-8") as f:
        json.dump(index, f, ensure_ascii=False)
    with open(os.path.join(OUT, "meta", "names.json"), "w", encoding="utf-8") as f:
        json.dump(names, f, ensure_ascii=False)

    tops = {}
    for p in index:
        tops.setdefault(p["top"], set()).add(p["sub"])
    print("%d products in %d top categories, %d skipped for missing "
          "asin/title/image/price" % (written, len(tops), skipped))
    print("index.json is %d KB" % (os.path.getsize(os.path.join(OUT, "meta", "index.json")) // 1024))


if __name__ == "__main__":
    main()
