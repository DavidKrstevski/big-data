import time
import random
import requests
import re
from bs4 import BeautifulSoup
from urllib.parse import urljoin

BASE = "https://www.immowelt.at/suche/wien/wohnungen/mieten?d=true&sd=DESC&sf=TIMESTAMP"
HEADERS = {"User-Agent": "Mozilla/5.0 (EducationalScraper/1.0)"}
SITE = "https://www.immowelt.at/"
def fetch(url: str) -> str:
    r = requests.get(url, headers=HEADERS, timeout=20)
    r.raise_for_status()
    # Rate limit: vermeidet Überlastung & Blocks
    time.sleep(random.uniform(0.8, 1.8))
    return r.text

def parse_list_page(html: str) -> list[str]:
    soup = BeautifulSoup(html, "lxml")

    links = []
    for a in soup.select('a[href^="https://www.immowelt.at/projekte/expose/"]'):
        links.append(urljoin(SITE, a["href"]))

    # dedupe, Reihenfolge behalten
    return list(dict.fromkeys(links))

def get_attribute(soup, wanted_title: str):
    wanted = wanted_title.casefold()

    for li in soup.select('li[data-testid="attribute-item"]'): # ngcontent-serverapp-c167
        title_el = li.select_one('[data-testid="attribute-title"]')
        value_el = li.select_one('[data-testid="attribute-value"]')
        if not title_el or not value_el:
            continue

        title = title_el.get_text(" ", strip=True).casefold()
        if title != wanted:
            continue

        # 1) normaler Text-Wert (z.B. "81,76 m²", "3")
        text = value_el.get_text(" ", strip=True)
        if text:
            return text

        # 2) kein Text -> z.B. Häkchen-Icon => True
        # meistens steckt dann ein <svg> oder ähnliches drin
        if value_el.select_one("svg"):
            return True

        # 3) sonst: vorhanden aber leer
        return ""

    return None

# Übresicht
def get_hardfact(soup, wanted_label: str):
    wanted = wanted_label.casefold().strip()
    for hf in soup.select("app-hardfacts .hardfact"):
        label_el = hf.select_one(".hardfact__label")
        if not label_el:
            continue
        label = label_el.get_text(" ", strip=True).casefold()
        if wanted not in label:
            continue
        val_el = hf.select_one("strong, span.has-font-300")
        return val_el.get_text(" ", strip=True) if val_el else None
    return None

# Adresse
def get_address(soup):
    street_el = soup.select_one('[data-cy="address-street"]')
    city_el = soup.select_one('[data-cy="address-city"]')

    street = street_el.get_text(" ", strip=True) if street_el else ""
    city = city_el.get_text(" ", strip=True) if city_el else ""

    return " ".join(x for x in [street, city] if x).strip()

# Details 
def get_equipment_value(soup, wanted_label: str):
    wanted = wanted_label.casefold().strip()
    for cell in soup.select("sd-card .equipment sd-cell-col"):
        ps = cell.select("p")
        if len(ps) < 2:
            continue
        label = ps[0].get_text(" ", strip=True).casefold()
        value = ps[1].get_text(" ", strip=True)
        if label == wanted:
            return value
    return None

# Details key-value
def get_list_kv(soup, wanted_label: str):
    wanted = wanted_label.casefold().strip()
    for li in soup.select("sd-card .textlist li"):
        key_el = li.select_one("span.color-grey-500")
        if not key_el:
            continue
        key = key_el.get_text(" ", strip=True).rstrip(":").casefold()
        if key != wanted:
            continue
        full = li.get_text(" ", strip=True)
        key_txt = key_el.get_text(" ", strip=True)
        value = full.replace(key_txt, "", 1).strip()
        return value or None
    return None

def has_feature(soup, needle: str) -> bool:
    n = needle.casefold().strip()
    for li in soup.select("sd-card .textlist li"):
        text = li.get_text(" ", strip=True).casefold()
        if n in text:
            return True
    return False

# Nimmt alle Texte aus dem Details-Block
def get_details_text(soup) -> str:
    parts = []
    for li in soup.select("sd-card .textlist li"):
        t = li.get_text(" ", strip=True)
        if t:
            parts.append(t)
    return "\n".join(parts)

def extract_features_from_details(details_text: str) -> dict:
    t = (details_text or "").casefold()

    def has_any(*needles):
        return any(n.casefold() in t for n in needles)

    out = {}

    out["balkon"] = has_any("balkon", "loggia")
    out["terrasse"] = has_any("terrasse")
    out["fahrstuhl"] = has_any("personenaufzug", "aufzug", "lift")
    out["einbauküche"] = has_any("einbauküche")
    out["keller"] = has_any("keller")
    out["haustiere_erlaubt"] = has_any("haustiere erlaubt")

    # "Ausstattung: teilweise möbliert" / "möbliert"
    out["teilmöbliert_/_möbliert"] = has_any("möbliert", "teilweise möbliert", "teilmöbliert")
    m = re.search(r"böden:\s*([^\n\r]+)", details_text, flags=re.IGNORECASE)
    out["böden"] = m.group(1).strip() if m else None
    m = re.search(r"zustand:\s*([^\n\r]+)", details_text, flags=re.IGNORECASE)
    out["zustand"] = m.group(1).strip() if m else None
    m = re.search(r"baujahr:\s*(\d{4})", details_text, flags=re.IGNORECASE)
    out["baujahr"] = int(m.group(1)) if m else None
    if (has_any("neubau", "Neubau")):
        out["bautyp"] = "Neubau"
    elif (has_any("altbau")):
        out["bautyp"] = "Altbau"

    # manchmal kommt zustand doppelt vor
    zustand = re.findall(r"zustand:\s*([^\n\r]+)", details_text, flags=re.IGNORECASE)

    out["zustand"] = None
    out["bautyp"] = None

    for v in [x.strip() for x in zustand]:
        vl = v.casefold()
        if vl in ("altbau", "neubau"):
            out["bautyp"] = v
        else:
            if out["zustand"] is None:
                out["zustand"] = v
        vl = v.casefold()
        if vl in ("altbau", "neubau"):
            out["bautyp"] = v
        else:
            if out["zustand"] is None:
                out["zustand"] = v

    return out

def extract_garage_parkplatz(details_text: str) -> dict:
    t = (details_text or "").casefold()

    garage = ("garage" in t) or ("tiefgarage" in t)

    parkplatz = (
        ("stellplatz" in t) or
        ("parkplatz" in t) or
        ("carport" in t)
    )

    return {"garage": garage, "parkplatz": parkplatz}

def parse_detail_page(html: str, url: str) -> dict:
    soup = BeautifulSoup(html, "lxml")

    title_el = soup.select_one("app-objectmeta h1")
    title = title_el.get_text(" ", strip=True) if title_el else ""

    preis = get_hardfact(soup, "Gesamtmiete")
    area = get_hardfact(soup, "Wohnfläche")
    rooms = get_hardfact(soup, "Zimmer")
    address = get_address(soup)

    stockwerk = get_equipment_value(soup, "Wohnungslage")
    bezug = get_equipment_value(soup, "Bezug")

    details_text = get_details_text(soup)
    feats = extract_features_from_details(details_text)

    garage_parkplatz = extract_garage_parkplatz(details_text)

    return {
        "url": url,
        "titel": title,
        "preis": preis,
        "wohnfläche": area,
        "zimmer": rooms,
        "address": address,
        "stockwerk": stockwerk,
        "verfügbar": bezug,
        "garage": garage_parkplatz["garage"],
        "parkplatz": garage_parkplatz["parkplatz"],
        "details": details_text,
        **feats,
    }

def scrape_pages(pages: int = 2) -> list[dict]:
    all_items = []
    seen = set()

    for page in range(1, pages + 1):
        list_url = f"{BASE}&sp={page}"
        list_html = fetch(list_url)

        detail_urls = parse_list_page(list_html)
        for durl in detail_urls:
            if durl in seen:
                continue
            seen.add(durl)

            detail_html = fetch(durl)
            item = parse_detail_page(detail_html, durl)
            all_items.append(item)

    return all_items

if __name__ == "__main__":
    items = scrape_pages(pages=1)
    print(f"Scraped {len(items)} items")
    if items:
        print(items[0])