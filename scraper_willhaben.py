import time
import random
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin

BASE = "https://www.willhaben.at/iad/immobilien/mietwohnungen/wien"
HEADERS = {"User-Agent": "Mozilla/5.0 (EducationalScraper/1.0)"}
SITE = "https://www.willhaben.at"

def fetch(url: str) -> str:
    r = requests.get(url, headers=HEADERS, timeout=20)
    r.raise_for_status()
    # Rate limit: vermeidet Überlastung & Blocks
    time.sleep(random.uniform(0.8, 1.8))
    return r.text

def parse_list_page(html: str) -> list[str]:
    soup = BeautifulSoup(html, "lxml")

    links = []
    for a in soup.select('a[href^="/iad/immobilien/"][data-testid^="search-result-entry-header-"]'):
        links.append(urljoin(SITE, a["href"]))

    # dedupe, Reihenfolge behalten
    return list(dict.fromkeys(links))

def get_attribute(soup, wanted_title: str):
    wanted = wanted_title.casefold()

    for li in soup.select('li[data-testid="attribute-item"]'):
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

def get_description_block(soup, name: str) -> str:
    el = soup.select_one(f'div[data-testid="ad-description-{name}"]')
    return el.get_text("\n", strip=True) if el else ""

def parse_detail_page(html: str, url: str) -> dict:
    soup = BeautifulSoup(html, "lxml")
    title_el = soup.select_one("h1")
    title = title_el.get_text(strip=True) if title_el else ""

    price_el = soup.select_one('span[data-testid^="contact-box-price-box-price-value-"]')
    price = price_el.get_text(strip=True) if price_el else ""

    address_el = soup.select_one('div[data-testid^="object-location-address"]')
    address = address_el.get_text(strip=True) if address_el else ""

    return {
        "url": url,
        "titel": title,
        "preis": price,
        "address": address,
        "objekttyp": get_attribute(soup, "Objekttyp"),
        "bautyp": get_attribute(soup, "Bautyp"),
        "zustand": get_attribute(soup, "Zustand"),
        "wohnfläche": get_attribute(soup, "Wohnfläche"),
        "grundfläche": get_attribute(soup, "Grundfläche"),
        "zimmer": get_attribute(soup, "Zimmer"),
        "stockwerk": get_attribute(soup, "Stockwerk(e)"),
        "böden": get_attribute(soup, "Böden"),
        "verfügbar": get_attribute(soup, "Verfügbar"),
        "befristung": get_attribute(soup, "Befristung"),
        "heizung": get_attribute(soup, "Heizung"),
        "einbauküche": get_attribute(soup, "Einbauküche"),
        "keller": get_attribute(soup, "Keller"),
        "abstellraum": get_attribute(soup, "Abstellraum"),
        "garage": get_attribute(soup, "Garage"),
        "carport": get_attribute(soup, "Carport"),
        "barrierefrei": get_attribute(soup, "Barrierefrei"),
        "fahrstuhl": get_attribute(soup, "Fahrstuhl"),
        "parkplatz": get_attribute(soup, "Parkplatz"),
        "balkon": get_attribute(soup, "Balkon"),
        "terrasse": get_attribute(soup, "Terrasse"),
        "teilmöbliert_/_möbliert": get_attribute(soup, "Teilmöbliert / Möbliert"),
        "objektbeschreibung": get_description_block(soup, "Objektbeschreibung"),
        "lage": get_description_block(soup, "Lage"),
        "ausstattung": get_description_block(soup, "Ausstattung"),
        "preis_und_detailinformation": get_description_block(soup, "Preis und Detailinformation"),
        "zusatzinformationen": get_description_block(soup, "Zusatzinformationen"),
        "sonstiges": get_description_block(soup, "Sonstiges"),
        "energieausweis_heizung": get_description_block(soup, "Energieausweis/Heizung")
        
    }

def scrape_pages(pages: int = 2) -> list[dict]:
    all_items = []
    seen = set()
    counter = 0
    for page in range(1, pages + 1):
        list_url = f"{BASE}?page={page}"
        list_html = fetch(list_url)

        detail_urls = parse_list_page(list_html)
        for durl in detail_urls:
            if durl in seen:
                continue
            seen.add(durl)

            detail_html = fetch(durl)
            item = parse_detail_page(detail_html, durl)
            all_items.append(item)
        counter = counter + 1
        print(f"Scraped pages: {counter}")
    return all_items

if __name__ == "__main__":
    items = scrape_pages(pages=1)
    print(f"Scraped {len(items)} items")
    if items:
        print(items[0])