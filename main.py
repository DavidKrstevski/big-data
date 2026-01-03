import mongo, scraper
if __name__ == "__main__":
    items = scraper.scrape_pages(pages=10)
    print(f"Scraped {len(items)} items")

    stats = mongo.save_items_to_mongo(items)
    print("Mongo stats:", stats)
