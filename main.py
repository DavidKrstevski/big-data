import mongo, scraper_willhaben, scraper_immowelt
if __name__ == "__main__":
    items = scraper_willhaben.scrape_pages(pages=20)
    print(f"Scraped {len(items)} items")

    stats = mongo.save_items_to_mongo(items, "willhaben")
    print("Mongo stats:", stats)

    items = scraper_immowelt.scrape_pages(pages=20)
    print(f"Scraped {len(items)} items")

    stats = mongo.save_items_to_mongo(items, "immowelt")
    print("Mongo stats:", stats)
