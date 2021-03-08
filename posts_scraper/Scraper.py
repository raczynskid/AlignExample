from posts_scraper.api_access.pushshift_wrapper import PushshiftScraper

ps = PushshiftScraper(queries=["invisalign"], days=10, interval=6)
print(ps.data)