from posts_scraper.api_access.pushshift_wrapper import PushshiftScraper
from posts_scraper.api_access.twitter_api_wrapper import TwitterScraper

ps = PushshiftScraper(queries=["invisalign"], days=10, interval=6)
ts = TwitterScraper("invisalign").results_to_dataframe()
