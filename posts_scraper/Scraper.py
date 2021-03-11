from posts_scraper.api_access.pushshift_wrapper import PushshiftScraper
from posts_scraper.api_access.twitter_api_wrapper import TwitterScraper
import sqlite3


def scrape():
    ps = PushshiftScraper(queries=["invisalign"], days=30, interval=6)  # 10, 6
    ts = TwitterScraper("invisalign").results_to_dataframe(max_tweets=5000)

    return ps.data, ts


def cache(ps, ts):
    con = sqlite3.connect("cache2.db")
    ps.to_sql(name="reddit_data", con=con, if_exists="append")
    ts.to_sql(name="twitter_data", con=con, if_exists="append")
    con.close()

if __name__ == '__main__':
    ps, ts = scrape()
    cache(ps, ts)