from posts_scraper import Scraper


if __name__ == '__main__':
    ps, ts = Scraper.scrape()
    Scraper.cache(ps, ts)
