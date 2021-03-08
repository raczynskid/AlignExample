from searchtweets import ResultStream, gen_request_parameters, load_credentials
import pandas as pd


class TwitterScraper:
    def __init__(self, query: str, limit: int = 1000):
        self.limit_results = limit

        self.search_args = load_credentials(r"D:\Python\Align\posts_scraper\api_access\twitter_keys.yaml",
                                            yaml_key="search_tweets_v2",
                                            env_overwrite=False)

        self.query = gen_request_parameters(query, results_per_call=100,
                                            tweet_fields="created_at,lang,public_metrics"
                                            )

    def results_to_dataframe(self, max_pages: int = 10) -> pd.DataFrame:
        """returns generator for tweets"""
        rs = ResultStream(request_parameters=self.query,
                          max_results=1000,
                          max_pages=max_pages,
                          max_tweets = 1000,
                          **self.search_args)

        return pd.DataFrame.from_dict(list(rs.stream()))[['public_metrics', 'text',
                                                          'lang', 'id', 'created_at', 'newest_id']]

