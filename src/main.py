from omegaconf import OmegaConf
from util import setup
import json
from datetime import datetime

from fetch.Fetchers import Fetcher


def main():
    """
    Fetch HTML from a given list of URLs 
    """
    
    config = setup("config/config.yaml")

    print("Config:")
    print(OmegaConf.to_yaml(config))

    urls = [
        "https://example.com",
        # "https://www.cbs.nl/nl-nl/sitemaps/jobsitemap",
        'https://www.cbs.nl/nl-nl/vacature/economisch-onderzoeker/4920ff438ef940fdaffa5dec7a8c94a2',
        'https://www.cbs.nl/nl-nl/vacature/economisch-analist-grote-ondernemingen/f7ab83e489d4428f8291d02947c2f13a',
        'https://www.cbs.nl/nl-nl/vacature/software-ontwikkelaar/1ae1a790f9284748aca2b34da8cae607',
        'https://www.cbs.nl/nl-nl/vacature/onderzoeker-doodsoorzakenstatistiek/e1effbdcc57f47579a530beb5b39ffb6',
        'https://www.cbs.nl/nl-nl/vacature/financieel-analist/2bb9b2473bcd43f481969dba5f298400'
    ]

    fetcher = Fetcher()
    for _url in urls:
        fetcher.fetch(url=_url)
    data = fetcher.get_results()

    filepath = f"{config.output.output_dir}/{datetime.now().strftime('%Y%m%d_%H%M%S')}fetched.jsonl"
    with open(filepath, 'w', encoding='utf-8') as file_out:
        for _url, _html in data.items():
            file_out.write(json.dumps({'URL': _url, 'HTML': _html}))


if __name__ == "__main__":
    main()
