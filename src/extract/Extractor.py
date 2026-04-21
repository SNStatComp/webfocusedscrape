import logging
from abc import ABC, abstractmethod
from bs4 import BeautifulSoup


class IExtractor(ABC):
    """
    Interface class for Extractor
    """

    @abstractmethod
    def extract(self, html: str) -> str:
        raise NotImplementedError("Do not call abstract base class.")


class EmptystringExtractor(IExtractor):
    """
    Testing Extractor that just returns an empty string
    """
    def __init__(self):
        logging.info("This testing extractor just returns an empty string for any given html")
        pass

    def extract(self, html: str) -> str:
        return ''


class MainContentExtractor(IExtractor):
    """
    Extract the main human-readable text from a web page
    using BeautifulSoup.
    Accepts raw HTML as input.
    """
    def __init__(self):
        logging.info("Initializing extractor that will look for main content in html using BeautifulSoup")
        self._disregard = ["script", "style", "nav", "footer", "header", "aside"]
        logging.debug(f"Extractor disregards tags: {', '.join(self._disregard)}")

    def extract(self, html: str) -> str:
        soup = BeautifulSoup(html, "html.parser")

        # Remove non-content, basic start
        for tag in soup(self._disregard):
            tag.decompose()
        text = soup.get_text(separator="\n", strip=True)
        logging.debug(f"First 100 characters of text extracted: {text[0:100]}")
        return text


if __name__ == "__main__":
    import requests

    logging.basicConfig(level=logging.DEBUG)
    
    url = "https://books.toscrape.com/catalogue/william-shakespeares-star-wars-verily-a-new-hope-william-shakespeares-star-wars-4_871/index.html"
    html_data = requests.get(url).text

    extractor = MainContentExtractor()
    job_text = extractor.extract(html=html_data)
    print(job_text)
