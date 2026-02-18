from abc import ABC, abstractmethod
from bs4 import BeautifulSoup


class IExtractor(ABC):
    """
    Interface class for Extractor
    """

    @abstractmethod
    def extract(self, html: str) -> str:
        raise NotImplementedError("Do not call abstract base class.")


class MainContentExtractor(IExtractor):
    """
    Extract the main human-readable text from a web page
    using BeautifulSoup.
    Accepts raw HTML as input.
    """
    def __init__(self):
        pass

    def extract(self, html: str) -> str:
        soup = BeautifulSoup(html, "html.parser")

        # Remove non-content, basic start
        for tag in soup(["script", "style", "nav", "footer", "header", "aside"]):
            tag.decompose()
        text = soup.get_text(separator="\n", strip=True)
        return text


if __name__ == "__main__":
    import requests

    # This is a cut-off example of the HTML input (cut-off so that this code is not too long)
    url = "https://www.abp.nl/over-abp/over-de-organisatie/werken-bij/vacature-senior-adviseur-pensioenen"
    html_data = requests.get(url).text

    extractor = MainContentExtractor()
    job_text = extractor.extract(html=html_data)
    print(job_text)
