from bs4 import BeautifulSoup
from readability import Document


class MainContentExtractor:
    """
    Extract the main human-readable text from a job advertisement
    using Mozilla Readability (readability-lxml) and BeautifulSoup.
    Accepts raw HTML as input.
    """
    def __init__(self):
        pass

    def extract(self, html: str) -> str:
        # Use Readability to isolate the main content
        doc = Document(html)
        readable_html = doc.summary()  # Extracted main-content HTML block

        # Convert the readable HTML to text
        soup = BeautifulSoup(readable_html, "html.parser")
        text = soup.get_text(separator="\n")

        # Clean and deduplicate whitespace lines
        cleaned = "\n".join(
            line.strip() for line in text.splitlines()
            if line.strip()
        )

        return cleaned


if __name__ == "__main__":

    # This is a cut-off example of the HTML input (cut-off so that this code is not too long)

    html_data = '''<!DOCTYPE html>
        <html>
        <head><title>Job Ad</title></head>
        <body>
        <h1>Software Engineer</h1>
        <p>We are looking for a talented Software Engineer to join our team.</p>
        <p>Responsibilities include developing web applications, collaborating with designers, and writing clean, efficient code.</p>
        <p><strong>Requirements:</strong> 3+ years of experience, proficiency in Python and JavaScript, and a passion for problem solving.</p>
        <p>Interested candidates should apply at <a href="mailto:jobs@example.com">jobs@example.com</a>.</p>
        </body>
        </html>'''

    extractor = MainContentExtractor()
    job_text = extractor.extract(html=html_data)
    print(job_text)
