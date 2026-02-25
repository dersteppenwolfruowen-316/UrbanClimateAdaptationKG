import fitz
from langchain_core.documents import Document
from ingestion.cleaner import clean_text


def load_pdf(file_path: str):
    """
    Load PDF and return cleaned LangChain Documents (page-level).
    """
    doc = fitz.open(file_path)
    pages = []

    for i, page in enumerate(doc):
        text = page.get_text()
        text = clean_text(text)

        if len(text) < 100:
            continue

        pages.append(
            Document(
                page_content=text,
                metadata={
                    "page": i + 1,
                    "source": file_path
                }
            )
        )

    return pages