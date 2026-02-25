import re


def clean_text(text: str) -> str:
    """
    Clean raw PDF text for policy document processing.
    Optimized for structured governance documents.
    """

    # Remove URLs
    text = re.sub(r'http\S+|www\.\S+', '', text)

    # Remove page numbers like "Page 12"
    text = re.sub(r'Page\s+\d+', '', text, flags=re.IGNORECASE)

    # Remove standalone numbers (common footer numbers)
    text = re.sub(r'\n\d+\n', '\n', text)

    # Fix hyphenated line breaks
    text = re.sub(r'-\n(\w)', r'\1', text)

    # Merge broken lines inside paragraphs
    text = re.sub(r'(?<!\n)\n(?!\n)', ' ', text)

    # Normalize multiple line breaks
    text = re.sub(r'\n{3,}', '\n\n', text)

    # Normalize spaces
    text = re.sub(r'[ \t]{2,}', ' ', text)

    return text.strip()