from langchain_text_splitters import RecursiveCharacterTextSplitter
from config import CHUNK_SIZE, CHUNK_OVERLAP


def chunk_documents(docs):
    """
    Split documents into graph-extraction-ready chunks.
    """

    splitter = RecursiveCharacterTextSplitter(
        chunk_size=CHUNK_SIZE,
        chunk_overlap=CHUNK_OVERLAP,
        separators=[
            "\n\n",   # section level
            "\n",     # paragraph level
            ".",      # sentence level
            " ",
            ""
        ]
    )

    return splitter.split_documents(docs)