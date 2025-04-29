import logging

from core.rag.extractor.extractor_base import BaseExtractor
from core.rag.models.document import Document

from core.rag.extractor.unstructured.my_chunking_strategy import ChunkProcessor

logger = logging.getLogger(__name__)


class UnstructuredPDFExtractor(BaseExtractor):
    """Load pdf files.


    Args:
        file_path: Path to the file to load.

        api_url: Unstructured API URL

        api_key: Unstructured API Key
    """

    def __init__(self, file_path: str):
        """Initialize with file path."""
        self._file_path = file_path

    def extract(self) -> list[Document]:
        from unstructured.partition.pdf import partition_pdf

        elements = partition_pdf(filename=self._file_path, strategy="auto")

        # from unstructured.chunking.title import chunk_by_title
        #
        # chunks = chunk_by_title(elements, max_characters=2000, combine_text_under_n_chars=2000)
        # documents = []
        # for chunk in chunks:
        #     text = chunk.text.strip()
        #     documents.append(Document(page_content=text))

        chunk_processor = ChunkProcessor()

        return chunk_processor.chunking(elements)
