import logging

from bs4 import BeautifulSoup
from core.rag.models.document import Document
from unstructured.documents.elements import Element

logger = logging.getLogger(__name__)


class ChunkProcessor:
    def __init__(self):
        self.all_chunks: list[Document] = []

    def convert_table_to_tsv_string(self, html_content: str) -> str:
        """
        Converts an HTML table to a TSV (Tab-Separated Values) string.

        Args:
            html_content (str): The HTML content containing the table.

        Returns:
            str: A TSV-formatted string representation of the table,
                 or an error message if no table is found.
        """
        try:
            # Parse HTML content
            soup = BeautifulSoup(html_content, 'html.parser')

            # Find the table
            table = soup.find('table')

            if not table:
                logger.error("No table found in the HTML content.")
                raise ValueError("No table found in the HTML content.")

            # Extract all rows
            rows = table.find_all('tr')

            # Initialize result string
            result = ""

            # Process each row
            for row in rows:
                # Get all cells in this row
                cells = row.find_all(['td', 'th'])

                # Extract text from each cell
                row_texts = [cell.get_text().strip() for cell in cells]

                # Join cell texts with tabs
                row_string = "\t".join(row_texts)

                # Add row to result
                result += row_string + "\n"

            logger.info("Successfully converted HTML table to TSV format.")
            return result
        except Exception as e:
            logger.error(f"Error converting table to TSV: {str(e)}")
            raise ValueError(f"Error converting table to TSV: {str(e)}")

    def _chunk_table(self, table_content: str, table_description: str, header_rows: int = 5, chunk_size: int = 10) -> \
            list[Document]:
        """
                Chunk a table by keeping the header rows and adding chunks of data rows.

                Args:
                    table_content (str): TSV-formatted table content
                    table_description (str): Description of the table
                    header_rows (int): Number of rows to treat as header
                    chunk_size (int): Number of data rows per chunk

                Returns:
                    List[Document]: List of chunks as Document objects
                """
        table_chunks = []

        # Split the TSV content into rows
        rows = table_content.strip().split('\n')

        # Check if there are enough rows for headers
        if len(rows) <= header_rows:
            # If table is too small, just return it as a single chunk
            return [Document(
                page_content=f"Miêu tả nội dung bảng: {table_description}\n{table_content}",
            )]

        # Extract header rows
        headers = rows[:header_rows]
        header_content = '\n'.join(headers)

        # Get data rows
        data_rows = rows[header_rows:]

        # Create chunks with header + chunk_size data rows
        for i in range(0, len(data_rows), chunk_size):
            chunk_data_rows = data_rows[i:i + chunk_size]
            chunk_content = f"{header_content}\n" + '\n'.join(chunk_data_rows)

            table_chunks.append(Document(
                page_content=f"Miêu tả nội dung bảng: {table_description}\n{chunk_content}",
            ))

        return table_chunks

    def chunking(self, elements: list[Element], header_rows: int = 5, table_chunk_size: int = 15) -> list[Document]:
        chunks_text = [
            Document(
                page_content=chunk.text,
            )
            for chunk in elements
        ]

        # Process table chunks
        chunks_table = []
        for i, element in enumerate(elements):
            if element.metadata.text_as_html is not None:
                # Get table description from previous element if available
                table_description = elements[i - 1].text if i > 0 else ""

                # Convert table to TSV format
                table_tsv = self.convert_table_to_tsv_string(str(element.metadata.text_as_html))

                # Chunk the table
                table_chunks = self._chunk_table(
                    table_tsv,
                    table_description,
                    header_rows=header_rows,
                    chunk_size=table_chunk_size
                )

                chunks_table.extend(table_chunks)
        # Store all chunks
        self.all_chunks.extend(chunks_text)
        self.all_chunks.extend(chunks_table)
        logger.info(f"Chunking completed successfully. Total chunks: {len(self.all_chunks)}")
        return self.all_chunks
