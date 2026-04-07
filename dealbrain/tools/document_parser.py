import os
import asyncio
import pdfplumber

def _parse_pdf_sync(file_path: str) -> str:
    """Synchronous helper to parse PDF and extract all text."""
    text_chunks = []
    with pdfplumber.open(file_path) as pdf:
        for page in pdf.pages:
            page_text = page.extract_text()
            if page_text:
                text_chunks.append(page_text)
    return "\n".join(text_chunks)

def _parse_txt_sync(file_path: str) -> str:
    """Synchronous helper to read standard text files."""
    with open(file_path, "r", encoding="utf-8") as f:
        return f.read()

def _parse_document_sync(file_path: str) -> str:
    """Synchronous generic parser handling routing and validations."""
    if not os.path.exists(file_path):
        raise ValueError(f"File does not exist: {file_path}")
        
    _, ext = os.path.splitext(file_path)
    ext = ext.lower()
    
    if ext == '.pdf':
        return _parse_pdf_sync(file_path)
    elif ext == '.txt':
        return _parse_txt_sync(file_path)
    else:
        raise ValueError(f"Unsupported file format: {ext}. Only .pdf and .txt are supported.")

async def parse_document(file_path: str) -> str:
    """
    Asynchronously parses a document (PDF or TXT) and extracts all text into a single string.
    No chunking or token splitting is performed.
    
    Args:
        file_path (str): The absolute or relative path to the document.
        
    Returns:
        str: The fully extracted text from the document.
        
    Raises:
        ValueError: If the file does not exist or has an unsupported format.
    """
    return await asyncio.to_thread(_parse_document_sync, file_path)