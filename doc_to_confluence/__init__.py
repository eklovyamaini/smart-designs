"""
doc_to_confluence: Word document to Confluence migration tool.

Entry point:
    python -m doc_to_confluence migrate --doc file.docx --config config.yaml
    python -m doc_to_confluence parse --doc file.docx

Public API:
    from doc_to_confluence.config import load_config
    from doc_to_confluence.parser import parse_docx
    from doc_to_confluence.orchestrator import MigrationOrchestrator
    from doc_to_confluence.confluence_client import ConfluenceClient
    from doc_to_confluence.llm_processor import LLMProcessor
"""

__version__ = "0.1.0"
