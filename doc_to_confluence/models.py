"""
Shared data models for the doc_to_confluence migration tool.
All types are TypedDicts to align with the project's existing dict-based patterns.
"""
from __future__ import annotations
from typing import Any, Dict, List, Literal, Optional
from typing_extensions import TypedDict


# ─── Content-element block (document-order interleaving) ─────────────────────

class ContentBlock(TypedDict):
    """
    One content element recorded in document order inside a ParsedSection.
    element_sequence is a list of these blocks that captures the original
    interleaving of text paragraphs, tables, and images so that they can be
    reproduced in the correct order in the Confluence output.

    block_type values:
      "text"  – a single body-text paragraph.
                Fields used: text (str), style (str)
      "table" – a table that appeared at this position.
                Fields used: table_index (int, index into section["tables"])
      "image" – an embedded image at this position.
                Fields used: image_index (int, index into section["images"])
    """
    block_type: str           # "text" | "table" | "image"
    text: Optional[str]       # body paragraph text  (block_type == "text")
    style: Optional[str]      # paragraph style name (block_type == "text")
    table_index: Optional[int]  # index into section["tables"] (block_type == "table")
    image_index: Optional[int]  # index into section["images"] (block_type == "image")


# ─── Parsed Document Models ───────────────────────────────────────────────────

class TableData(TypedDict):
    """A single table extracted from a Word document."""
    rows: List[List[str]]   # row-major: rows[row_index][col_index]
    header_row: bool        # True if first row is treated as a header


class ImageData(TypedDict):
    """An image embedded in a Word document section."""
    filename: str           # suggested filename, e.g. "image1.png"
    content_type: str       # MIME type, e.g. "image/png"
    data_b64: str           # base64-encoded raw image bytes
    paragraph_index: int    # which body paragraph (0-based) the image appeared after


class ParsedSection(TypedDict):
    """One logical section extracted from the Word document."""
    id: str                           # stable zero-padded id: "sec_001", "sec_002", ...
    title: str                        # heading text or custom delimiter text
    level: int                        # 1=H1, 2=H2, 3=H3, 0=standalone table block
    content_type: Literal["text", "table", "custom"]
    raw_text: str                     # concatenated plain text of all paragraphs
    tables: List[TableData]           # any tables directly under this section
    images: List[ImageData]           # embedded images in document order
    children: List[ParsedSection]     # nested sections (e.g. H2s under H1)
    paragraph_styles: List[str]       # style name per body paragraph (parallel to raw_text lines)
    page_number_hint: Optional[int]   # approximate Word page (best-effort, may be None)
    element_sequence: List[Any]       # List[ContentBlock] — interleaved text/table/image blocks
                                      # in original document order.  Used for correct image
                                      # positioning relative to tables in the Confluence output.


# ─── LLM Processing Models ────────────────────────────────────────────────────

LLMTaskName = Literal["reformat", "summarize", "plantuml_diagram", "confluence_format"]


class LLMResult(TypedDict):
    """Result of a single LLM task execution."""
    task: LLMTaskName
    input_text: str
    output_text: str
    success: bool
    error: Optional[str]


# ─── Configuration Models (runtime representation) ────────────────────────────

class ConfluenceTarget(TypedDict):
    space_key: str
    parent_page_id: Optional[str]               # for create: parent to nest under
    page_id: Optional[str]                      # for update/append: target page id
    action: Literal["create", "update", "append"]


class LLMConfig(TypedDict):
    enabled: bool
    tasks: List[LLMTaskName]                    # ordered, confluence_format last if present


class SectionMapping(TypedDict):
    match: str                                  # heading text or regex pattern
    match_type: Literal["heading", "custom_delimiter", "table", "regex"]
    confluence: ConfluenceTarget
    llm: LLMConfig


class MigrationConfig(TypedDict):
    """Top-level parsed config.yaml structure (runtime representation)."""
    llm_model: str
    llm_temperature: float
    confluence_base_url: str
    confluence_user: str
    confluence_api_token: str
    db_logging: bool
    db_path: str
    sections: List[SectionMapping]


# ─── Migration Result Models ──────────────────────────────────────────────────

class SectionResult(TypedDict):
    """Result of migrating one section mapping."""
    section_id: str
    section_title: str
    mapping_match: str
    action: Literal["create", "update", "append"]
    llm_results: List[LLMResult]
    confluence_page_id: Optional[str]
    confluence_page_url: Optional[str]
    status: Literal["success", "skipped", "failed"]
    error: Optional[str]


class MigrationReport(TypedDict):
    """Full migration run report."""
    doc_path: str
    config_path: str
    total_sections_in_doc: int
    total_mappings: int
    results: List[SectionResult]
    dry_run: bool
    started_at: str
    finished_at: str
