"""
Word document parser for the doc_to_confluence migration tool.

Parses .docx files into a tree of ParsedSection objects, preserving:
  - Heading-based section boundaries (H1/H2/H3)
  - Tables in their correct document position (interleaved with paragraphs)
  - Custom delimiter patterns (configurable regex)
  - Nested structure (H2s become children of H1s, etc.)

Key design: uses lxml-level iteration over doc.element.body children to preserve
the true paragraph/table interleaving order. doc.paragraphs skips tables entirely.
"""
import base64
import mimetypes
import re
from typing import Dict, List, Optional, Tuple

from docx import Document
from docx.table import Table as DocxTable
from docx.text.paragraph import Paragraph
from lxml import etree

from .models import ContentBlock, ImageData, ParsedSection, TableData

# Word / DrawingML XML namespaces used for image extraction
_NS = {
    "a":   "http://schemas.openxmlformats.org/drawingml/2006/main",
    "pic": "http://schemas.openxmlformats.org/drawingml/2006/picture",
    "r":   "http://schemas.openxmlformats.org/officeDocument/2006/relationships",
    "v":   "urn:schemas-microsoft-com:vml",
    "w":   "http://schemas.openxmlformats.org/wordprocessingml/2006/main",
}

# Map common image content-types → extension
_CT_TO_EXT: Dict[str, str] = {
    "image/png":  ".png",
    "image/jpeg": ".jpg",
    "image/gif":  ".gif",
    "image/bmp":  ".bmp",
    "image/tiff": ".tiff",
    "image/webp": ".webp",
    "image/svg+xml": ".svg",
    "image/x-emf": ".emf",
    "image/x-wmf": ".wmf",
}


# Word heading style names → heading level mapping
HEADING_STYLES: Dict[str, int] = {
    "Heading 1": 1,
    "Heading 2": 2,
    "Heading 3": 3,
    "Heading 4": 4,
    "Heading 5": 5,
    "Title": 1,       # Word "Title" style treated as H1
}

_section_counter = 0


def parse_docx(
    doc_path: str,
    custom_delimiter_patterns: Optional[List[str]] = None,
) -> List[ParsedSection]:
    """
    Parse a .docx file into a tree of ParsedSection objects.

    Args:
        doc_path: Path to the .docx file.
        custom_delimiter_patterns: Optional list of regex strings. Any paragraph
            whose text fully matches one of these patterns starts a new section
            with content_type='custom'.

    Returns:
        List of top-level ParsedSection objects. Nested sections (H2, H3) are
        stored in each section's `children` list.

    Raises:
        FileNotFoundError: if doc_path does not exist.
        Exception: if the file cannot be opened as a valid .docx.
    """
    doc = Document(doc_path)
    compiled_delimiters = [
        re.compile(p, re.IGNORECASE)
        for p in (custom_delimiter_patterns or [])
    ]

    counter = [0]  # mutable counter passed via closure
    flat: List[ParsedSection] = []
    current_section: Optional[ParsedSection] = None

    body = doc.element.body
    for child in body:
        # Extract local tag name (strip namespace)
        tag = child.tag.split("}")[-1] if "}" in child.tag else child.tag

        if tag == "p":
            para = Paragraph(child, doc)
            text = para.text.strip()

            # Always extract images from this paragraph element first.
            # Pass the current section image count so filenames are unique
            # across all paragraphs in the section (not just within this one).
            section_img_count = len(current_section["images"]) if current_section else 0
            para_images = _extract_images_from_para(child, doc, counter[0], section_img_count)

            # A paragraph with no text but images: attach images to current
            # section and skip the rest of paragraph processing
            if not text:
                if para_images and current_section is not None:
                    for img in para_images:
                        # update paragraph_index relative to section
                        img_idx = len(current_section["images"])
                        img["paragraph_index"] = img_idx
                        current_section["images"].append(img)
                        # Record in element_sequence
                        current_section["element_sequence"].append(
                            ContentBlock(
                                block_type="image",
                                text=None,
                                style=None,
                                table_index=None,
                                image_index=img_idx,
                            )
                        )
                elif para_images and current_section is None:
                    # Images before any heading → create preamble section
                    counter[0] += 1
                    current_section = _make_section(
                        id=_build_section_id(counter[0]),
                        title="(preamble)",
                        level=1,
                        content_type="text",
                    )
                    flat.append(current_section)
                    for idx, img in enumerate(para_images):
                        img["paragraph_index"] = idx
                        current_section["images"].append(img)
                        current_section["element_sequence"].append(
                            ContentBlock(
                                block_type="image",
                                text=None,
                                style=None,
                                table_index=None,
                                image_index=idx,
                            )
                        )
                continue

            heading_level = _get_heading_level(para)
            is_custom = _matches_custom_delimiter(text, compiled_delimiters)

            if heading_level is not None:
                counter[0] += 1
                current_section = _make_section(
                    id=_build_section_id(counter[0]),
                    title=text,
                    level=heading_level,
                    content_type="text",
                )
                flat.append(current_section)

            elif is_custom:
                counter[0] += 1
                current_section = _make_section(
                    id=_build_section_id(counter[0]),
                    title=text,
                    level=1,
                    content_type="custom",
                )
                flat.append(current_section)

            else:
                # Body paragraph: append to current section
                if current_section is None:
                    # Content before any heading → implicit preamble section
                    counter[0] += 1
                    current_section = _make_section(
                        id=_build_section_id(counter[0]),
                        title="(preamble)",
                        level=1,
                        content_type="text",
                    )
                    flat.append(current_section)

                para_style = para.style.name if para.style else "Normal"
                sep = "\n" if current_section["raw_text"] else ""
                current_section["raw_text"] += sep + text
                current_section["paragraph_styles"].append(para_style)

                # Record text block in element_sequence
                current_section["element_sequence"].append(
                    ContentBlock(
                        block_type="text",
                        text=text,
                        style=para_style,
                        table_index=None,
                        image_index=None,
                    )
                )

                # Attach any images found inline in this text paragraph
                # (they appear after the text in document order)
                for img in para_images:
                    img_idx = len(current_section["images"])
                    img["paragraph_index"] = img_idx
                    current_section["images"].append(img)
                    current_section["element_sequence"].append(
                        ContentBlock(
                            block_type="image",
                            text=None,
                            style=None,
                            table_index=None,
                            image_index=img_idx,
                        )
                    )

        elif tag == "tbl":
            table_obj = DocxTable(child, doc)
            table_data = _extract_table(table_obj)

            if current_section is not None:
                tbl_idx = len(current_section["tables"])
                current_section["tables"].append(table_data)
                current_section["element_sequence"].append(
                    ContentBlock(
                        block_type="table",
                        text=None,
                        style=None,
                        table_index=tbl_idx,
                        image_index=None,
                    )
                )
            else:
                # Standalone table before any heading
                counter[0] += 1
                current_section = _make_section(
                    id=_build_section_id(counter[0]),
                    title="(table)",
                    level=0,
                    content_type="table",
                )
                current_section["tables"].append(table_data)
                current_section["element_sequence"].append(
                    ContentBlock(
                        block_type="table",
                        text=None,
                        style=None,
                        table_index=0,
                        image_index=None,
                    )
                )
                flat.append(current_section)

        # All other element types (w:sectPr, etc.) are silently ignored

    return _build_tree(flat)


# ─── Section Helpers ──────────────────────────────────────────────────────────

def _make_section(
    id: str,
    title: str,
    level: int,
    content_type: str,
) -> ParsedSection:
    return ParsedSection(
        id=id,
        title=title,
        level=level,
        content_type=content_type,  # type: ignore[arg-type]
        raw_text="",
        tables=[],
        images=[],
        children=[],
        paragraph_styles=[],
        page_number_hint=None,
        element_sequence=[],
    )


def _build_section_id(index: int) -> str:
    """Generate a stable zero-padded section id: 'sec_001', 'sec_002', ..."""
    return f"sec_{index:03d}"


def _get_heading_level(paragraph: Paragraph) -> Optional[int]:
    """Return the heading level (1-5) for a paragraph, or None if not a heading."""
    style_name = paragraph.style.name if paragraph.style else ""
    return HEADING_STYLES.get(style_name)


def _matches_custom_delimiter(
    text: str,
    patterns: List[re.Pattern],
) -> bool:
    """Return True if text matches any of the compiled delimiter patterns."""
    return any(p.search(text) for p in patterns)


def _extract_table(table: DocxTable) -> TableData:
    """
    Convert a python-docx Table object to a TableData dict.
    Merged cells repeat content (python-docx behaviour).
    First row is assumed to be a header if the table has more than 1 row.
    """
    rows: List[List[str]] = []
    for row in table.rows:
        row_data = [cell.text.strip() for cell in row.cells]
        rows.append(row_data)
    header_row = len(rows) > 1
    return TableData(rows=rows, header_row=header_row)


# ─── Image Extraction ─────────────────────────────────────────────────────────

def _extract_images_from_para(
    para_elem: etree._Element,
    doc: Document,
    section_counter: int,
    section_image_offset: int = 0,
) -> List[ImageData]:
    """
    Extract all embedded images from a paragraph lxml element.

    Handles two embedding mechanisms:
    1. DrawingML: <w:drawing> → <a:blip r:embed="rId…"> (modern .docx)
    2. VML:       <v:imagedata r:id="rId…"> (legacy .doc-converted-to-.docx)

    Args:
        section_image_offset: Number of images already collected in the
            parent section before this paragraph.  Used to generate unique
            filenames (sec001_img1.png, sec001_img2.png …) across all
            paragraphs in the section, not just within the current paragraph.

    Returns a list of ImageData dicts (may be empty if no images found).
    """
    images: List[ImageData] = []
    seen_rids: set = set()  # deduplicate if same rId appears twice

    # ── DrawingML blip references ──────────────────────────────────────────
    for blip in para_elem.iter("{%s}blip" % _NS["a"]):
        rid = blip.get("{%s}embed" % _NS["r"]) or blip.get("{%s}link" % _NS["r"])
        if rid and rid not in seen_rids:
            img = _load_image_part(doc, rid, section_image_offset + len(images), section_counter)
            if img:
                images.append(img)
                seen_rids.add(rid)

    # ── VML imagedata references ───────────────────────────────────────────
    for imgdata in para_elem.iter("{%s}imagedata" % _NS["v"]):
        rid = imgdata.get("{%s}id" % _NS["r"])
        if rid and rid not in seen_rids:
            img = _load_image_part(doc, rid, section_image_offset + len(images), section_counter)
            if img:
                images.append(img)
                seen_rids.add(rid)

    return images


def _load_image_part(
    doc: Document,
    rid: str,
    img_index: int,
    section_counter: int,
) -> Optional[ImageData]:
    """
    Resolve a relationship ID to an image part and return ImageData.
    Returns None if the relationship does not point to an image.
    """
    try:
        part = doc.part.related_parts.get(rid)
        if part is None:
            return None

        content_type: str = part.content_type  # e.g. "image/png"
        if not content_type.startswith("image/"):
            return None  # not an image (could be chart, smartart, etc.)

        ext = _CT_TO_EXT.get(content_type)
        if ext is None:
            # fall back to mimetypes
            ext = mimetypes.guess_extension(content_type) or ".bin"

        filename = f"sec{section_counter:03d}_img{img_index + 1}{ext}"
        data_b64 = base64.b64encode(part.blob).decode("ascii")

        return ImageData(
            filename=filename,
            content_type=content_type,
            data_b64=data_b64,
            paragraph_index=img_index,  # caller may override
        )
    except Exception:
        return None


# ─── Tree Builder ─────────────────────────────────────────────────────────────

def _build_tree(flat_sections: List[ParsedSection]) -> List[ParsedSection]:
    """
    Convert a flat ordered list of ParsedSection into a nested tree by
    assigning lower-level headings as children of the most recent higher-level heading.

    Algorithm (stack-based, O(n)):
      - Maintain a stack of (level, section).
      - For each section:
          * Pop stack entries with level >= current section level
          * If stack is non-empty: add section as child of stack top
          * Otherwise: add section to roots
          * Push current section onto stack

    Standalone table blocks (level=0) and custom sections always go to roots
    or under the current H1, depending on stack state.

    Returns only the root-level sections.
    """
    roots: List[ParsedSection] = []
    stack: List[Tuple[int, ParsedSection]] = []

    for section in flat_sections:
        level = section["level"]

        # Pop entries at same or deeper level
        while stack and stack[-1][0] >= level:
            stack.pop()

        if stack:
            stack[-1][1]["children"].append(section)
        else:
            roots.append(section)

        stack.append((level, section))

    return roots
