"""
PlantUML → PNG renderer for the doc_to_confluence migration tool.

Uses the public Kroki rendering service (https://kroki.io) to convert
PlantUML source code into PNG images.  No Confluence plugin required.
PNG is used (not SVG) because Confluence Cloud blocks SVG attachments
from rendering by default due to site-level security restrictions.

The main entry point is `render_and_embed_plantuml_diagrams()`, which:
  1. Scans Confluence storage format content for PlantUML macro blocks
  2. Injects `!theme <name>` into the source (unless already present)
  3. Renders each diagram to PNG via Kroki
  4. Uploads each PNG as a Confluence page attachment
  5. Replaces the macro block with:
       Left column  → code block with the themed PlantUML source
       Right column → the rendered PNG image (via attachment macro)

If Kroki is unreachable or returns an error for a diagram, that diagram
is left as the original code block fallback (no PNG column is inserted).

Kroki API (no auth required):
  POST https://kroki.io/plantuml/png
  body: plain-text PlantUML source
  returns: PNG bytes (Content-Type: image/png)

Layout produced in Confluence storage format:
  <table>
    <tbody><tr>
      <td style="width:45%;vertical-align:top">
        <ac:structured-macro ac:name="code" ac:schema-version="1">
          <ac:parameter ac:name="language">text</ac:parameter>
          <ac:plain-text-body><![CDATA[...themed source...]]></ac:plain-text-body>
        </ac:structured-macro>
      </td>
      <td style="width:55%;vertical-align:top;text-align:center">
        <ac:image ac:width="500">
          <ri:attachment ri:filename="plantuml_diagram_N.png"/>
        </ac:image>
      </td>
    </tr></tbody>
  </table>
"""
import re
import threading
import time
from typing import List, Optional, Tuple

import requests

# ─── Constants ────────────────────────────────────────────────────────────────

KROKI_URL = "https://kroki.io/plantuml/png"   # PNG — always renders in Confluence Cloud
KROKI_TIMEOUT = 30   # seconds per diagram
ATTACHMENT_PREFIX = "plantuml_diagram"

# Semaphore limiting concurrent Kroki requests across all threads.
# The public kroki.io endpoint has no SLA; flooding it with parallel renders
# from multiple section workers causes 429s and silent diagram loss.
# 3 concurrent renders is a safe ceiling without meaningfully slowing throughput.
_KROKI_SEMAPHORE = threading.Semaphore(3)
ATTACHMENT_EXT = "png"
ATTACHMENT_CONTENT_TYPE = "image/png"
DEFAULT_THEME = "cerulean"

# Matches a full PlantUML macro block (greedy, DOTALL so newlines match)
# Captures the raw CDATA content inside the <ac:plain-text-body>
_PLANTUML_MACRO_RE = re.compile(
    r'<ac:structured-macro\s+ac:name="plantuml"[^>]*>'   # opening tag
    r'.*?'                                                 # any attributes / parameters
    r'<ac:plain-text-body>\s*<!\[CDATA\[(.*?)\]\]>\s*</ac:plain-text-body>'
    r'\s*</ac:structured-macro>',
    re.DOTALL | re.IGNORECASE,
)

# Matches the companion code-block fallback that was emitted right after the
# plantuml macro (optional — may not always be present)
_CODE_BLOCK_FALLBACK_RE = re.compile(
    r'\s*<ac:structured-macro\s+ac:name="code"[^>]*>'
    r'.*?'
    r'</ac:structured-macro>',
    re.DOTALL | re.IGNORECASE,
)


# ─── Public API ───────────────────────────────────────────────────────────────

def render_and_embed_plantuml_diagrams(
    content: str,
    page_id: str,
    confluence_client,           # ConfluenceClient instance (duck-typed)
    start_index: int = 1,
    theme: str = DEFAULT_THEME,
    natural_size: bool = False,
) -> Tuple[str, int]:
    """
    Find all PlantUML macros in `content`, render each to SVG via Kroki,
    upload the SVG as a Confluence attachment, and replace each macro block
    with a two-column table (source code left, rendered image right).

    Args:
        content:           Confluence storage format page body
        page_id:           ID of the target Confluence page (for attachment upload)
        confluence_client: ConfluenceClient instance with upload_attachment()
        start_index:       Counter for attachment filenames (plantuml_diagram_N.svg).
                           Pass the return value into the next call to avoid collisions.
        theme:             PlantUML skin theme name (e.g. "cerulean", "plain").
                           Injected as !theme <name> at the top of each diagram unless
                           the source already contains a !theme directive.
        natural_size:      If True, omit ac:width so Confluence renders the image at
                           its actual PNG pixel width.  Defaults to False (700 px).

    Returns:
        (updated_content, next_index)
        updated_content has each successfully-rendered macro replaced with the
        two-column layout.  Failed diagrams retain the original macro block.
        next_index is start_index + number of successfully rendered diagrams.
    """
    idx = start_index
    updated = content

    # Find all macro matches (we iterate over the ORIGINAL content to get
    # match positions, then rebuild from scratch to avoid offset drift)
    matches = list(_PLANTUML_MACRO_RE.finditer(content))
    if not matches:
        return content, idx

    print(
        f"  [plantuml_renderer] Found {len(matches)} PlantUML macro(s) on page {page_id} "
        f"(theme: {theme or 'none'})"
    )

    # Process in reverse order so string replacements don't shift earlier offsets
    for match in reversed(matches):
        plantuml_source = match.group(1).strip()

        # Look for an immediately-following code block fallback to also replace
        # (the one we added in the previous session as a text fallback)
        after_macro_pos = match.end()
        fallback_match = _CODE_BLOCK_FALLBACK_RE.match(updated, after_macro_pos)
        replacement_end = fallback_match.end() if fallback_match else match.end()
        replacement_start = match.start()

        # Inject theme — themed_source is used for both rendering AND the code
        # block so the reader can see exactly what theme was applied
        themed_source = _inject_theme(plantuml_source, theme)

        # Render to PNG
        png_bytes = _render_to_png(themed_source)
        if png_bytes is None:
            print(f"    WARN: Kroki rendering failed for diagram {idx} — leaving as-is")
            continue

        # Upload PNG attachment
        filename = f"{ATTACHMENT_PREFIX}_{idx}.{ATTACHMENT_EXT}"
        try:
            confluence_client.upload_attachment(
                page_id=page_id,
                filename=filename,
                data_bytes=png_bytes,
                content_type=ATTACHMENT_CONTENT_TYPE,
            )
            print(f"    Uploaded PNG attachment: {filename}")
        except Exception as exc:
            print(f"    WARN: Could not upload {filename}: {exc} — leaving macro as-is")
            continue

        # Use natural (actual PNG) size for use case diagrams — they tend to be
        # compact and should not be stretched to 700 px.  Other diagram types
        # (sequence, component, etc.) keep the default 700 px width.
        is_usecase_diagram = _is_usecase_diagram(themed_source)
        use_natural = natural_size or is_usecase_diagram

        # Build two-column replacement; use themed_source in the code block so
        # the !theme line is visible — makes it clear what theme was applied
        two_col = _build_two_column_layout(themed_source, filename, natural_size=use_natural)

        # Replace macro (+ optional fallback) with two-column layout
        updated = updated[:replacement_start] + two_col + updated[replacement_end:]
        idx += 1

    return updated, idx


# ─── Theme Injection ──────────────────────────────────────────────────────────

def _inject_theme(source: str, theme: str) -> str:
    """
    Insert `!theme <theme>` into a PlantUML source block, right after the
    opening @start* directive, unless:
      - theme is blank / "none"
      - the source already contains a !theme directive (user override respected)

    Example input (@startuml\n...@enduml) with theme="cerulean":
      @startuml
      !theme cerulean
      ...
      @enduml
    """
    if not theme or theme.lower() == "none":
        return source
    # Respect any existing !theme directive in the source
    if re.search(r'^\s*!theme\b', source, re.IGNORECASE | re.MULTILINE):
        return source

    # Insert after the opening @start* line
    return re.sub(
        r'(@start\S+)([ \t]*(?:\r\n|\r|\n))',
        lambda m: f"{m.group(1)}{m.group(2)}!theme {theme}\n",
        source,
        count=1,
        flags=re.IGNORECASE,
    )


# ─── Kroki Rendering ──────────────────────────────────────────────────────────

def _render_to_png(plantuml_source: str) -> Optional[bytes]:
    """
    POST plantuml_source to Kroki and return the PNG bytes, or None on failure.
    PNG is used instead of SVG because Confluence Cloud blocks SVG attachments
    from rendering by default (site-level security restriction).

    Acquires _KROKI_SEMAPHORE before each request to cap concurrent calls to
    the public kroki.io endpoint and avoid 429 rate-limit errors.
    """
    with _KROKI_SEMAPHORE:
        try:
            resp = requests.post(
                KROKI_URL,
                data=plantuml_source.encode("utf-8"),
                headers={"Content-Type": "text/plain; charset=utf-8"},
                timeout=KROKI_TIMEOUT,
            )
            if resp.status_code == 200:
                # Verify we got actual PNG bytes (magic: 89 50 4E 47)
                if resp.content[:4] == b'\x89PNG':
                    return resp.content
                print(
                    f"  [plantuml_renderer] Kroki returned non-PNG content "
                    f"({len(resp.content)} bytes, first 100: {resp.content[:100]})"
                )
                return None
            print(
                f"  [plantuml_renderer] Kroki returned HTTP {resp.status_code}: "
                f"{resp.text[:200]}"
            )
            return None
        except requests.RequestException as exc:
            print(f"  [plantuml_renderer] Kroki request failed: {exc}")
            return None


# ─── Diagram Type Detection ───────────────────────────────────────────────────

def _is_usecase_diagram(plantuml_source: str) -> bool:
    """
    Return True if the PlantUML source looks like a use case diagram.

    Use case diagrams typically contain either:
      - 'usecase' keyword (e.g. "usecase UC1 as ...")
      - Both 'actor' and '(' in the source (shorthand notation: "actor User" + "(action)")

    This heuristic is used to determine whether to render at natural size
    instead of the default 700 px width.
    """
    src_lower = plantuml_source.lower()
    has_usecase_keyword = bool(re.search(r'\busecase\b', src_lower))
    has_actor_and_parens = bool(re.search(r'\bactor\b', src_lower)) and '(' in plantuml_source
    return has_usecase_keyword or has_actor_and_parens


# ─── Layout Builder ───────────────────────────────────────────────────────────

def _build_two_column_layout(
    plantuml_source: str,
    svg_filename: str,
    natural_size: bool = False,
) -> str:
    """
    Build a Confluence storage format layout:
      1. PNG image (full width, or natural size if natural_size=True)
      2. Collapsible "expand" macro containing the raw PlantUML source code
    """
    # Escape source for CDATA — ]]> must be split if present
    safe_source = plantuml_source.replace("]]>", "]]]]><![CDATA[>")

    # Confluence Cloud storage format for an inline PNG attachment image.
    # When natural_size=True, omit ac:width so Confluence uses the PNG's
    # intrinsic dimensions (avoids upscaling small use-case diagrams).
    if natural_size:
        image_macro = (
            '<ac:image>\n'
            f'  <ri:attachment ri:filename="{svg_filename}"/>\n'
            '</ac:image>'
        )
    else:
        image_macro = (
            '<ac:image ac:width="700">\n'
            f'  <ri:attachment ri:filename="{svg_filename}"/>\n'
            '</ac:image>'
        )

    code_block = (
        '<ac:structured-macro ac:name="code" ac:schema-version="1">\n'
        '  <ac:parameter ac:name="language">text</ac:parameter>\n'
        '  <ac:plain-text-body><![CDATA[\n'
        f'{safe_source}\n'
        '  ]]></ac:plain-text-body>\n'
        '</ac:structured-macro>'
    )

    # Collapsible expand macro wrapping the code block
    expand_macro = (
        '<ac:structured-macro ac:name="expand" ac:schema-version="1">\n'
        '  <ac:parameter ac:name="title">PlantUML Source</ac:parameter>\n'
        '  <ac:rich-text-body>\n'
        f'    {code_block}\n'
        '  </ac:rich-text-body>\n'
        '</ac:structured-macro>'
    )

    return f'{image_macro}\n{expand_macro}'
