"""
Confluence page metadata manager.

Applies standardised metadata blocks to Confluence pages:
  - 📋 Document Metadata  (Confluence Page Properties macro inside an Expand macro)
  - 🕓 Change History      (standalone table inside an Expand macro)

The operation is idempotent: pages that already contain the Document Metadata
Expand block are skipped unless force=True is passed.

Values auto-populated per page:
  - Author / Changed By  resolved from the page creator's Atlassian account via
                         ConfluenceClient.get_user_display_name()
  - Module               derived from the title of the parent URL page the page
                         falls under (one API call per unique parent URL)

Usage:
    from doc_to_confluence.confluence_client import ConfluenceClient
    from doc_to_confluence.metadata_manager import MetadataManager

    client = ConfluenceClient(base_url, user, api_token)
    mgr    = MetadataManager(client)

    # Bulk apply to all descendants of a list of parent pages
    for event in mgr.apply_to_scope(parent_urls, force=False, default_approvers=""):
        print(event)
"""
import re
import uuid
from datetime import date, datetime
from html import unescape
from typing import Generator, List, Optional
from urllib.parse import urlparse

from doc_to_confluence.confluence_client import ConfluenceAPIError, ConfluenceClient

# Sentinel string used to detect whether the metadata block is already present.
_METADATA_SENTINEL = "📋 Document Metadata"
# Sentinel for the legacy Change History expand block (also stripped on force re-apply).
_CHANGE_HISTORY_SENTINEL = "🕓 Change History"


def _to_label_slug(text: str) -> str:
    """Convert a display name to a Confluence-safe label slug.

    Output: lowercase, alphanumeric + hyphens only, max 200 chars.

    Examples::

        _to_label_slug("Contract Budget")          -> "contract-budget"
        _to_label_slug("S313 Contract Budget")     -> "s313-contract-budget"
        _to_label_slug("Use Cases & Workflows")    -> "use-cases-workflows"
    """
    slug = text.lower().strip()
    slug = re.sub(r"[^\w\s-]", "", slug)
    slug = re.sub(r"[\s_]+", "-", slug)
    slug = re.sub(r"-+", "-", slug).strip("-")
    return slug[:200]


class MetadataManager:
    """
    Applies Document Metadata + Change History Expand blocks to Confluence pages.

    Args:
        client:   An authenticated ConfluenceClient instance.
        base_url: Confluence base URL (e.g. "https://myorg.atlassian.net").
                  Used to build page URLs in progress events.
    """

    def __init__(self, client: ConfluenceClient, base_url: str) -> None:
        self._client = client
        self._base_url = base_url.rstrip("/")

    # ─── Public API ──────────────────────────────────────────────────────────

    def extract_page_id_from_url(self, url: str) -> str:
        """
        Extract the numeric Confluence page ID from a page URL.

        Handles both v1 format:
            https://org.atlassian.net/wiki/spaces/KEY/pages/12345678/Page-Title
        and v2 format:
            https://org.atlassian.net/wiki/spaces/KEY/pages/12345678

        Returns:
            The numeric page ID as a string.

        Raises:
            ValueError if no numeric page ID can be found in the URL.
        """
        parsed = urlparse(url)
        match = re.search(r"/pages/(\d+)", parsed.path)
        if match:
            return match.group(1)
        raise ValueError(
            f"Could not extract a numeric page ID from URL: {url!r}. "
            "Expected format: .../pages/12345678/..."
        )

    def has_metadata_blocks(self, page_body: str) -> bool:
        """
        Return True if the page body already contains the Document Metadata block.

        Detection uses the sentinel string embedded in the Expand macro title,
        so it is robust to minor whitespace or attribute ordering differences.
        """
        return _METADATA_SENTINEL in page_body

    # ─── Stripping helpers (used by force re-apply) ───────────────────────────

    @staticmethod
    def _find_macro_end(body: str, start: int) -> int:
        """
        Return the index immediately after the closing ``</ac:structured-macro>``
        tag that matches the opening ``<ac:structured-macro`` at *start*.

        Uses a depth counter to handle the nested macros correctly:
        - Increments depth on every ``<ac:structured-macro``
        - Decrements depth on every ``</ac:structured-macro>``
        - Returns when depth reaches 0 (the matched closing tag has been consumed)

        Returns -1 if the body is malformed (no matching close found).
        """
        OPEN  = "<ac:structured-macro"
        CLOSE = "</ac:structured-macro>"
        depth = 0
        pos   = start

        while pos < len(body):
            next_open  = body.find(OPEN,  pos)
            next_close = body.find(CLOSE, pos)

            if next_close == -1:
                return -1   # malformed — no closing tag at all

            if next_open != -1 and next_open < next_close:
                depth += 1
                pos    = next_open + len(OPEN)
            else:
                depth -= 1
                pos    = next_close + len(CLOSE)
                if depth == 0:
                    return pos

        return -1

    def strip_metadata_blocks(self, page_body: str) -> str:
        """
        Remove **all** existing metadata Expand macro pairs (Document Metadata +
        Change History) that were previously prepended by :meth:`apply_to_page`.

        The method loops until the sentinel is no longer present, so it correctly
        handles pages that accumulated multiple duplicate sets from earlier
        force-applies.  It is safe to call on pages with no metadata blocks —
        the body is returned unchanged when the sentinel is absent.

        Each iteration strips one consecutive pair of ``<ac:structured-macro>``
        blocks from the start of the body:
        1. Find block 1 (Document Metadata Expand — first macro in the body)
        2. Walk to its matching close tag via :meth:`_find_macro_end`
        3. Find block 2 (Change History Expand — next macro after block 1)
        4. Walk to its matching close tag
        5. Splice both out; repeat until the sentinel is gone or body is unchanged
        """
        OPEN   = "<ac:structured-macro"
        result = page_body

        while _METADATA_SENTINEL in result:
            block1_start = result.find(OPEN)
            if block1_start == -1:
                break   # no macro tag at all — stop

            block1_end = self._find_macro_end(result, block1_start)
            if block1_end == -1:
                break   # malformed XML — stop rather than corrupt the page

            block2_start = result.find(OPEN, block1_end)
            if block2_start == -1:
                # Only one block remains — strip it and we're done
                result = result[:block1_start] + result[block1_end:].lstrip("\n")
                break

            block2_end = self._find_macro_end(result, block2_start)
            if block2_end == -1:
                # Block 2 is malformed — strip block 1 only and stop
                result = result[:block1_start] + result[block1_end:].lstrip("\n")
                break

            result = result[:block1_start] + result[block2_end:].lstrip("\n")

        return result

    # ─── Confluence @mention helpers ─────────────────────────────────────────

    @staticmethod
    def _make_mention(account_id: str) -> str:
        """
        Return the Confluence storage-format XML for a single user @mention.

        Renders as a blue pill with the user's avatar and display name inside
        any Confluence page — including Page Properties table cells.
        """
        return f'<ac:link><ri:user ri:account-id="{account_id}"/></ac:link>'

    def _resolve_mentions(self, names_csv: str) -> str:
        """
        Convert a comma-separated string of display names into Confluence
        @mention macros wherever the name can be resolved to an account ID.

        For each comma-separated token:
        - If ``find_user_by_name()`` returns an account ID → emit a mention macro
        - If the lookup fails (user not found, API unavailable) → emit the name
          as HTML-escaped plain text so the cell still shows something useful

        Tokens are joined with ``", "`` (comma-space) in the returned string.

        Args:
            names_csv: Free-text string, e.g. "Jane Smith, Bob Jones".

        Returns:
            HTML/XML fragment ready to be placed directly inside a ``<td>``
            cell.  An empty string is returned for empty input.
        """
        from html import escape as _html_escape

        if not names_csv.strip():
            return ""

        parts: list = []
        for name in names_csv.split(","):
            name = name.strip()
            if not name:
                continue
            account_id = self._client.find_user_by_name(name)
            if account_id:
                parts.append(self._make_mention(account_id))
            else:
                parts.append(_html_escape(name))   # safe fallback — plain text

        return ", ".join(parts)

    # ─── ADF helpers (Atlassian Document Format — avoids "legacy" warning) ───

    @staticmethod
    def _adf_text(text: str) -> dict:
        """ADF inline text node."""
        return {"type": "text", "text": text}

    @staticmethod
    def _adf_mention(account_id: str, display_name: str = "") -> dict:
        """ADF inline user mention node."""
        return {
            "type": "mention",
            "attrs": {
                "id":          account_id,
                "text":        display_name or f"@{account_id}",
                "accessLevel": "",
            },
        }

    @staticmethod
    def _adf_status_inline(colour: str, title: str) -> dict:
        """ADF inline Status macro node."""
        return {
            "type": "inlineExtension",
            "attrs": {
                "extensionType": "com.atlassian.confluence.macro.core",
                "extensionKey":  "status",
                "parameters": {
                    "macroParams": {
                        "colour": {"value": colour},
                        "title":  {"value": title},
                    },
                    "macroMetadata": {},
                },
            },
        }

    @staticmethod
    def _adf_contributors_inline() -> dict:
        """
        ADF inline Contributors macro node.

        Uses the same ``inlineExtension`` pattern as the Status macro so it
        fits inside the Page Properties table cell paragraph.  No macroParams
        are required — the macro auto-discovers contributors from the page
        edit history.
        """
        return {
            "type": "inlineExtension",
            "attrs": {
                "extensionType": "com.atlassian.confluence.macro.core",
                "extensionKey":  "contributors",
                "parameters": {
                    "macroParams": {},
                    "macroMetadata": {
                        "macroId":       {"value": str(uuid.uuid4())},
                        "schemaVersion": {"value": "1"},
                        "title":         "Contributors",
                    },
                },
            },
        }

    @staticmethod
    def _adf_table_row(key: str, value_nodes: list) -> dict:
        """ADF tableRow with one tableHeader (key) and one tableCell (value)."""
        return {
            "type": "tableRow",
            "content": [
                {
                    "type":    "tableHeader",
                    "attrs":   {},
                    "content": [
                        {
                            "type":    "paragraph",
                            "content": [{"type": "text", "text": key}],
                        }
                    ],
                },
                {
                    "type":    "tableCell",
                    "attrs":   {},
                    "content": [
                        {
                            "type":    "paragraph",
                            "content": value_nodes or [{"type": "text", "text": ""}],
                        }
                    ],
                },
            ],
        }

    def _resolve_mention_nodes(self, names_csv: str) -> list:
        """
        Convert a comma-separated list of display names to a list of ADF inline
        nodes (mention nodes where the account ID can be resolved, text nodes as
        fallback).

        This is the ADF equivalent of :meth:`_resolve_mentions`.
        """
        nodes: list = []
        for name in (n.strip() for n in names_csv.split(",") if n.strip()):
            account_id = self._client.find_user_by_name(name)
            if account_id:
                nodes.append(self._adf_mention(account_id, f"@{name}"))
            else:
                nodes.append(self._adf_text(name))
            nodes.append(self._adf_text(" "))
        return nodes

    def generate_metadata_adf_nodes(
        self,
        author_id: str = "",
        approvers: str = "",
        module: str = "",
        version: int = 1,
    ) -> list:
        """
        Build the ADF nodes for the Document Metadata and Change History blocks.

        Returns a **3-item list**:

        1. An ``h4`` heading (text = ``_METADATA_SENTINEL``) — visual label and
           detection/stripping sentinel.
        2. A top-level Page Properties (``details``) ``bodiedExtension`` — the
           metadata table.  **Not** wrapped in an expand; nesting ``details``
           inside an expand triggers Confluence's "This is legacy content" banner.
        3. A collapsible ``expand`` node (title = ``_CHANGE_HISTORY_SENTINEL``)
           that wraps the native Confluence Change History ``extension``.

        Writing in ADF format (``atlas_doc_format``) avoids the legacy banner.
        The ``details`` extension key and ``macroMetadata`` shape are confirmed
        by inspecting the raw ADF of a page saved by Confluence's own editor.

        Args:
            author_id: Atlassian account ID of the page creator.
            approvers: Comma-separated display names of default approvers.
            module:    Module name derived from parent page title.

        Returns:
            List of three ADF nodes ready to prepend to a document's top-level
            ``content`` array.
        """
        author_nodes   = (
            [self._adf_mention(author_id)] if author_id else [self._adf_text("")]
        )
        approver_nodes = (
            self._resolve_mention_nodes(approvers) if approvers else [self._adf_text("")]
        )

        rows = [
            self._adf_table_row("Author",           author_nodes),
            self._adf_table_row("Version",          [self._adf_text(str(version))]),
            self._adf_table_row("Status",           [self._adf_status_inline("Grey", "Draft")]),
            self._adf_table_row("Contributors",     [self._adf_contributors_inline()]),
            self._adf_table_row("Approvers",        approver_nodes),
            self._adf_table_row("Review Round",     [self._adf_text("0")]),
            self._adf_table_row("Last Reviewed",    [self._adf_text("")]),
            self._adf_table_row("Next Review Date", [self._adf_text("")]),
            self._adf_table_row("Module",           [self._adf_text(module or "")]),
            self._adf_table_row("Sensitivity",      [self._adf_text("Internal")]),
        ]

        # h4 heading — serves as the visual label AND the detection/stripping
        # sentinel.  Using a heading (not an expand) means the details macro
        # below it is a direct top-level ADF node, which is required to avoid
        # Confluence's "This is legacy content" banner.
        heading_metadata = {
            "type":    "heading",
            "attrs":   {"level": 4},
            "content": [{"type": "text", "text": _METADATA_SENTINEL}],
        }

        # Page Properties macro — top-level bodiedExtension (NO expand wrapper).
        # "details" is the correct Confluence Cloud extensionKey (confirmed from
        # live ADF).  macroMetadata.title must be a plain string — the
        # {"value": "..."} wrapper applies only to macroParams values.
        details_node = {
            "type": "bodiedExtension",
            "attrs": {
                "extensionType": "com.atlassian.confluence.macro.core",
                "extensionKey":  "details",
                "parameters": {
                    "macroParams": {},
                    "macroMetadata": {
                        "macroId":       {"value": str(uuid.uuid4())},
                        "schemaVersion": {"value": "1"},
                        "title":         "Page Properties",
                    },
                },
                "layout":  "default",
                "localId": str(uuid.uuid4()),
            },
            "content": [
                {
                    "type":    "table",
                    "attrs":   {"isNumberColumnEnabled": False, "layout": "default"},
                    "content": rows,
                }
            ],
        }

        # Native Change History macro — type "extension" (no body), confirmed
        # from live ADF.  Displays Confluence's built-in page version history.
        # Kept inside a collapsible expand (already working correctly).
        change_history_node = {
            "type": "extension",
            "attrs": {
                "extensionType": "com.atlassian.confluence.macro.core",
                "extensionKey":  "change-history",
                "parameters": {
                    "macroParams": {},
                    "macroMetadata": {
                        "macroId":       {"value": str(uuid.uuid4())},
                        "schemaVersion": {"value": "1"},
                        "title":         "Change History",
                    },
                },
                "layout":  "default",
                "localId": str(uuid.uuid4()),
            },
        }

        # Use the native ADF "expand" node type.  Confluence normalises any
        # "bodiedExtension" with extensionKey "expand" back to the native expand
        # type on every GET, so writing it natively avoids a round-trip mismatch.
        expand_history = {
            "type":    "expand",
            "attrs":   {"title": _CHANGE_HISTORY_SENTINEL},
            "content": [change_history_node],
        }

        return [heading_metadata, details_node, expand_history]

    def _has_metadata_adf(self, adf_doc: dict) -> bool:
        """
        Return True if the first top-level node of an ADF document is the
        Document Metadata block written by :meth:`generate_metadata_adf_nodes`.

        Uses JSON serialisation to search for the sentinel, making detection
        robust against Confluence's varying storage→ADF conversion output.

        Handles the current format (``heading`` node containing the sentinel)
        and the previous format (``expand`` / ``extension`` / ``bodiedExtension``
        node containing the sentinel) for backward compatibility.
        """
        import json as _json

        content = adf_doc.get("content", [])
        if not content:
            return False
        first = content[0]
        first_str = _json.dumps(first, ensure_ascii=False)
        # Current format: h4 sentinel heading
        if first.get("type") == "heading":
            return _METADATA_SENTINEL in first_str
        # Previous format: expand / extension / bodiedExtension with sentinel
        if first.get("type") in ("extension", "bodiedExtension", "expand"):
            return _METADATA_SENTINEL in first_str
        return False

    def _strip_metadata_adf(self, adf_doc: dict) -> dict:
        """
        Remove all leading Document Metadata **and** Change History blocks from
        an ADF document.

        Uses JSON serialisation to match sentinels, making the check robust
        against Confluence's varying storage→ADF conversion output.

        **Current format** — each block is a sentinel heading followed
        immediately by its macro node (``bodiedExtension`` or ``extension``).
        When a sentinel heading is found, both the heading and the following
        macro node are popped together.

        **Previous format** — each block was a standalone ``expand`` /
        ``extension`` / ``bodiedExtension`` node containing the sentinel.
        These are popped individually (backward compatibility).

        Safe to call when no metadata blocks are present — the document is
        returned unchanged.
        """
        import json as _json

        _SENTINELS = (_METADATA_SENTINEL, _CHANGE_HISTORY_SENTINEL)
        content = list(adf_doc.get("content", []))
        changed = True
        while changed and content:
            changed = False
            first = content[0]
            first_str = _json.dumps(first, ensure_ascii=False)
            if (first.get("type") == "heading"
                    and any(s in first_str for s in _SENTINELS)):
                content.pop(0)   # remove sentinel heading
                # Also pop the macro node immediately following the heading
                if content and content[0].get("type") in (
                        "extension", "bodiedExtension"):
                    content.pop(0)
                changed = True
            elif (first.get("type") in ("extension", "bodiedExtension", "expand")
                  and any(s in first_str for s in _SENTINELS)):
                content.pop(0)   # previous format: standalone expand/macro
                changed = True
        return {**adf_doc, "content": content}

    def parse_metadata_fields(self, page_body: str) -> dict:
        """
        Extract Document Metadata field values from a page's storage-format body.

        Scans for the two-cell table rows produced by :meth:`generate_metadata_template`:

            <tr><th>Author</th><td>John Smith</td></tr>

        The Change History table uses all-``<th>`` header rows and all-``<td>`` data
        rows, so it is never matched.

        Returns:
            Dict mapping field name → plain-text value, e.g.::

                {"Author": "John Smith", "Version": "1.0", "Status": "Draft", ...}

            Empty dict when the page has no metadata block.
        """
        if _METADATA_SENTINEL not in page_body:
            return {}

        # Match a row with exactly one <th> header followed by one <td> value.
        # re.DOTALL allows the value to span whitespace / embedded tags.
        pattern = r"<tr>\s*<th>\s*(.*?)\s*</th>\s*<td>(.*?)</td>\s*</tr>"
        fields: dict = {}
        for key, raw_value in re.findall(pattern, page_body, re.DOTALL):

            if "<ri:user" in raw_value:
                # User mention field (Author, Approvers, Contributors, Changed By).
                # Reconstruct canonical mention macros from the embedded account IDs
                # so the tracker page can embed them directly in its table cells.
                account_ids = re.findall(r'ri:account-id="([^"]+)"', raw_value)
                clean = ", ".join(
                    f'<ac:link><ri:user ri:account-id="{aid}"/></ac:link>'
                    for aid in account_ids
                )

            elif 'ac:name="status"' in raw_value:
                # Confluence Status macro — extract the title parameter value
                # rather than stripping all tags (which yields "GreyDraft").
                title_m = re.search(
                    r'ac:name="title"[^>]*>\s*(.*?)\s*<', raw_value, re.DOTALL
                )
                clean = unescape(title_m.group(1)) if title_m else ""

            else:
                # Generic plain-text field — strip XML/HTML tags
                clean = unescape(re.sub(r"<[^>]+>", "", raw_value).strip())

            # Strip any HTML/XML tags from the key — ADF-written pages produce
            # <th><p>Author</p></th> in storage format, so the captured key group
            # may be "<p>Author</p>" rather than "Author".
            fields[re.sub(r"<[^>]+>", "", key).strip()] = clean
        return fields

    def generate_metadata_template(
        self,
        author: str = "",
        author_id: str = "",
        approvers_html: str = "",
        module: str = "",
    ) -> str:
        """
        Build the Confluence storage-format XML for both Expand blocks.

        Args:
            author:        Page creator's display name — used as plain-text
                           fallback when ``author_id`` is not available.
            author_id:     Atlassian account ID of the page creator.  When
                           provided the Author and Changed By cells render as
                           native Confluence @mention pills instead of text.
            approvers_html: Pre-resolved HTML for the Approvers cell.  Pass the
                           output of :meth:`_resolve_mentions` here — it may
                           contain ``<ac:link>`` mention macros, plain-text
                           names, or a mix.  The value is inserted verbatim
                           (no additional HTML-escaping).
            module:        Module name derived from parent page title.

        Returns a string that can be prepended to an existing page body.
        The Change History initial row uses today's date.
        """
        today = date.today().isoformat()   # YYYY-MM-DD

        # Escape plain-text values that go directly into XML text nodes
        def _esc(s: str) -> str:
            return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

        safe_module = _esc(module)

        # Author / Changed By — prefer mention macro, fall back to escaped name
        if author_id:
            author_cell = self._make_mention(author_id)
        else:
            author_cell = _esc(author)

        return f"""\
<ac:structured-macro ac:name="expand" ac:schema-version="1">
  <ac:parameter ac:name="title">📋 Document Metadata</ac:parameter>
  <ac:rich-text-body>
    <ac:structured-macro ac:name="details" ac:schema-version="1">
      <ac:rich-text-body>
        <table>
          <tbody>
            <tr><th>Author</th><td>{author_cell}</td></tr>
            <tr><th>Version</th><td>1.0</td></tr>
            <tr><th>Status</th><td><ac:structured-macro ac:name="status" ac:schema-version="1"><ac:parameter ac:name="colour">Grey</ac:parameter><ac:parameter ac:name="title">Draft</ac:parameter></ac:structured-macro></td></tr>
            <tr><th>Contributors</th><td><ac:structured-macro ac:name="contributors" ac:schema-version="1"/></td></tr>
            <tr><th>Approvers</th><td>{approvers_html}</td></tr>
            <tr><th>Review Round</th><td>0</td></tr>
            <tr><th>Last Reviewed</th><td></td></tr>
            <tr><th>Next Review Date</th><td></td></tr>
            <tr><th>Module</th><td>{safe_module}</td></tr>
            <tr><th>Sensitivity</th><td>Internal</td></tr>
          </tbody>
        </table>
      </ac:rich-text-body>
    </ac:structured-macro>
  </ac:rich-text-body>
</ac:structured-macro>
<ac:structured-macro ac:name="expand" ac:schema-version="1">
  <ac:parameter ac:name="title">🕓 Change History</ac:parameter>
  <ac:rich-text-body>
    <table>
      <tbody>
        <tr>
          <th>Version</th>
          <th>Date</th>
          <th>Changed By</th>
          <th>Sections Updated</th>
          <th>Reason for Change</th>
          <th>Change Type</th>
          <th>Review Round</th>
        </tr>
        <tr>
          <td>1.0</td>
          <td>{today}</td>
          <td>{author_cell}</td>
          <td>Initial creation</td>
          <td>Initial draft</td>
          <td>Major</td>
          <td>0</td>
        </tr>
      </tbody>
    </table>
  </ac:rich-text-body>
</ac:structured-macro>
"""

    def apply_to_page(
        self,
        page_id: str,
        force: bool = False,
        approvers: str = "",
        module: str = "",
        label: str = "",
    ) -> dict:
        """
        Prepend the Document Metadata block to a single Confluence page.

        The block is written in **ADF format** (``atlas_doc_format``) so that
        Confluence renders it as the new "Content properties (Page properties)"
        macro without the "This is legacy content" banner.

        Confluence's native page version history is used as the change log —
        no custom Change History table is added to the page body.

        Author is resolved automatically from the page's creator account ID.

        **Dual-GET strategy:**
        * A first GET in *storage* format is used for idempotency detection.
          ``has_metadata_blocks()`` searches the raw XML for the sentinel string,
          which is reliable even when the page was originally written in storage
          format (old blocks) or ADF-converted back to storage by Confluence.
        * A second GET in *atlas_doc_format* is used for structural manipulation
          (stripping old blocks, prepending the new ADF node, writing back).

        Args:
            page_id:   Confluence page ID.
            force:     When True, re-apply even if blocks are already present.
            approvers: Default approvers string to pre-populate.
            module:    Module name to pre-populate (derived from parent page).

        Returns:
            dict with keys:
                page_id (str)
                title   (str)
                status  ("applied" | "skipped" | "error")
                error   (str, only when status="error")
        """
        import json as _json

        # ── 1. GET storage — reliable sentinel detection ──────────────────────
        # Confluence auto-converts ADF→storage on read, so this path works
        # regardless of whether the page was last written in storage or ADF.
        try:
            page_s = self._client.get_page(page_id, fmt="storage")
        except ConfluenceAPIError as exc:
            return {
                "page_id": page_id,
                "title":   "",
                "status":  "error",
                "error":   str(exc),
            }

        title       = page_s["title"]
        already_has = self.has_metadata_blocks(page_s["body"])

        if already_has and not force:
            return {"page_id": page_id, "title": title, "status": "skipped"}

        # ── 2. GET ADF — structural manipulation ─────────────────────────────
        try:
            page_a = self._client.get_page(page_id, fmt="atlas_doc_format")
        except ConfluenceAPIError as exc:
            return {
                "page_id": page_id,
                "title":   title,
                "status":  "error",
                "error":   str(exc),
            }

        raw = page_a.get("body", "")
        adf_doc: dict = {}
        if raw:
            try:
                adf_doc = _json.loads(raw)
            except ValueError:
                pass

        # Safety guard: don't overwrite with a blank document
        if not adf_doc:
            return {
                "page_id": page_id,
                "title":   title,
                "status":  "error",
                "error":   "Could not retrieve ADF body from Confluence API. "
                           "The page may contain content that cannot be read in "
                           "atlas_doc_format.",
            }

        # ── 3. Strip old metadata + Change History blocks ─────────────────────
        # Uses JSON-search so both storage-converted and native ADF blocks are
        # matched, including the legacy "🕓 Change History" expand block.
        if already_has:
            adf_doc = self._strip_metadata_adf(adf_doc)

        # ── 4. Prepend new ADF metadata block ─────────────────────────────────
        # Prefer author_id from the storage GET (more reliable); fall back to ADF.
        author_id = page_s.get("author_id", "") or page_a.get("author_id", "")
        new_nodes = self.generate_metadata_adf_nodes(
            author_id=author_id,
            approvers=approvers,
            module=module,
            version=page_s["version"] + 1,
        )
        adf_doc["content"] = new_nodes + adf_doc.get("content", [])

        # ── 5. Write back in ADF format (version from storage GET) ────────────
        try:
            self._client.update_page(
                page_id=page_id,
                title=title,
                content=_json.dumps(adf_doc),
                current_version=page_s["version"],
                representation="atlas_doc_format",
            )
        except ConfluenceAPIError as exc:
            return {
                "page_id": page_id,
                "title":   title,
                "status":  "error",
                "error":   str(exc),
            }

        # ── 6. Optionally add tracking label (non-fatal) ─────────────────────
        # The label is what the details-summary macro uses to find this page.
        # Label failure must NOT prevent the "applied" status being returned —
        # the page content has already been written successfully.
        if label:
            try:
                self._client.add_label_to_page(page_id, label)
            except Exception as exc:
                return {
                    "page_id":       page_id,
                    "title":         title,
                    "status":        "applied",
                    "label_warning": f"Label '{label}' could not be added: {exc}",
                }

        return {"page_id": page_id, "title": title, "status": "applied"}

    def preview_scope(self, parent_urls: List[str]) -> List[dict]:
        """
        Return a list of all pages in scope (descendants of each parent URL).

        Each entry contains:
            page_id    (str)
            title      (str)
            url        (str)
            module     (str)   — title of the parent URL page
            has_blocks (bool)  — whether the page already has metadata blocks
            error      (str | None)

        Does not modify any pages.
        """
        pages: List[dict] = []
        seen: set = set()

        for url in parent_urls:
            url = url.strip()
            if not url:
                continue

            try:
                parent_id = self.extract_page_id_from_url(url)
            except ValueError as exc:
                pages.append({
                    "page_id": "",
                    "title": url,
                    "url": url,
                    "module": "",
                    "has_blocks": False,
                    "error": str(exc),
                })
                continue

            # Resolve the parent page title → becomes the Module for all descendants
            module_name = ""
            try:
                parent_page = self._client.get_page(parent_id)
                module_name = parent_page["title"]
            except ConfluenceAPIError:
                pass   # Fall back to empty module if parent can't be fetched

            try:
                descendants = self._client.get_all_descendants(parent_id)
            except ConfluenceAPIError as exc:
                pages.append({
                    "page_id": parent_id,
                    "title": f"(parent {parent_id})",
                    "url": url,
                    "module": module_name,
                    "has_blocks": False,
                    "error": str(exc),
                })
                continue

            for desc in descendants:
                pid = desc["id"]
                if pid in seen:
                    continue
                seen.add(pid)

                has_blocks = False
                error_msg = None
                try:
                    page_data = self._client.get_page(pid)
                    has_blocks = self.has_metadata_blocks(page_data["body"])
                except ConfluenceAPIError as exc:
                    error_msg = str(exc)

                page_url = f"{self._base_url}/wiki/pages/{pid}"
                pages.append({
                    "page_id": pid,
                    "title": desc["title"],
                    "url": page_url,
                    "module": module_name,
                    "has_blocks": has_blocks,
                    "error": error_msg,
                })

        return pages

    def apply_to_scope(
        self,
        parent_urls: List[str],
        force: bool = False,
        default_approvers: str = "",
        label: str = "",
    ) -> Generator[dict, None, None]:
        """
        Apply metadata blocks to all descendant pages under each parent URL.

        Per-page values set automatically:
          - Author / Changed By  resolved from the Confluence user API
          - Module               derived from the parent URL page's title

        Yields progress event dicts as each page is processed:
            {type: "start",    total: N}
            {type: "progress", page_id, title, status, current, total}
            {type: "complete", applied, skipped, errors, total}

        Args:
            parent_urls:       List of Confluence page URLs.
            force:             Re-apply to pages that already have blocks.
            default_approvers: Default approvers string to pre-populate.
            label:             Confluence label to add to each page after
                               applying metadata (used by the live
                               details-summary macro on the tracker page).
                               Non-fatal if labelling fails.
        """
        # Collect all page IDs with their module assignment first
        all_pages: List[dict] = []   # [{id, title, parent_id, module}, ...]
        seen: set = set()

        for url in parent_urls:
            url = url.strip()
            if not url:
                continue

            try:
                parent_id = self.extract_page_id_from_url(url)
            except ValueError:
                continue

            # Resolve module name from the parent page title
            module_name = ""
            try:
                parent_page = self._client.get_page(parent_id)
                module_name = parent_page["title"]
            except ConfluenceAPIError:
                pass

            # Include the parent page itself so module pages also receive metadata
            if parent_id not in seen:
                seen.add(parent_id)
                all_pages.append({
                    "id":     parent_id,
                    "title":  module_name,
                    "module": module_name,
                })

            try:
                descendants = self._client.get_all_descendants(parent_id)
            except ConfluenceAPIError:
                continue

            for desc in descendants:
                if desc["id"] not in seen:
                    seen.add(desc["id"])
                    all_pages.append({**desc, "module": module_name})

        total = len(all_pages)
        yield {"type": "start", "total": total}

        applied = skipped = errors = 0

        for i, page_info in enumerate(all_pages, start=1):
            result = self.apply_to_page(
                page_info["id"],
                force=force,
                approvers=default_approvers,
                module=page_info["module"],
                label=label,
            )
            result["current"] = i
            result["total"] = total
            result["type"] = "progress"

            if result["status"] == "applied":
                applied += 1
            elif result["status"] == "skipped":
                skipped += 1
            else:
                errors += 1

            yield result

        yield {
            "type": "complete",
            "applied": applied,
            "skipped": skipped,
            "errors": errors,
            "total": total,
        }

    def _derive_page_labels(self, title: str, module: str) -> List[str]:
        """
        Derive smart Confluence labels from a page title and its module name.

        Rules:
          1. **Module label** — slugified module name
             e.g. ``"Contract Budget"`` → ``"contract-budget"``
          2. **Page-type label** — the last ``" - X"`` segment of the title is
             mapped to a canonical slug via ``TYPE_MAP``; unknown types are
             auto-slugified.

        Known canonical mappings in ``TYPE_MAP``:
            "business process"       → "business-process"
            "use case" / "use cases" → "use-case"
            "screen design(s)"       → "screen-designs"
            "screen mockup(s)"       → "screen-mockup"
            "functional description" → "functional-description"
            "process flow"           → "process-flow"
            "data flow"              → "data-flow"

        Returns:
            Ordered list of label slugs (no duplicates).
        """
        TYPE_MAP = {
            "business process":       "business-process",
            "use case":               "use-case",
            "use cases":              "use-case",
            "screen design":          "screen-designs",
            "screen designs":         "screen-designs",
            "screen mockup":          "screen-mockup",
            "screen mockups":         "screen-mockup",
            "functional description": "functional-description",
            "process flow":           "process-flow",
            "data flow":              "data-flow",
            "module":                 "module",
        }
        labels: List[str] = []

        # 1. Module label
        if module:
            m_slug = _to_label_slug(module)
            if m_slug:
                labels.append(m_slug)

        # 2. Page-type label — last segment after any dash / em-dash separator
        parts = re.split(r"\s*[–—-]\s*", title.strip())
        if len(parts) >= 2:
            last = parts[-1].strip().lower()
            for keyword, canonical in TYPE_MAP.items():
                if keyword in last:
                    if canonical not in labels:
                        labels.append(canonical)
                    break
            else:
                auto = _to_label_slug(parts[-1])
                if auto and auto not in labels:
                    labels.append(auto)

        return labels

    def auto_label_scope(
        self,
        parent_urls: List[str],
    ) -> Generator[dict, None, None]:
        """
        Add module + page-type labels to all descendant pages under each parent URL.

        The module name is derived from each parent page's own title (identical
        to how :meth:`apply_to_scope` works).  Existing labels are fetched
        first — only labels that are not yet present on a page are added, so
        the operation is safe to run multiple times without creating duplicates.

        Yields progress events:
            ``{type: "start",    total: N}``
            ``{type: "progress", page_id, title, new_labels, already_had, status, current, total}``
            ``{type: "complete", labeled, unchanged, errors, total}``

        Args:
            parent_urls: List of Confluence page URLs whose descendants should
                         be labelled.
        """
        all_pages: List[dict] = []
        seen: set = set()

        for url in parent_urls:
            url = url.strip()
            if not url:
                continue
            try:
                parent_id = self.extract_page_id_from_url(url)
            except ValueError:
                continue

            module_name = ""
            raw_title   = ""
            try:
                parent_page = self._client.get_page(parent_id)
                raw_title   = parent_page["title"]
                # Strip the page-type suffix to get a clean module name.
                # e.g. "Contract Budget - Module" → "Contract Budget"
                # e.g. "Contract Budget Modification" (no suffix) → unchanged
                title_parts = re.split(r"\s*[–—-]\s*", raw_title.strip())
                module_name = title_parts[0].strip() if len(title_parts) >= 2 else raw_title
            except Exception:
                pass

            # Include the parent page itself so module pages are also labelled
            if raw_title and parent_id not in seen:
                seen.add(parent_id)
                all_pages.append({
                    "id":     parent_id,
                    "title":  raw_title,
                    "module": module_name,
                })

            try:
                descendants = self._client.get_all_descendants(parent_id)
            except Exception:
                continue

            for desc in descendants:
                if desc["id"] not in seen:
                    seen.add(desc["id"])
                    all_pages.append({**desc, "module": module_name})

        total = len(all_pages)
        yield {"type": "start", "total": total}

        labeled = unchanged = errors = 0

        for i, page_info in enumerate(all_pages, start=1):
            page_id = page_info["id"]
            title   = page_info["title"]
            module  = page_info["module"]
            event: dict = {
                "type":       "progress",
                "page_id":    page_id,
                "title":      title,
                "new_labels": [],
                "already_had": [],
                "current":    i,
                "total":      total,
            }
            try:
                desired  = self._derive_page_labels(title=title, module=module)
                existing = set(self._client.get_page_labels(page_id))
                to_add   = [lbl for lbl in desired if lbl not in existing]
                already  = [lbl for lbl in desired if lbl in existing]

                if to_add:
                    self._client.add_labels_to_page(page_id, to_add)
                    labeled += 1
                else:
                    unchanged += 1

                event["new_labels"]  = to_add
                event["already_had"] = already
                event["status"]      = "labeled" if to_add else "unchanged"
            except Exception as exc:
                event["status"] = "error"
                event["error"]  = str(exc)
                errors += 1

            yield event

        yield {
            "type":      "complete",
            "labeled":   labeled,
            "unchanged": unchanged,
            "errors":    errors,
            "total":     total,
        }

    def create_or_update_tracker_page(
        self,
        space_key: str,
        tracker_title: str = "DS Review Tracking Dashboard",
        parent_page_id: Optional[str] = None,
        label: str = "ds-tracked",
    ) -> dict:
        """
        Create (or update) a Confluence page with a three-section review tracker.

        The tracker page contains:
          1. **Static Status Summary** — count table (status → pages), generated
             at click time from a live scan of all space pages.
          2. **Static Bar Chart** — ``chart`` macro with an embedded counts table,
             also generated at click time.
          3. **Live Page Properties Report** — native ``details-summary`` macro
             filtered by ``space_key`` + ``label``.  This section auto-updates
             every time the page is viewed in Confluence without needing a button
             click.

        The ``details-summary`` macro reads from the ``details`` (Page Properties)
        macro that ``apply_to_page()`` embeds on each content page.  Pages must
        also carry the ``label`` Confluence label (added by ``apply_to_page()``)
        so the macro can find them.

        Algorithm:
        1. Fetch all pages in the space using ``get_all_pages_in_space()``.
        2. Filter those that contain the Document Metadata block (sentinel check).
        3. Parse each page's metadata fields with :meth:`parse_metadata_fields`.
        4. Sort rows by Module → Page Title.
        5. Build Sections 1 & 2 from the parsed data (static snapshot).
        6. Build Section 3 as a ``details-summary`` macro (live, label-filtered).
        7. Create or update the tracker page with the rendered content.

        Args:
            space_key:       Target Confluence space key (e.g. "DS").
            tracker_title:   Title for the tracker page.
            parent_page_id:  Optional parent page ID.  When None the page is
                             placed at the space root.
            label:           Confluence label used to filter pages in the live
                             ``details-summary`` macro.  Must match the label
                             passed to ``apply_to_page()`` / ``apply_to_scope()``.
                             Default: ``"ds-tracked"``.

        Returns:
            dict with keys: id, title, url
        """
        # ── 1. Gather all pages with metadata blocks ──────────────────────────
        all_pages = self._client.get_all_pages_in_space(space_key)

        rows: List[dict] = []
        for page in all_pages:
            if not self.has_metadata_blocks(page["body"]):
                continue
            if page["title"] == tracker_title:
                continue   # exclude the tracker page itself
            fields = self.parse_metadata_fields(page["body"])
            page_url = (
                f"{self._base_url}/wiki/spaces/{space_key}/pages/{page['id']}"
            )
            rows.append({
                "id":      page["id"],
                "title":   page["title"],
                "url":     page_url,
                "fields":  fields,
                "version": page.get("version", 0),
            })

        # Sort: Module (blank last) then Page Title
        rows.sort(key=lambda r: (
            r["fields"].get("Module", "zzz") or "zzz",
            r["title"].lower(),
        ))

        # ── Shared helpers ─────────────────────────────────────────────────────
        _STATUS_COLOURS = {
            "draft":      "Grey",
            "in review":  "Yellow",
            "review r1":  "Yellow",
            "review r2":  "Yellow",
            "approved":   "Green",
            "deprecated": "Red",
        }

        def _status_macro(status: str) -> str:
            colour    = _STATUS_COLOURS.get(status.strip().lower(), "Grey")
            label_txt = status.strip() or "—"
            return (
                f'<ac:structured-macro ac:name="status" ac:schema-version="1">'
                f'<ac:parameter ac:name="colour">{colour}</ac:parameter>'
                f'<ac:parameter ac:name="title">{label_txt}</ac:parameter>'
                f'</ac:structured-macro>'
            )

        COLUMNS = [
            "Author", "Status",
            "Approvers", "Review Round", "Last Reviewed",
            "Next Review Date", "Sensitivity",
        ]

        from collections import Counter
        status_counts: Counter = Counter(
            row["fields"].get("Status", "").strip() or "Unknown"
            for row in rows
        )

        # ── Section 1: Static status summary table ─────────────────────────────
        summary_rows_html = "".join(
            f"<tr><td>{_status_macro(s)}</td>"
            f"<td><strong>{c}</strong></td></tr>"
            for s, c in sorted(status_counts.items())
        ) or '<tr><td colspan="2"><em>No pages tracked yet.</em></td></tr>'

        section_1 = (
            "<h2>&#128202; Status Summary</h2>"
            "<p><em>Snapshot generated at click time — click "
            "&#128202; Create / Update Tracker to refresh.</em></p>"
            "<table><thead><tr>"
            "<th><strong>Status</strong></th>"
            "<th><strong>Pages</strong></th>"
            "</tr></thead><tbody>"
            + summary_rows_html
            + "</tbody></table>"
        )

        # ── Section 2: Bar chart macro (static, embedded data table) ───────────
        chart_rows = "".join(
            f"<tr><td>{s}</td><td>{c}</td></tr>"
            for s, c in sorted(status_counts.items())
        ) or "<tr><td>No data</td><td>0</td></tr>"

        section_2 = (
            "<h2>&#128200; Status Distribution</h2>"
            '<ac:structured-macro ac:name="chart" ac:schema-version="1">'
            '<ac:parameter ac:name="type">bar</ac:parameter>'
            '<ac:parameter ac:name="title">Status Distribution</ac:parameter>'
            '<ac:rich-text-body>'
            '<table><tr><th>Status</th><th>Count</th></tr>'
            + chart_rows
            + "</table>"
            "</ac:rich-text-body>"
            "</ac:structured-macro>"
        )

        # ── Section 3: Live Page Properties Report (details-summary macro) ──────
        # This section auto-updates each time the page is viewed in Confluence.
        # It displays all pages in the space that carry the tracking label and
        # have a ``details`` (Page Properties) macro applied.
        headings_param = ",".join(COLUMNS)
        section_3 = (
            "<h2>&#128203; Live Page Properties Report</h2>"
            "<p>This table updates automatically each time this page is viewed "
            "in Confluence. It shows all pages in this space that have the "
            f"<code>{label}</code> label applied. "
            "If a page is missing, re-apply its metadata blocks via the "
            "Metadata Manager (use <em>Force re-apply</em> if already applied) "
            "to ensure the label is added.</p>"
            '<ac:structured-macro ac:name="details-summary" ac:schema-version="1">'
            f'<ac:parameter ac:name="headings">{headings_param}</ac:parameter>'
            f'<ac:parameter ac:name="spaces">{space_key}</ac:parameter>'
            f'<ac:parameter ac:name="labels">{label}</ac:parameter>'
            "</ac:structured-macro>"
        )

        # ── Compose full page content ──────────────────────────────────────────
        updated_at = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
        content = (
            f"<p>Review status for all pages in the "
            f"<strong>{space_key}</strong> space. "
            f"Sections 1&#8211;2 are a static snapshot "
            f"(<em>last refreshed: {updated_at}</em>). "
            f"Section 3 is a live view that auto-updates on every page view.</p>\n"
            f"<p><strong>{len(rows)}</strong> page(s) tracked at snapshot time.</p>\n"
            f"{section_1}\n"
            f"{section_2}\n"
            f"{section_3}\n"
        )

        # ── Create or update the tracker page ─────────────────────────────────
        existing = self._client.get_page_by_title(space_key, tracker_title)
        if existing:
            result  = self._client.update_page(
                page_id=existing["id"],
                title=tracker_title,
                content=content,
                current_version=existing["version"],
                representation="storage",
            )
            page_id = result["id"]
        else:
            result  = self._client.create_page(
                space_key=space_key,
                title=tracker_title,
                content=content,
                parent_id=parent_page_id,
                representation="storage",
            )
            page_id = result["id"]

        return {
            "id":    page_id,
            "title": tracker_title,
            "url":   f"{self._base_url}/wiki/spaces/{space_key}/pages/{page_id}",
        }
