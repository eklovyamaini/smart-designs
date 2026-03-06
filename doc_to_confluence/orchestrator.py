"""
Main migration orchestrator for the doc_to_confluence tool.

Coordinates the full pipeline:
  1. Parse Word document → ParsedSection tree
  2. Flatten to list for section matching
  3. For each SectionMapping in config:
     a. Find matching section in parsed document
     b. Run LLM processing pipeline (if enabled)
     c. Execute Confluence action (create / update / append)
     d. Record result
  4. Log run to SQLite (mirrors smart_agents/tools.py patterns)
  5. Print summary

Per-section errors are caught and logged without stopping the migration.
LLMProcessor and ConfluenceClient are lazily instantiated.
"""
import base64
import os
import re
import sqlite3
from datetime import datetime, timezone
from typing import List, Optional, Tuple

from .config import MigrationConfigModel, SectionMappingModel
from .confluence_client import ConfluenceAPIError, ConfluenceClient
from .llm_processor import LLMProcessor
from .models import LLMResult, MigrationReport, ParsedSection, SectionResult
from .parser import parse_docx
from .plantuml_renderer import render_and_embed_plantuml_diagrams


class MigrationOrchestrator:
    """Orchestrates the full Word-to-Confluence migration pipeline."""

    def __init__(
        self,
        config: MigrationConfigModel,
        dry_run: bool = False,
        verbose: bool = False,
        overwrite: bool = False,
        pre_delete: bool = True,
    ) -> None:
        """
        Args:
            config:     Validated MigrationConfigModel
            dry_run:    If True, parse and LLM-process but do not push to Confluence
            verbose:    If True, print parsed sections and LLM outputs
            overwrite:  If True, delete any existing Confluence page with the same
                        title before re-creating it (prevents duplicate pages on
                        re-upload).  Ignored when dry_run=True.
            pre_delete: If True, delete the entire module folder hierarchy in
                        Confluence before starting migration, giving every run a
                        clean slate.  Ignored when dry_run=True.  Defaults to True.
        """
        self._config = config
        self._dry_run = dry_run
        self._verbose = verbose
        self._overwrite = overwrite
        self._pre_delete = pre_delete
        self._llm: Optional[LLMProcessor] = None
        self._confluence: Optional[ConfluenceClient] = None

        if config.db_logging:
            self._init_db()

    # ─── Public Entry Point ──────────────────────────────────────────────────

    def run(self, doc_path: str, config_path: str = "") -> MigrationReport:
        """
        Execute the full migration pipeline.

        Args:
            doc_path: Path to the .docx file
            config_path: Path to the config YAML (for logging only)

        Returns:
            MigrationReport with results for every configured section mapping
        """
        started_at = _now_iso()
        print(f"[orchestrator] Starting migration: {doc_path}")
        if self._dry_run:
            print("[orchestrator] DRY RUN mode: no Confluence writes will occur")

        # Step 0: Pre-delete module folder hierarchy (if enabled and not dry-run)
        if self._pre_delete and not self._dry_run:
            print("[orchestrator] pre_delete=True: wiping existing module pages before migration...")
            self._pre_delete_module_pages()

        # Step 1: Parse document
        print("[orchestrator] Parsing Word document...")
        top_level_sections = parse_docx(doc_path)
        all_sections = _flatten_sections(top_level_sections)
        print(f"[orchestrator] Parsed {len(all_sections)} sections total")

        print("[orchestrator] Document section+table audit:")
        for s in all_sections:
            indent = "  " * max(0, s["level"] - 1)
            print(f"  [doc] {indent}[{s['id']}] H{s['level']} '{s['title']}' — {len(s.get('tables', []))} table(s), {len(s.get('children', []))} child(ren)")
            for t_idx, tbl in enumerate(s.get("tables", [])):
                rows = tbl.get("rows", [])
                hdr = rows[0] if rows else []
                print(f"  [doc] {indent}  table[{t_idx}]: {len(rows)} rows | headers: {hdr}")

        # Step 2: Process each mapping
        results: List[SectionResult] = []
        matched_section_ids: set = set()   # tracks which doc sections were claimed
        # Level-aware folder stack: list of (level, folder_name) pairs.
        # Rule: when a folder_only section at level N is encountered, pop only
        # entries that are STRICTLY DEEPER (level > N), then push the new entry.
        # This means same-level and shallower folders stay on the stack, so
        # consecutive H1s nest inside each other (each one goes deeper).
        # Example with all H1s selected as folder-only:
        #   H1 "Module A"  → stack: [(1,"Module A")]           → path: Module A
        #   H1 "Process"   → stack: [(1,"Module A"),(1,"Process")] → path: Module A/Process
        #   H2 "Sub"       → stack: [...,(2,"Sub")]             → path: Module A/Process/Sub
        #   H1 "Module B"  → pops (2,"Sub"),(1,"Process") → stack: [(1,"Module A"),(1,"Module B")]
        #                                                    → path: Module A/Module B
        # To reset to root level, leave a gap in the list (don't mark as folder-only).
        folder_stack: List[tuple] = []   # [(level, name), …]

        for i, mapping in enumerate(self._config.sections):
            print(
                f"\n[orchestrator] Mapping {i + 1}/{len(self._config.sections)}: "
                f"match='{mapping.match}' action='{mapping.confluence.action}'"
            )
            inherited_folder = "/".join(name for _, name in folder_stack) or None
            result = self._process_mapping(
                mapping, all_sections, inherited_folder=inherited_folder
            )
            if result.get("section_id"):
                matched_section_ids.add(result["section_id"])
            results.append(result)
            _print_result(result)

            # If this was a folder_only mapping, push it onto the level stack
            if mapping.confluence.folder_only:
                new_folder = mapping.confluence.folder_path
                if not new_folder and result.get("section_title"):
                    new_folder = result["section_title"]
                if new_folder:
                    level = mapping.level or 1
                    # Only pop entries that are strictly deeper than current level
                    while folder_stack and folder_stack[-1][0] > level:
                        folder_stack.pop()
                    folder_stack.append((level, new_folder))
                    inherited_folder = "/".join(name for _, name in folder_stack)
                    print(
                        f"[orchestrator] Folder stack → '{inherited_folder}' "
                        f"(level={level})"
                    )

        # Step 2b: Audit — sections and tables not claimed by any mapping
        unmatched = [s for s in all_sections if s["id"] not in matched_section_ids]
        if unmatched:
            print(f"\n[audit] {len(unmatched)} section(s) not matched by any mapping:")
            for s in unmatched:
                indent = "  " * max(0, s["level"] - 1)
                tbl_summary = f"{len(s['tables'])} table(s)" if s.get("tables") else "no tables"
                print(f"  [audit] {indent}UNMATCHED [{s['id']}] H{s['level']} '{s['title']}' — {tbl_summary}")
                for t_idx, tbl in enumerate(s.get("tables", [])):
                    rows = tbl.get("rows", [])
                    hdr = rows[0] if rows else []
                    print(f"  [audit] {indent}  table[{t_idx}]: {len(rows)} rows | headers: {hdr}")
        else:
            print("\n[audit] All document sections were matched by a mapping.")

        # Step 3: Build report
        finished_at = _now_iso()
        report = MigrationReport(
            doc_path=os.path.abspath(doc_path),
            config_path=config_path,
            total_sections_in_doc=len(all_sections),
            total_mappings=len(self._config.sections),
            results=results,
            dry_run=self._dry_run,
            started_at=started_at,
            finished_at=finished_at,
        )

        # Step 4: Log to SQLite
        if self._config.db_logging:
            self._log_report_to_db(report)

        _print_summary(report)
        return report

    # ─── Private: Pre-Delete Module ──────────────────────────────────────────

    def _pre_delete_module_pages(self) -> None:
        """
        Delete all Confluence pages under every unique root module folder
        referenced in the config, before migration starts.

        The module root folder is the FIRST segment of each section's
        confluence.folder_path (e.g. "My App - Module" from
        "My App - Module/My App - Screen Designs/...").

        Pages are deleted deepest-first (reversed BFS order from
        get_all_descendants) so Confluence does not reject parent deletions.

        NOTE: Uses space-wide title lookup — ensure module folder names are
        unique within each space.
        """
        client = self._get_confluence()

        # Collect unique (space_key, root_folder_title) pairs
        roots: set = set()
        for mapping in self._config.sections:
            space_key   = mapping.confluence.space_key
            folder_path = mapping.confluence.folder_path
            if not space_key or not folder_path:
                continue
            first_segment = folder_path.split("/")[0].strip()
            if first_segment:
                roots.add((space_key, first_segment))

        if not roots:
            print("[orchestrator] pre_delete: no folder paths configured, skipping.")
            return

        for space_key, root_title in sorted(roots):
            print(
                f"[orchestrator] pre_delete: looking for '{root_title}' "
                f"in space '{space_key}'..."
            )
            try:
                root_page = client.get_page_by_title(space_key, root_title)
            except Exception as exc:
                print(f"[orchestrator] pre_delete: WARN: lookup failed for '{root_title}': {exc}")
                continue

            if root_page is None:
                print(
                    f"[orchestrator] pre_delete: '{root_title}' not found "
                    f"in space '{space_key}' — nothing to delete."
                )
                continue

            root_id = root_page["id"]
            try:
                descendants = client.get_all_descendants(root_id)
            except Exception as exc:
                print(f"[orchestrator] pre_delete: WARN: could not fetch descendants of '{root_title}': {exc}")
                continue

            # Delete deepest pages first (reverse BFS order = leaves first)
            for page in reversed(descendants):
                print(
                    f"[orchestrator] pre_delete:   deleting "
                    f"'{page['title']}' (id={page['id']})"
                )
                try:
                    client.delete_page(page["id"])
                except Exception as exc:
                    print(f"[orchestrator] pre_delete: WARN: could not delete '{page['title']}': {exc}")

            # Delete root folder page last
            print(
                f"[orchestrator] pre_delete: deleting root folder "
                f"'{root_title}' (id={root_id})"
            )
            try:
                client.delete_page(root_id)
            except Exception as exc:
                print(f"[orchestrator] pre_delete: WARN: could not delete root '{root_title}': {exc}")
                continue

            print(
                f"[orchestrator] pre_delete: deleted {len(descendants) + 1} page(s) "
                f"under '{root_title}' in space '{space_key}'."
            )

    # ─── Private: Process One Mapping ────────────────────────────────────────

    def _process_mapping(
        self,
        mapping: SectionMappingModel,
        all_sections: List[ParsedSection],
        inherited_folder: Optional[str] = None,
    ) -> SectionResult:
        """Find the matching section, run LLM, push to Confluence. Always returns a result."""
        # Find matching section
        matched = self._find_matching_section(mapping, all_sections)
        if matched is None:
            print(f"  [orchestrator] WARN: No section matched '{mapping.match}' - skipping")
            return SectionResult(
                section_id="",
                section_title="",
                mapping_match=mapping.match,
                action=mapping.confluence.action,
                llm_results=[],
                confluence_page_id=None,
                confluence_page_url=None,
                status="skipped",
                error=f"No section in document matched '{mapping.match}'",
            )

        print(f"  [orchestrator] Matched: [{matched['id']}] {matched['title']!r}")

        # ── Per-section audit: rules applied ──────────────────────────────────
        _tbls = matched.get("tables", [])
        _kids = matched.get("children", [])
        print(f"  [audit] section '{matched['title']}' [{matched['id']}] H{matched['level']} — {len(_tbls)} table(s), {len(_kids)} child section(s)")
        for _ti, _tbl in enumerate(_tbls):
            _rows = _tbl.get("rows", [])
            _hdr  = _rows[0] if _rows else []
            print(f"  [audit]   table[{_ti}]: {len(_rows)} rows | headers: {_hdr}")
        print(f"  [audit]   rules → folder_only={mapping.confluence.folder_only} | action={mapping.confluence.action} | folder_path={mapping.confluence.folder_path!r}")
        print(f"  [audit]          llm.enabled={mapping.llm.enabled} | llm.tasks={mapping.llm.tasks} | expand_tables_to_pages={mapping.llm.expand_tables_to_pages}")
        print(f"  [audit]          table_rows_to_pages={mapping.confluence.table_rows_to_pages} | row_page_title={mapping.confluence.row_page_title!r}")
        # ── end audit ──────────────────────────────────────────────────────────

        # folder_only: resolve/create the folder path but skip page creation
        if mapping.confluence.folder_only:
            folder_name = mapping.confluence.folder_path or matched["title"]
            print(
                f"  [orchestrator] folder_only=True — treating '{folder_name}' as folder, "
                f"no page will be created."
            )
            if not self._dry_run and mapping.confluence.space_key:
                try:
                    client = self._get_confluence()
                    client.resolve_or_create_folder_path(
                        space_key=mapping.confluence.space_key,
                        folder_path=folder_name,
                        root_parent_id=mapping.confluence.parent_page_id,
                    )
                except Exception as exc:
                    print(f"  [orchestrator] WARN: folder creation failed: {exc}")

            # NEW: table_rows_to_pages — create one page per data row in section tables
            if (
                mapping.confluence.table_rows_to_pages
                and matched.get("tables")
                and not self._dry_run
                and mapping.confluence.space_key
            ):
                try:
                    base_path   = (mapping.confluence.folder_path or "").rstrip("/")
                    elem_title  = mapping.confluence.page_title or matched["title"]
                    elem_folder = f"{base_path}/{elem_title}" if base_path else elem_title
                    self._create_table_row_pages(mapping, matched, elem_folder)
                except Exception as exc:
                    print(f"  [orchestrator] WARN: table-rows-to-pages failed: {exc}")

            return SectionResult(
                section_id=matched["id"],
                section_title=matched["title"],
                mapping_match=mapping.match,
                action="create",
                llm_results=[],
                confluence_page_id=None,
                confluence_page_url=None,
                status="skipped",
                error="folder_only — no page created",
            )

        # Apply inherited folder context if this mapping has no explicit folder_path
        if inherited_folder and not mapping.confluence.folder_path:
            # Temporarily inject the inherited folder into the mapping's confluence target
            # We use a local variable rather than mutating the Pydantic model
            effective_folder_path = inherited_folder
            print(
                f"  [orchestrator] Inheriting folder context '{inherited_folder}' "
                f"for section '{matched['title']}'"
            )
        else:
            effective_folder_path = mapping.confluence.folder_path

        # Run LLM pipeline
        final_content: str
        llm_results: List[LLMResult] = []

        if mapping.llm.enabled and mapping.llm.tasks:
            print(f"  [orchestrator] LLM tasks: {mapping.llm.tasks}")
            try:
                llm = self._get_llm()
                final_content, llm_results = llm.process_section(
                    matched,
                    mapping.llm.tasks,
                    verbose=self._verbose,
                )
            except Exception as exc:
                print(f"  [orchestrator] ERROR in LLM pipeline: {exc}")
                final_content = _wrap_plain_text(matched)

            # DEBUG: dump full LLM pipeline outputs to a dedicated log file
            import os as _os
            _debug_log = "/tmp/migration_debug.log"
            with open(_debug_log, "w") as _f:
                _f.write(f"=== MIGRATION DEBUG LOG ===\n\n")
                for lr in llm_results:
                    has_macro = 'ac:name="plantuml"' in lr["output_text"]
                    has_cdata = 'CDATA' in lr["output_text"]
                    has_theme = '!theme' in lr["output_text"]
                    _f.write(f"--- TASK: {lr['task']} ---\n")
                    _f.write(f"success={lr['success']}  len={len(lr['output_text'])}  has_plantuml_macro={has_macro}  has_cdata={has_cdata}  has_theme={has_theme}\n")
                    if not lr["success"]:
                        _f.write(f"ERROR: {lr['error']}\n")
                    _f.write(f"FULL OUTPUT:\n{lr['output_text']}\n\n")
                    # Also print summary to uvicorn log
                    print(f"  [DEBUG] task={lr['task']} success={lr['success']} "
                          f"has_plantuml_macro={has_macro} has_cdata={has_cdata} has_theme={has_theme}")
                    if not lr["success"]:
                        print(f"  [DEBUG]   error: {lr['error']}")

                _f.write(f"--- FINAL CONTENT (sent to Confluence) ---\n")
                _f.write(final_content)
                _f.write(f"\n\n=== END ===\n")

            _has_macro = 'ac:name="plantuml"' in final_content
            _has_cdata = 'CDATA' in final_content
            _has_theme = '!theme' in final_content
            print(f"  [DEBUG] Full pipeline dump written to {_debug_log}")
            print(f"  [DEBUG] final_content: has_plantuml_macro={_has_macro}  has_cdata={_has_cdata}  has_theme={_has_theme}")
        else:
            # No LLM: wrap plain text in minimal Confluence XHTML
            final_content = _wrap_plain_text(matched)
            if not mapping.llm.enabled:
                print("  [orchestrator] LLM disabled for this section")

        if self._verbose:
            preview = final_content[:500].replace("\n", " ")
            print(f"  [orchestrator] Final content ({len(final_content)} chars): {preview}...")

        # Dry run: skip Confluence API call
        if self._dry_run:
            print(f"  [orchestrator] DRY RUN: would {mapping.confluence.action} Confluence page")
            return SectionResult(
                section_id=matched["id"],
                section_title=matched["title"],
                mapping_match=mapping.match,
                action=mapping.confluence.action,
                llm_results=llm_results,
                confluence_page_id=None,
                confluence_page_url=None,
                status="success",
                error=None,
            )

        # Execute Confluence action
        try:
            page_id, page_url = self._execute_confluence_action(
                mapping, matched, final_content,
                effective_folder_path=effective_folder_path,
            )

            # Render PlantUML diagrams to PNG and embed as attachments
            if not self._dry_run and "plantuml" in final_content:
                try:
                    final_content = self._render_plantuml_diagrams(
                        page_id=page_id,
                        content=final_content,
                    )
                except Exception as puml_exc:
                    print(f"  [orchestrator] WARN: PlantUML PNG rendering failed: {puml_exc}")

            # Upload embedded images as attachments and update page body
            if matched.get("images") and not self._dry_run:
                try:
                    final_content = self._upload_images_and_update_page(
                        page_id=page_id,
                        section=matched,
                        content=final_content,
                        space_key=mapping.confluence.space_key or "",
                        current_version=None,  # fetched inside method
                    )
                except Exception as img_exc:
                    print(f"  [orchestrator] WARN: image upload failed: {img_exc}")

            # When expanding tables to pages, also upload child-section images to the main page
            if mapping.llm.expand_tables_to_pages and not self._dry_run:
                child_images = _collect_child_images(matched)
                if child_images:
                    try:
                        print(f"  [orchestrator] Uploading {len(child_images)} image(s) from child sections to main page")
                        self._upload_images_and_update_page(
                            page_id=page_id,
                            section={"images": child_images},
                            content=final_content,
                            space_key=mapping.confluence.space_key or "",
                            current_version=None,
                        )
                    except Exception as exc:
                        print(f"  [orchestrator] WARN: child image upload failed: {exc}")

            # Expand table rows to individual pages (section + all subsections)
            print(f"  [expand] expand_tables_to_pages={mapping.llm.expand_tables_to_pages} | section='{matched['title']}' | tables={len(matched.get('tables', []))} | children={len(matched.get('children', []))}")
            if mapping.llm.expand_tables_to_pages:
                try:
                    self._expand_tables_to_pages_recursive(mapping, matched, page_id, depth=0)
                except Exception as exc:
                    print(f"  [expand] WARN: expand_tables_to_pages failed: {exc}")

            return SectionResult(
                section_id=matched["id"],
                section_title=matched["title"],
                mapping_match=mapping.match,
                action=mapping.confluence.action,
                llm_results=llm_results,
                confluence_page_id=page_id,
                confluence_page_url=page_url,
                status="success",
                error=None,
            )
        except ConfluenceAPIError as exc:
            print(f"  [orchestrator] ERROR (Confluence API): {exc}")
            return SectionResult(
                section_id=matched["id"],
                section_title=matched["title"],
                mapping_match=mapping.match,
                action=mapping.confluence.action,
                llm_results=llm_results,
                confluence_page_id=None,
                confluence_page_url=None,
                status="failed",
                error=str(exc),
            )
        except Exception as exc:
            print(f"  [orchestrator] ERROR (unexpected): {type(exc).__name__}: {exc}")
            return SectionResult(
                section_id=matched["id"],
                section_title=matched["title"],
                mapping_match=mapping.match,
                action=mapping.confluence.action,
                llm_results=llm_results,
                confluence_page_id=None,
                confluence_page_url=None,
                status="failed",
                error=f"{type(exc).__name__}: {exc}",
            )

    def _create_table_row_pages(
        self,
        mapping: SectionMappingModel,
        section: ParsedSection,
        folder_path: str,
    ) -> None:
        """
        Create one Confluence page per data row in each table contained in `section`.

        The page content is a 2-column transposed table:
          column 1 = original table headers (as <th>)
          column 2 = the data row's cell values (as <td>)

        Page titles are built from `mapping.confluence.row_page_title` (a template
        string where {col_0}, {col_1}, … are replaced with row cell values).
        Falls back to {col_0} (first column) if no template is set.

        The `folder_path` argument is the full Confluence path for the parent folder
        (e.g. "Module X/Screen Designs/S313 – Screen/S313 – Page Elements").
        """
        client = self._get_confluence()

        # Resolve / create the Page Elements folder
        print(f"  [table-rows] Resolving folder '{folder_path}'")
        folder_id = client.resolve_or_create_folder_path(
            space_key=mapping.confluence.space_key,
            folder_path=folder_path,
            root_parent_id=mapping.confluence.parent_page_id,
        )
        print(f"  [table-rows] Folder resolved → page_id={folder_id}")

        row_title_tpl = mapping.confluence.row_page_title or "{col_0}"

        for table in section.get("tables", []):
            rows = table.get("rows", [])
            if len(rows) < 2:
                continue  # header-only table — nothing to expand
            headers   = rows[0]
            data_rows = rows[1:]

            for data_row in data_rows:
                if not any(cell.strip() for cell in data_row):
                    continue  # skip blank rows

                page_title = _fill_row_title(row_title_tpl, data_row)
                content    = _build_transposed_table_html(headers, data_row)

                # Overwrite: delete existing page with the same title first
                if self._overwrite:
                    try:
                        existing = client.get_page_by_title(
                            mapping.confluence.space_key, page_title
                        )
                        if existing:
                            print(
                                f"    [table-rows] OVERWRITE: deleting existing page "
                                f"'{page_title}' (id={existing['id']})"
                            )
                            client.delete_page(existing["id"])
                    except Exception:
                        pass

                try:
                    result = client.create_page(
                        space_key=mapping.confluence.space_key,
                        title=page_title,
                        content=content,
                        parent_id=folder_id,
                    )
                    print(
                        f"    [table-rows] Created '{page_title}' (id={result['id']})"
                    )
                except ConfluenceAPIError as exc:
                    if (
                        exc.status_code == 400
                        and "title already exists" in exc.response_body.lower()
                    ):
                        print(
                            f"    [table-rows] WARN: '{page_title}' already exists — skipping"
                        )
                    else:
                        print(
                            f"    [table-rows] ERROR creating '{page_title}': {exc}"
                        )

    def _expand_tables_to_pages_recursive(
        self,
        mapping: SectionMappingModel,
        section: ParsedSection,
        parent_page_id: str,
        depth: int = 0,
    ) -> None:
        """
        Recursively create one Confluence page per data row for every table in
        `section` and all its descendant subsections (children).

        Triggered by ``llm.expand_tables_to_pages: true`` in the config.  Unlike
        the ``confluence.table_rows_to_pages`` flag (which only operates on
        folder-only sections), this method runs after the main section page has
        been created and nests the row pages directly under `parent_page_id`.

        Each page body is a 2-column transposed table:
          column 1 = original table headers (rendered as <th>)
          column 2 = the data row's cell values (rendered as <td>)

        Page titles are built from ``mapping.confluence.row_page_title``
        (supports {col_0}, {col_1}, … placeholders); falls back to {col_0}.
        """
        indent = "  " * depth
        client = self._get_confluence()
        row_title_tpl = mapping.confluence.row_page_title or "{col_0}"

        tables = section.get("tables", [])
        children = section.get("children", [])
        print(f"  [expand] {indent}section '{section.get('title', '?')}' — {len(tables)} table(s), {len(children)} child(ren)")

        for t_idx, table in enumerate(tables):
            rows = table.get("rows", [])
            headers   = rows[0] if rows else []
            data_rows = rows[1:] if len(rows) > 1 else []
            print(f"  [expand] {indent}  table[{t_idx}]: {len(rows)} rows | headers: {headers} | data rows: {len(data_rows)}")

            for r_idx, data_row in enumerate(data_rows):
                is_blank = not any(cell.strip() for cell in data_row)
                page_title = _fill_row_title(row_title_tpl, data_row) if not is_blank else "(blank)"
                print(f"  [expand] {indent}    row[{r_idx}]: {data_row} → {'SKIP (blank)' if is_blank else repr(page_title)}")
                if is_blank:
                    continue

                content = _build_transposed_table_html(headers, data_row)

                if self._overwrite:
                    try:
                        existing = client.get_page_by_title(
                            mapping.confluence.space_key, page_title
                        )
                        if existing:
                            print(
                                f"    [expand-tables] OVERWRITE: deleting existing page "
                                f"'{page_title}' (id={existing['id']})"
                            )
                            client.delete_page(existing["id"])
                    except Exception:
                        pass

                try:
                    result = client.create_page(
                        space_key=mapping.confluence.space_key,
                        title=page_title,
                        content=content,
                        parent_id=parent_page_id,
                    )
                    print(
                        f"    [expand-tables] Created '{page_title}' (id={result['id']})"
                    )
                except ConfluenceAPIError as exc:
                    if (
                        exc.status_code == 400
                        and "title already exists" in exc.response_body.lower()
                    ):
                        print(
                            f"    [expand-tables] WARN: '{page_title}' already exists — skipping"
                        )
                    else:
                        print(
                            f"    [expand-tables] ERROR creating '{page_title}': {exc}"
                        )

        # Recurse into subsections so their tables are also expanded
        for child in children:
            self._expand_tables_to_pages_recursive(mapping, child, parent_page_id, depth=depth + 1)

    def _find_matching_section(
        self,
        mapping: SectionMappingModel,
        all_sections: List[ParsedSection],
    ) -> Optional[ParsedSection]:
        """
        Return the matching ParsedSection for a mapping.

        Lookup priority:
          1. section_id (exact ID match) — avoids ambiguity when multiple
             sections share the same heading text (e.g. many "Screen Mockup"
             sections).  Set by the UI when generating section mappings.
          2. match_type == "table"  — first table-content section.
          3. Title regex match       — first section whose title matches.
        """
        # Priority 1: exact ID match (set by the UI to avoid title collisions)
        if mapping.section_id:
            for s in all_sections:
                if s["id"] == mapping.section_id:
                    return s
            # If an explicit section_id was provided but nothing matched,
            # don't fall through to regex — the mapping is stale.
            print(
                f"  [orchestrator] WARN: section_id='{mapping.section_id}' not found "
                f"in document (section may have been removed or the doc changed)"
            )
            return None

        # Priority 2: table content-type match
        if mapping.match_type == "table":
            for s in all_sections:
                if s["content_type"] == "table":
                    return s
            return None

        # Priority 3: title regex match
        for s in all_sections:
            if mapping.matches_title(s["title"]):
                return s
        return None

    def _execute_confluence_action(
        self,
        mapping: SectionMappingModel,
        section: ParsedSection,
        content: str,
        effective_folder_path: Optional[str] = None,
    ) -> Tuple[str, str]:
        """
        Execute the Confluence API call.
        Returns (page_id, page_url).
        effective_folder_path overrides mapping.confluence.folder_path when provided
        (used to apply inherited folder context from folder_only sections).
        """
        client = self._get_confluence()
        action = mapping.confluence.action
        target = mapping.confluence
        # Use caller-supplied folder path (may be inherited) over the mapping's own
        resolved_folder = effective_folder_path if effective_folder_path is not None else target.folder_path

        if action == "create":
            # Resolve (or create) folder hierarchy first if folder_path is set.
            # The leaf folder page ID takes precedence over parent_page_id.
            effective_parent_id = target.parent_page_id
            if resolved_folder:
                print(
                    f"  [orchestrator] Resolving folder path '{resolved_folder}' "
                    f"in space '{target.space_key}'"
                )
                effective_parent_id = client.resolve_or_create_folder_path(
                    space_key=target.space_key,
                    folder_path=resolved_folder,
                    root_parent_id=target.parent_page_id,
                )
                print(
                    f"  [orchestrator] Folder resolved to page_id={effective_parent_id}"
                )

            # Proactively prefix the page title with the most specific folder
            # identifier extracted from the folder path:
            #   - Screen code (S313, S486) takes priority for screen sub-pages
            #   - Module name ("Contract Budget") is used for pages directly
            #     under a module folder (e.g. "Module – Contract Budget")
            # This ensures uniqueness across both multiple screens AND multiple modules.
            # page_title override: set explicitly by the UI rule engine.
            # Falls back to the auto-prefix logic when not provided.
            page_title_override = mapping.confluence.page_title
            if page_title_override:
                page_title = page_title_override.strip()
                print(f"  [orchestrator] page_title override: '{page_title}'")
            else:
                folder_prefix = _extract_folder_prefix(resolved_folder)
                page_title = (
                    f"{folder_prefix} \u2013 {section['title']}"
                    if folder_prefix
                    else section["title"]
                )
                if folder_prefix:
                    print(
                        f"  [orchestrator] Folder prefix '{folder_prefix}' detected — "
                        f"using title: '{page_title}'"
                    )

            # ── Overwrite: delete any existing page with the same title ─────────
            if self._overwrite:
                try:
                    existing = client.get_page_by_title(target.space_key, page_title)
                except Exception as lookup_exc:
                    print(f"  [orchestrator] WARN: overwrite lookup failed: {lookup_exc}")
                    existing = None
                if existing:
                    print(
                        f"  [orchestrator] OVERWRITE: deleting existing page "
                        f"'{page_title}' (id={existing['id']}) before re-create"
                    )
                    client.delete_page(existing["id"])

            try:
                result = client.create_page(
                    space_key=target.space_key,
                    title=page_title,
                    content=content,
                    parent_id=effective_parent_id,
                )
            except ConfluenceAPIError as exc:
                if exc.status_code == 400 and (
                    "title already exists" in exc.response_body.lower()
                    or "already exists with the same" in exc.response_body.lower()
                ):
                    # Page already exists — look it up and return it so that
                    # re-running the migration is idempotent (no duplicate pages,
                    # no cascading failures on subsequent runs).
                    # Use the Overwrite flag to replace the content of existing pages.
                    try:
                        existing = client.get_page_by_title(target.space_key, page_title)
                    except Exception:
                        existing = None
                    if existing:
                        print(
                            f"  [orchestrator] Page '{page_title}' already exists "
                            f"(id={existing['id']}) — skipping creation. "
                            f"Enable 'Overwrite Existing Pages' to replace content."
                        )
                        existing_url = (
                            existing.get("url")
                            or f"{self._config.confluence_base_url}/wiki/spaces/"
                               f"{target.space_key}/pages/{existing['id']}"
                        )
                        return existing["id"], existing_url
                    # Page not found by title despite conflict error (rare race
                    # condition) — fall back to suffix disambiguation.
                    section_code = section["id"].upper()
                    page_title = f"{page_title} ({section_code})"
                    print(
                        f"  [orchestrator] Title conflict (page lookup failed) — "
                        f"retrying with disambiguated title: '{page_title}'"
                    )
                    result = client.create_page(
                        space_key=target.space_key,
                        title=page_title,
                        content=content,
                        parent_id=effective_parent_id,
                    )
                else:
                    raise
            return result["id"], result["url"]

        elif action == "update":
            current_page = client.get_page(target.page_id)  # type: ignore[arg-type]
            result = client.update_page(
                page_id=target.page_id,  # type: ignore[arg-type]
                title=current_page["title"],
                content=content,
                current_version=current_page["version"],
            )
            page_url = f"{self._config.confluence_base_url}/wiki/pages/{result['id']}"
            return result["id"], page_url

        elif action == "append":
            # Resolve the target page: explicit page_id takes priority;
            # otherwise look it up by page_title in the space (same approach
            # as create uses for parent-folder resolution).
            append_page_id = target.page_id
            if not append_page_id:
                target_title = (target.page_title or "").strip()
                if not target_title:
                    raise ValueError(
                        "append action requires either page_id or page_title "
                        f"(section: '{section['title']}')"
                    )
                found = client.get_page_by_title(target.space_key, target_title)
                if found is None:
                    raise ValueError(
                        f"append: target page '{target_title}' not found "
                        f"in space '{target.space_key}' — ensure it is created first."
                    )
                append_page_id = found["id"]
                print(
                    f"  [orchestrator] append: resolved '{target_title}' "
                    f"→ page_id={append_page_id}"
                )
            result = client.append_to_page(
                page_id=append_page_id,
                new_content=content,
            )
            page_url = f"{self._config.confluence_base_url}/wiki/pages/{result['id']}"
            return result["id"], page_url

        else:
            raise ValueError(f"Unknown Confluence action: '{action}'")

    def _upload_images_and_update_page(
        self,
        page_id: str,
        section: "ParsedSection",
        content: str,
        space_key: str,
        current_version: Optional[int],
    ) -> str:
        """
        Upload each image in section['images'] as a Confluence attachment, then
        embed <ac:image> macros at the correct position in the page body.

        Positioning strategy (in priority order):
          1. If the content contains a ``%%IMG_N%%`` sentinel (placed by
             _build_initial_text() using element_sequence), replace it with the
             <ac:image> macro at the exact document position.  The LLM may wrap
             the sentinel in a <p> tag; both ``%%IMG_N%%`` and
             ``<p>%%IMG_N%%</p>`` forms are handled.
          2. If no sentinel is found for an image (e.g. legacy sections without
             element_sequence, or the LLM dropped the token), the macro is
             appended after all existing content — same as the old behaviour.

        Each <ac:image> is always wrapped in <p>…</p> for well-formed
        Confluence storage XML.

        Returns the updated content string (with image macros in place).
        """
        client = self._get_confluence()
        images = section.get("images", [])
        if not images:
            return content

        print(f"  [orchestrator] Uploading {len(images)} image(s) for page {page_id}")

        # --- Upload all images and build {image_index → macro_html} map ------
        uploaded: List[Tuple[int, str]] = []  # (image_index_in_section, macro_html)

        for img_idx, img in enumerate(images):
            try:
                data_bytes = base64.b64decode(img["data_b64"])
                att = client.upload_attachment(
                    page_id=page_id,
                    filename=img["filename"],
                    data_bytes=data_bytes,
                    content_type=img["content_type"],
                )
                print(
                    f"    Uploaded '{img['filename']}' → attachment id={att['id']}"
                )
                # Wrap in <p> for well-formed Confluence storage XML
                macro = (
                    f'<p>'
                    f'<ac:image>'
                    f'<ri:attachment ri:filename="{_xml_escape(img["filename"])}"/>'
                    f'</ac:image>'
                    f'</p>'
                )
                uploaded.append((img_idx, macro))
            except Exception as exc:
                print(f"    WARN: could not upload '{img['filename']}': {exc}")

        if not uploaded:
            return content

        # --- Resolve sentinels or append ----------------------------------------
        updated_content = content
        appended: List[str] = []   # macros that couldn't find their sentinel

        for img_idx, macro in uploaded:
            sentinel = f"%%IMG_{img_idx}%%"
            # Form 1: LLM may wrap the sentinel in <p> tags
            p_pattern = re.compile(
                r'<p>\s*' + re.escape(sentinel) + r'\s*</p>',
                re.IGNORECASE,
            )
            if p_pattern.search(updated_content):
                updated_content = p_pattern.sub(macro, updated_content)
            elif sentinel in updated_content:
                # Form 2: bare sentinel (no <p> wrapper)
                updated_content = updated_content.replace(sentinel, macro)
            else:
                # Fallback: couldn't locate sentinel → append at end
                print(
                    f"    [orchestrator] WARN: sentinel {sentinel!r} not found in "
                    f"content — image will be appended after page body"
                )
                appended.append(macro)

        if appended:
            updated_content = updated_content + "\n" + "\n".join(appended)

        # --- Push updated content back to the page ----------------------------
        page_info = client.get_page(page_id)
        client.update_page(
            page_id=page_id,
            title=page_info["title"],
            content=updated_content,
            current_version=page_info["version"],
        )
        print(f"  [orchestrator] Page {page_id} updated with {len(uploaded)} image macro(s)")
        return updated_content

    def _render_plantuml_diagrams(
        self,
        page_id: str,
        content: str,
    ) -> str:
        """
        Find all PlantUML macros in `content`, render each to PNG via Kroki,
        upload as attachments, and replace macros with a two-column layout
        (source code on left, rendered PNG image on right).

        If no PlantUML macros are found, or Kroki fails for every diagram,
        the original content is returned unchanged.

        Returns the updated content string.
        """
        client = self._get_confluence()
        updated_content, _next_idx = render_and_embed_plantuml_diagrams(
            content=content,
            page_id=page_id,
            confluence_client=client,
            theme=getattr(self._config, "plantuml_theme", "cerulean"),
        )

        if updated_content == content:
            # Nothing changed (no diagrams or all renders failed)
            return content

        # Push the updated content back to the page
        page_info = client.get_page(page_id)
        client.update_page(
            page_id=page_id,
            title=page_info["title"],
            content=updated_content,
            current_version=page_info["version"],
        )
        print(
            f"  [orchestrator] Page {page_id} updated with PlantUML PNG diagram(s)"
        )
        return updated_content

    # ─── Lazy Client Initializers ─────────────────────────────────────────────

    def _get_llm(self) -> LLMProcessor:
        if self._llm is None:
            self._llm = LLMProcessor(
                model_name=self._config.llm_model,
                temperature=self._config.llm_temperature,
            )
        return self._llm

    def _get_confluence(self) -> ConfluenceClient:
        if self._confluence is None:
            self._confluence = ConfluenceClient(
                base_url=self._config.confluence_base_url,
                user=self._config.confluence_user,
                api_token=self._config.confluence_api_token,
            )
        return self._confluence

    # ─── SQLite Logging ──────────────────────────────────────────────────────

    def _init_db(self) -> None:
        """
        Create migration tracking tables if they don't exist.
        Mirrors the init_db() pattern from smart_agents/tools.py.
        """
        conn = self._db_conn()
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS migration_runs (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                doc_path     TEXT,
                config_path  TEXT,
                dry_run      INTEGER,
                started_at   TEXT,
                finished_at  TEXT,
                total_sections INTEGER,
                total_mappings INTEGER
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS migration_results (
                id                   INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id               INTEGER,
                section_id           TEXT,
                section_title        TEXT,
                mapping_match        TEXT,
                action               TEXT,
                status               TEXT,
                confluence_page_id   TEXT,
                confluence_page_url  TEXT,
                error                TEXT,
                created_at           TEXT,
                FOREIGN KEY(run_id) REFERENCES migration_runs(id)
            )
        """)
        conn.commit()
        conn.close()

    def _log_report_to_db(self, report: MigrationReport) -> None:
        """Write the MigrationReport to SQLite. Failures are logged as warnings."""
        try:
            conn = self._db_conn()
            cur = conn.cursor()
            cur.execute(
                """INSERT INTO migration_runs
                   (doc_path, config_path, dry_run, started_at, finished_at,
                    total_sections, total_mappings)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (
                    report["doc_path"],
                    report["config_path"],
                    1 if report["dry_run"] else 0,
                    report["started_at"],
                    report["finished_at"],
                    report["total_sections_in_doc"],
                    report["total_mappings"],
                ),
            )
            run_id = cur.lastrowid
            for result in report["results"]:
                cur.execute(
                    """INSERT INTO migration_results
                       (run_id, section_id, section_title, mapping_match, action,
                        status, confluence_page_id, confluence_page_url, error, created_at)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        run_id,
                        result["section_id"],
                        result["section_title"],
                        result["mapping_match"],
                        result["action"],
                        result["status"],
                        result["confluence_page_id"],
                        result["confluence_page_url"],
                        result["error"],
                        _now_iso(),
                    ),
                )
            conn.commit()
            conn.close()
            print(f"[orchestrator] Run logged to DB at {self._config.db_path} (run_id={run_id})")
        except Exception as exc:
            print(f"[orchestrator] WARN: Failed to write to DB: {exc}")

    def _db_conn(self) -> sqlite3.Connection:
        db_dir = os.path.dirname(self._config.db_path)
        if db_dir:
            os.makedirs(db_dir, exist_ok=True)
        return sqlite3.connect(self._config.db_path, check_same_thread=False)


# ─── Module-Level Helpers ─────────────────────────────────────────────────────

def _collect_child_images(section: ParsedSection) -> list:
    """
    Return images from the first direct child that has images.
    This captures the introductory screen mockup that appears immediately
    under a section heading (e.g. 'Contract Budget Information' under
    'Page Elements') without pulling in images from every subsequent
    sub-section (Collapsible Widgets, Documents, Advances, etc.).
    """
    for child in section.get("children", []):
        images = child.get("images", [])
        if images:
            return list(images)
    return []


def _flatten_sections(sections: List[ParsedSection]) -> List[ParsedSection]:
    """
    Recursively flatten a section tree into a depth-first ordered list.
    Parent sections appear before their children.
    """
    result: List[ParsedSection] = []
    for s in sections:
        result.append(s)
        if s["children"]:
            result.extend(_flatten_sections(s["children"]))
    return result


def _wrap_plain_text(section: ParsedSection) -> str:
    """
    Minimal Confluence storage format wrapping when no LLM is used.
    Paragraphs → <p> tags; tables → <table> blocks; images → %%IMG_N%% sentinels.

    Uses element_sequence when available (preserves original document order of
    text, tables, and images).  Falls back to the legacy flat approach (text
    then tables) for backward compatibility.

    Image sentinels are resolved after attachment upload by
    _upload_images_and_update_page().
    """
    element_sequence = section.get("element_sequence") or []

    # ── New path: element_sequence honours document order ─────────────────────
    if element_sequence:
        parts = []
        for elem in element_sequence:
            btype = elem.get("block_type", "")
            if btype == "text":
                line = (elem.get("text") or "").strip()
                if line:
                    parts.append(f"<p>{_xml_escape(line)}</p>")
            elif btype == "table":
                t_idx = elem.get("table_index", 0)
                tables = section.get("tables", [])
                if 0 <= t_idx < len(tables):
                    table = tables[t_idx]
                    parts.append("<table><tbody>")
                    for i, row in enumerate(table["rows"]):
                        parts.append("<tr>")
                        tag = "th" if (i == 0 and table["header_row"]) else "td"
                        for cell in row:
                            parts.append(f"<{tag}>{_xml_escape(cell)}</{tag}>")
                        parts.append("</tr>")
                    parts.append("</tbody></table>")
            elif btype == "image":
                i_idx = elem.get("image_index", 0)
                parts.append(f"<p>%%IMG_{i_idx}%%</p>")
        return "\n".join(parts)

    # ── Legacy fallback: flat text then all tables (no image sentinels) ───────
    parts = []
    for line in section["raw_text"].split("\n"):
        line = line.strip()
        if line:
            parts.append(f"<p>{_xml_escape(line)}</p>")

    for table in section.get("tables", []):
        parts.append("<table><tbody>")
        for i, row in enumerate(table["rows"]):
            parts.append("<tr>")
            tag = "th" if (i == 0 and table["header_row"]) else "td"
            for cell in row:
                parts.append(f"<{tag}>{_xml_escape(cell)}</{tag}>")
            parts.append("</tr>")
        parts.append("</tbody></table>")

    return "\n".join(parts)


def _xml_escape(text: str) -> str:
    """Minimal XML escaping for Confluence storage format text nodes."""
    return (
        text.replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
            .replace('"', "&quot;")
    )


def _build_transposed_table_html(headers: List[str], row_values: List[str]) -> str:
    """
    Build a 2-column transposed table: headers as column 1, values as column 2.

    Example — original row ["Submit", "Button", "Submits the form"] with
    headers ["Element", "Type", "Description"] produces:

        | Element     | Submit           |
        | Type        | Button           |
        | Description | Submits the form |

    Each header cell is rendered as <th> and each value cell as <td>.
    """
    parts = ["<table><tbody>"]
    for h, v in zip(headers, row_values):
        parts.append(
            f"<tr><th>{_xml_escape(h)}</th><td>{_xml_escape(v)}</td></tr>"
        )
    parts.append("</tbody></table>")
    return "\n".join(parts)


def _fill_row_title(template: str, row: List[str]) -> str:
    """
    Replace {col_0}, {col_1}, … placeholders in `template` with the
    corresponding cell values from `row`.

    Example: _fill_row_title("S313 - {col_0}", ["Submit", "Button"])
             → "S313 - Submit"
    """
    result = template
    for i, val in enumerate(row):
        result = result.replace(f"{{col_{i}}}", val.strip())
    return result


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# Screen code: segment starting with S followed by digits, e.g. "S313", "S486"
_SCREEN_CODE_RE = re.compile(r'^(S\d+)\b', re.IGNORECASE)
# Dash separator: em dash, en dash, or plain hyphen surrounded by optional spaces
_DASH_SEP_RE = re.compile(r'\s*[–—\-]\s*')


def _extract_folder_prefix(folder_path: Optional[str]) -> Optional[str]:
    """
    Extract the most specific meaningful prefix from a folder path to use as
    a page title prefix, ensuring uniqueness across modules and screens.

    Priority (per segment, deepest wins):
      1. Screen code (S\\d+) at segment start → returns e.g. "S313"
      2. Name after a dash separator           → "Module – Contract Budget" → "Contract Budget"

    The deepest screen code always wins over a shallower dash-based name.

    Examples:
        "Module – Contract Budget"
            → "Contract Budget"   (module-level pages get module name prefix)

        "Module – Contract Budget/Screen Designs/S313 – Contract Budget"
            → "S313"              (screen code takes priority)

        "Module – Contract Budget/Screen Designs/S313 – Contract Budget/Page Elements"
            → "S313"              (screen code still wins from grandparent folder)

        "Module – Vendor Management"
            → "Vendor Management" (different module, different prefix)

        "Engineering/Services"
            → None                (no dash, no screen code → no prefix)
    """
    if not folder_path:
        return None

    screen_code: Optional[str] = None
    dash_name: Optional[str] = None

    for segment in folder_path.split("/"):
        segment = segment.strip()
        if not segment:
            continue

        # Screen code check (highest priority — keep the deepest one found)
        m = _SCREEN_CODE_RE.match(segment)
        if m:
            screen_code = m.group(1).upper()
            continue

        # Dash separator check — "Word – Meaningful Name" → "Meaningful Name"
        parts = _DASH_SEP_RE.split(segment, maxsplit=1)
        if len(parts) >= 2 and parts[1].strip():
            dash_name = parts[1].strip()

    # Screen code wins; otherwise use the dash-extracted name
    return screen_code or dash_name


def _print_result(result: SectionResult) -> None:
    icons = {"success": "OK  ", "skipped": "SKIP", "failed": "FAIL"}
    icon = icons.get(result["status"], "????")
    label = result["section_title"] or result["mapping_match"]
    msg = f"  [{icon}] '{label}' → {result['action']}"
    if result["confluence_page_id"]:
        msg += f" (page_id={result['confluence_page_id']})"
    if result["error"]:
        msg += f"\n        ERROR: {result['error']}"
    print(msg)


def _print_summary(report: MigrationReport) -> None:
    success = sum(1 for r in report["results"] if r["status"] == "success")
    failed = sum(1 for r in report["results"] if r["status"] == "failed")
    skipped = sum(1 for r in report["results"] if r["status"] == "skipped")
    print("\n" + "─" * 50)
    print("[orchestrator] Migration complete")
    print(f"  Doc sections : {report['total_sections_in_doc']}")
    print(f"  Mappings     : {report['total_mappings']}")
    print(f"  Success      : {success}")
    print(f"  Failed       : {failed}")
    print(f"  Skipped      : {skipped}")
    if report["dry_run"]:
        print("  (DRY RUN - Confluence was NOT modified)")
    print("─" * 50)
