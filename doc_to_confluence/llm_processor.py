"""
LLM processing pipeline for the doc_to_confluence migration tool.

Uses ChatOllama (langchain_ollama) to apply sequential processing tasks to
parsed Word document sections before they are published to Confluence.

Task execution order (enforced by config validator, but defined here):
  reformat -> summarize -> plantuml_diagram -> confluence_format

Each task receives the output of the previous task as its input text.
On task failure, the unchanged input text is passed to the next task.

PlantUML diagram output is emitted as two adjacent blocks:
  1. A plantuml macro (ac:schema-version="1") — picked up by plantuml_renderer.py
     which renders it to PNG via Kroki and embeds it as a Confluence attachment.
  2. A code block with the same PlantUML source — becomes the left column of the
     two-column layout that plantuml_renderer.py builds around the PNG image.

The rendering pipeline in plantuml_renderer.py replaces both blocks with a
side-by-side table: source code on the left (45%), PNG image on the right (55%).

IMPORTANT — macro preservation through confluence_format:
  LLMs cannot reliably preserve <ac:structured-macro> blocks with CDATA sections
  when asked to reformat surrounding text. The confluence_format task therefore
  uses _extract_macros() / _restore_macros() to:
    1. Replace every <ac:structured-macro>...</ac:structured-macro> span with a
       stable sentinel token %%MACRO_0%%, %%MACRO_1%%, … before sending to LLM
    2. Restore the original verbatim blocks after the LLM responds
  This guarantees plantuml and code macros are never touched by the formatter.
"""
import re
from typing import List, Tuple

from langchain_core.messages import HumanMessage, SystemMessage
from langchain_ollama import ChatOllama

from .models import LLMResult, LLMTaskName, ParsedSection


# ─── Prompt Templates ─────────────────────────────────────────────────────────

SYSTEM_PROMPTS: dict = {
    "reformat": """\
You are a technical writing assistant. Your job is to clean up and reformat text content.

Rules:
- Fix grammar and spelling errors
- Improve sentence clarity and readability
- Preserve all technical terms, proper nouns, acronyms, and numbers exactly as-is
- Do NOT add new information or remove existing facts
- Do NOT add headings, bullet points, or markdown unless they already exist in the input
- Return ONLY the reformatted plain text, no commentary or preamble
""",

    "summarize": """\
You are a concise technical summarizer.

Rules:
- Write exactly 2-3 sentences summarizing the key points of the provided content
- Return ONLY the summary — do NOT include the original text
- Return ONLY the summary sentences, no labels like "Summary:", no preamble, no commentary
""",

    "plantuml_diagram": """\
You are a diagram designer specializing in PlantUML.

Rules:
- Analyze the provided text for processes, workflows, relationships, data flows, or system architectures
- Generate a valid PlantUML diagram definition appropriate for the content
- Choose the most suitable diagram type:
  * @startuml / @enduml for sequence, class, component, activity diagrams
  * @startmindmap / @endmindmap for mind maps
  * @startwbs / @endwbs for work breakdown structures
- If the content contains no clear structure worth diagramming, return exactly the word: NO_DIAGRAM
- If a diagram IS generated, output the following two blocks after the original text, separated by two blank lines:

Block 1 — PlantUML macro (the rendering pipeline will convert this to a PNG image):
<ac:structured-macro ac:name="plantuml" ac:schema-version="1">
  <ac:plain-text-body><![CDATA[
PLANTUML_DEFINITION_HERE
  ]]></ac:plain-text-body>
</ac:structured-macro>

Block 2 — Code block (shown as source alongside the rendered image):
<ac:structured-macro ac:name="code" ac:schema-version="1">
  <ac:parameter ac:name="language">text</ac:parameter>
  <ac:plain-text-body><![CDATA[
PLANTUML_DEFINITION_HERE
  ]]></ac:plain-text-body>
</ac:structured-macro>

- Replace PLANTUML_DEFINITION_HERE with the actual PlantUML source in BOTH blocks (identical content)
- The two blocks must be adjacent, separated by a single blank line
- Return: <original text> + two newlines + <Block 1> + one blank line + <Block 2>
- Do NOT include "NO_DIAGRAM" if you produced a valid diagram
- Do NOT add any explanation or commentary around the output
""",

    # usecase_diagrams uses a per-use-case prompt (fed one use case at a time by
    # _run_usecase_diagrams_task), so this system prompt describes a SINGLE use case.
    "usecase_diagrams": """\
You are a diagram designer specializing in PlantUML use case diagrams.

The input is the text of a SINGLE use case (starting with "Use Case:").
Your job is to output a PlantUML use case diagram for it.

Output ONLY these two adjacent blocks (identical PlantUML source in both):

Block 1 — PlantUML macro:
<ac:structured-macro ac:name="plantuml" ac:schema-version="1">
  <ac:plain-text-body><![CDATA[
PLANTUML_DEFINITION_HERE
  ]]></ac:plain-text-body>
</ac:structured-macro>

Block 2 — Code block:
<ac:structured-macro ac:name="code" ac:schema-version="1">
  <ac:parameter ac:name="language">text</ac:parameter>
  <ac:plain-text-body><![CDATA[
PLANTUML_DEFINITION_HERE
  ]]></ac:plain-text-body>
</ac:structured-macro>

Rules:
- Use @startuml / @enduml with usecase diagram syntax (actor, usecase, arrows)
- Keep the diagram focused on THIS use case only — actors, system, and actions described in the text
- Replace PLANTUML_DEFINITION_HERE with the actual PlantUML source (identical in both blocks)
- Do NOT output the original use case text — only the two macro blocks
- Do NOT add any explanation, commentary, or preamble
""",

    "confluence_format": """\
You are a Confluence content formatter. Convert the provided text to valid Confluence storage format (XHTML subset).

Conversion rules:
- Wrap each paragraph in <p>text</p>
- Bold text: <strong>text</strong>
- Italic text: <em>text</em>
- Bullet lists: <ul><li>item</li></ul>
- Numbered lists: <ol><li>item</li></ol>
- Tables: <table><tbody><tr><th>header</th></tr><tr><td>cell</td></tr></tbody></table>
- Section headings: <h2>text</h2> or <h3>text</h3>
- Inline code: <code>text</code>
- Code blocks:
  <ac:structured-macro ac:name="code">
    <ac:plain-text-body><![CDATA[code here]]></ac:plain-text-body>
  </ac:structured-macro>
- Info panels:
  <ac:structured-macro ac:name="info">
    <ac:rich-text-body><p>text</p></ac:rich-text-body>
  </ac:structured-macro>

Important preservation rules:
- If the input already contains Confluence XML macros (e.g., <ac:structured-macro ac:name="plantuml" ...> or <ac:structured-macro ac:name="code" ...>), preserve them EXACTLY as-is without any modification, escaping, or attribute removal — including ac:schema-version attributes and CDATA sections
- Do NOT wrap the output in <html>, <body>, <page>, or any outer container tags
- Do NOT include XML declarations (<?xml ...>)
- Escape special characters in text nodes: & → &amp;  < → &lt;  > → &gt;  " → &quot;
  (but NOT inside existing CDATA sections or macros)
- Return ONLY the Confluence storage format XML, no commentary
""",
}

HUMAN_PROMPT_TEMPLATES: dict = {
    "reformat": "Please reformat the following content:\n\n{text}",
    "summarize": "Please summarize the following content in 2-3 sentences. Return ONLY the summary, not the original text:\n\n{text}",
    "plantuml_diagram": "Please analyze this content and generate a PlantUML diagram:\n\n{text}",
    "usecase_diagrams": "Generate a PlantUML use case diagram for the following single use case. Output only the two macro blocks (plantuml + code), no other text:\n\n{text}",
    "confluence_format": "Please convert the following to Confluence storage format:\n\n{text}",
}


# ─── LLMProcessor ─────────────────────────────────────────────────────────────

class LLMProcessor:
    """
    Stateless processor that holds a ChatOllama instance and applies
    sequential LLM tasks to ParsedSection content.
    """

    def __init__(self, model_name: str, temperature: float = 0.1) -> None:
        """
        Args:
            model_name: Ollama model name, e.g. "gpt-oss:20b" or "llama3:8b"
            temperature: LLM temperature (lower = more deterministic)
        """
        self._llm = ChatOllama(model=model_name, temperature=temperature)
        self._model_name = model_name

    def process_section(
        self,
        section: ParsedSection,
        tasks: List[LLMTaskName],
        verbose: bool = False,
    ) -> Tuple[str, List[LLMResult]]:
        """
        Apply a sequence of LLM tasks to a section's content.

        Text flows through tasks in order; each task receives the previous
        task's output as its input. On failure, unchanged text is passed forward.

        Args:
            section: The ParsedSection to process
            tasks: Ordered list of LLMTaskName values
            verbose: If True, print intermediate inputs/outputs to stdout

        Returns:
            (final_text, list_of_LLMResult)
            final_text is the output after all tasks are applied.
            If tasks is empty, final_text == section raw text.
        """
        current_text = _build_initial_text(section)
        results: List[LLMResult] = []

        for task in tasks:
            # usecase_diagrams uses a special per-use-case splitting approach
            # to avoid sending the entire section (with many use cases) as one
            # huge LLM prompt — which causes timeouts.
            if task == "usecase_diagrams":
                result = self._run_usecase_diagrams_task(current_text, verbose=verbose)
            else:
                result = self._run_task(task, current_text, verbose=verbose)
            results.append(result)
            if result["success"]:
                current_text = result["output_text"]
            else:
                print(
                    f"[llm_processor] WARN: task '{task}' failed for "
                    f"section '{section['title']}': {result['error']}"
                )
                # Continue with unchanged text on failure

        return current_text, results

    def _run_usecase_diagrams_task(
        self,
        text: str,
        verbose: bool = False,
    ) -> LLMResult:
        """
        Handle the 'usecase_diagrams' task by splitting the input on 'Use Case:'
        boundaries in Python (no LLM needed for splitting), then calling the LLM
        once per use case to generate only the diagram blocks.  This avoids
        sending a massive multi-use-case prompt and eliminates LLM timeouts.

        Output format:
            <prefix text if any>
            <use case 1 text>
            <plantuml macro for use case 1>
            <code macro for use case 1>
            <use case 2 text>
            ...
        """
        try:
            # Split on "Use Case:" boundaries, keeping the delimiter
            _USE_CASE_RE = re.compile(r'(?=Use Case\s*:)', re.IGNORECASE)
            parts = _USE_CASE_RE.split(text)

            # If there are no use cases, return text unchanged
            use_case_parts = [p for p in parts if re.match(r'Use Case\s*:', p.strip(), re.IGNORECASE)]
            prefix_parts   = [p for p in parts if not re.match(r'Use Case\s*:', p.strip(), re.IGNORECASE)]

            if not use_case_parts:
                print("[llm_processor] usecase_diagrams: no 'Use Case:' blocks found — passing through unchanged")
                return LLMResult(
                    task="usecase_diagrams",
                    input_text=text,
                    output_text=text,
                    success=True,
                    error=None,
                )

            print(f"[llm_processor] usecase_diagrams: found {len(use_case_parts)} use case block(s)")

            # Collect prefix text (content before the first "Use Case:")
            prefix_text = "".join(prefix_parts).strip()
            output_parts: List[str] = []
            if prefix_text:
                output_parts.append(prefix_text)

            # Process each use case individually
            for uc_idx, uc_text in enumerate(use_case_parts):
                uc_text = uc_text.strip()
                if not uc_text:
                    continue

                if verbose:
                    print(f"  [llm_processor] usecase_diagrams: processing use case {uc_idx + 1}/{len(use_case_parts)}")

                # Always emit the original use case text verbatim
                output_parts.append(uc_text)

                # Ask the LLM for ONLY the diagram blocks for this single use case
                try:
                    system_prompt = SYSTEM_PROMPTS["usecase_diagrams"]
                    human_prompt  = HUMAN_PROMPT_TEMPLATES["usecase_diagrams"].format(text=uc_text)
                    messages = [
                        SystemMessage(content=system_prompt),
                        HumanMessage(content=human_prompt),
                    ]
                    response = self._llm.invoke(messages)
                    diagram_output = response.content.strip()  # type: ignore[union-attr]

                    # Strip any prose the model added (keep only the macro blocks)
                    diagram_output = _extract_macro_blocks_only(diagram_output)

                    if diagram_output:
                        output_parts.append(diagram_output)
                    else:
                        print(
                            f"  [llm_processor] usecase_diagrams: use case {uc_idx + 1} "
                            f"— no macro blocks extracted from LLM output, skipping diagram"
                        )
                except Exception as exc:
                    print(
                        f"  [llm_processor] usecase_diagrams: ERROR on use case {uc_idx + 1}: "
                        f"{type(exc).__name__}: {exc} — skipping diagram"
                    )

            combined = "\n\n".join(output_parts)
            return LLMResult(
                task="usecase_diagrams",
                input_text=text,
                output_text=combined,
                success=True,
                error=None,
            )

        except Exception as exc:
            error_msg = f"{type(exc).__name__}: {exc}"
            print(f"[llm_processor] ERROR in usecase_diagrams task: {error_msg}")
            return LLMResult(
                task="usecase_diagrams",
                input_text=text,
                output_text=text,
                success=False,
                error=error_msg,
            )

    def _run_task(
        self,
        task: LLMTaskName,
        text: str,
        verbose: bool = False,
    ) -> LLMResult:
        """Execute a single LLM task and return an LLMResult."""
        system_prompt = SYSTEM_PROMPTS[task]

        if verbose:
            print(f"\n[llm_processor] Running task '{task}' ...")
            preview = text[:300].replace("\n", " ")
            print(f"  Input ({len(text)} chars): {preview}...")

        try:
            # For confluence_format: extract all <ac:structured-macro> blocks
            # and replace with sentinels before sending to the LLM.  This
            # prevents the LLM from escaping CDATA sections or mangling macros.
            macros: List[str] = []
            llm_input = text
            if task == "confluence_format":
                llm_input, macros = _extract_macros(text)
                print(
                    f"[llm_processor] confluence_format: extracted {len(macros)} macro/table block(s) "
                    f"from input ({len(text)} → {len(llm_input)} chars)"
                )

            human_prompt = HUMAN_PROMPT_TEMPLATES[task].format(text=llm_input)

            messages = [
                SystemMessage(content=system_prompt),
                HumanMessage(content=human_prompt),
            ]
            response = self._llm.invoke(messages)
            output = response.content.strip()  # type: ignore[union-attr]

            # Restore extracted macro blocks verbatim
            if task == "confluence_format" and macros:
                before = output
                output = _restore_macros(output, macros)
                _hp = 'ac:name="plantuml"' in output
                print(
                    f"[llm_processor] confluence_format: restored {len(macros)} block(s) "
                    f"({len(before)} → {len(output)} chars) "
                    f"has_plantuml={_hp}"
                )

            # confluence_format: strip LLM chain-of-thought preamble/postamble.
            # The model sometimes emits reasoning text before/after the XML.
            # Keep only the content from the first XML tag onwards.
            if task == "confluence_format":
                output = _strip_llm_preamble(output)

            # plantuml_diagram: if LLM signals no diagram possible, pass text through unchanged
            if task == "plantuml_diagram" and output.strip() == "NO_DIAGRAM":
                output = text

            if verbose:
                preview_out = output[:300].replace("\n", " ")
                print(f"  Output ({len(output)} chars): {preview_out}...")

            return LLMResult(
                task=task,
                input_text=text,
                output_text=output,
                success=True,
                error=None,
            )

        except Exception as exc:
            error_msg = f"{type(exc).__name__}: {exc}"
            print(f"[llm_processor] ERROR in task '{task}': {error_msg}")
            return LLMResult(
                task=task,
                input_text=text,
                output_text=text,   # fallback: pass input unchanged
                success=False,
                error=error_msg,
            )


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _strip_llm_preamble(text: str) -> str:
    """
    Remove chain-of-thought reasoning the LLM emits before/after the real XML.

    Strategy:
    1. Split on lines that look like "top-level" XML block openers
       (<p>, <h2>, <ul>, <ol>, <ac:structured-macro, <table).
    2. Collect contiguous runs of XML lines.
    3. Return the LAST such run — models that think-aloud repeat themselves and
       put the correct final answer last (after "Thus final answer." etc.).
    """
    # Pre-process: if the LLM writes "some prose.<p>..." on one line, split it
    # so the <p> tag starts its own line and we don't discard it as prose.
    text = re.sub(r'([^<\n])(<(?:p|h[1-6]|ul|ol|ac:structured-macro)\b)', r'\1\n\2', text, flags=re.IGNORECASE)

    lines = text.splitlines()

    # A line "belongs to XML" if it starts with a recognised tag or is a
    # continuation of a multi-line XML block (starts with whitespace, </, or
    # is inside a CDATA section, or is blank between XML lines).
    _TOP_LEVEL = re.compile(
        r'^\s*<(?:p|h[1-6]|ul|ol|li|strong|em|table|ac:structured-macro|ac:image)\b',
        re.IGNORECASE,
    )

    # Split the text into blocks separated by non-XML "reasoning" lines.
    # A block boundary is a non-blank line that looks like plain prose
    # (doesn't start with < and isn't inside a CDATA section).
    blocks: List[List[str]] = []
    current: List[str] = []
    in_cdata = False

    for line in lines:
        stripped = line.strip()

        # Track CDATA entry/exit so we never break inside a code block
        if '<![CDATA[' in line:
            in_cdata = True
        if ']]>' in line:
            in_cdata = False

        if in_cdata or stripped == '':
            # Always keep blank lines and CDATA content with current block
            current.append(line)
            continue

        is_xml_line = stripped.startswith('<') or stripped.startswith(']]>')
        if is_xml_line:
            current.append(line)
        else:
            # Plain-text prose line — potential block boundary
            if current:
                blocks.append(current)
                current = []
            # Discard the prose line itself

    if current:
        blocks.append(current)

    if not blocks:
        return text.strip()

    # Return the LAST block that contains at least one top-level XML tag
    for block in reversed(blocks):
        block_text = '\n'.join(block).strip()
        if _TOP_LEVEL.search(block_text):
            return block_text

    # Fallback: return last block regardless
    return '\n'.join(blocks[-1]).strip()


def _extract_macro_blocks_only(text: str) -> str:
    """
    Extract only <ac:structured-macro>…</ac:structured-macro> blocks from LLM output,
    discarding any surrounding prose or preamble.

    Used by _run_usecase_diagrams_task to strip any extra commentary the LLM
    might emit alongside the two diagram macro blocks.

    Returns the concatenation of all macro blocks found, or empty string if none.
    """
    matches = _MACRO_RE.findall(text)
    if not matches:
        return ""
    return "\n".join(matches)


def _xml_escape_text(text: str) -> str:
    """Minimal XML escaping for Confluence storage format text nodes."""
    return (
        text.replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
            .replace('"', "&quot;")
    )


def _table_to_html(table: dict) -> str:
    """Render a TableData dict as Confluence-compatible HTML <table> markup."""
    rows = table.get("rows", [])
    if not rows:
        return ""
    table_parts = ["<table><tbody>"]
    for i, row in enumerate(rows):
        table_parts.append("<tr>")
        tag = "th" if (i == 0 and table.get("header_row", False)) else "td"
        for cell in row:
            table_parts.append(f"<{tag}>{_xml_escape_text(cell)}</{tag}>")
        table_parts.append("</tr>")
    table_parts.append("</tbody></table>")
    return "\n".join(table_parts)


def _build_initial_text(section: ParsedSection) -> str:
    """
    Combine section content into a single string for LLM input, preserving
    the original document order of text paragraphs, tables, and images.

    Tables are rendered as Confluence HTML <table> elements — the confluence_format
    task protects these via _TABLE_RE sentinels in _extract_macros(), so they pass
    through the LLM untouched.

    Images are represented as ``%%IMG_N%%`` sentinel tokens (where N is the image's
    index in section["images"]).  The confluence_format task forwards these tokens
    as plain text; _upload_images_and_update_page() resolves them to <ac:image>
    macros after uploading the attachments.

    Falls back to the legacy flat approach (raw_text then all tables) for sections
    that were parsed before element_sequence was introduced.
    """
    element_sequence = section.get("element_sequence") or []

    # ── New path: use element_sequence for correct document ordering ──────────
    if element_sequence:
        parts = []
        for elem in element_sequence:
            btype = elem.get("block_type", "")
            if btype == "text":
                t = elem.get("text") or ""
                if t:
                    parts.append(t)
            elif btype == "table":
                t_idx = elem.get("table_index", 0)
                tables = section.get("tables", [])
                if 0 <= t_idx < len(tables):
                    html = _table_to_html(tables[t_idx])
                    if html:
                        parts.append(html)
            elif btype == "image":
                i_idx = elem.get("image_index", 0)
                parts.append(f"%%IMG_{i_idx}%%")
        return "\n".join(parts)

    # ── Legacy fallback: flat raw_text then all tables (no image sentinels) ───
    parts = []
    if section["raw_text"]:
        parts.append(section["raw_text"])

    for table in section.get("tables", []):
        html = _table_to_html(table)
        if html:
            parts.append(html)

    return "\n".join(parts)


# ─── Macro Extraction Helpers ─────────────────────────────────────────────────
# Used by confluence_format to prevent the LLM from mangling <ac:structured-macro>
# blocks (especially CDATA sections in plantuml / code macros).

_MACRO_RE = re.compile(
    r'<ac:structured-macro\b.*?</ac:structured-macro>',
    re.DOTALL | re.IGNORECASE,
)
_SENTINEL_PATTERN = "%%MACRO_{n}%%"
# Also protect two-column layout tables that were already built by plantuml_renderer
_TABLE_RE = re.compile(
    r'<table\b[^>]*>.*?</table>',
    re.DOTALL | re.IGNORECASE,
)
_TABLE_SENTINEL = "%%TABLE_{n}%%"


def _extract_macros(text: str) -> tuple:
    """
    Replace every <ac:structured-macro>…</ac:structured-macro> block and every
    plantuml layout <table> with a unique sentinel token.

    Returns (sanitised_text, list_of_original_blocks).
    The i-th item in the list corresponds to sentinel %%MACRO_i%% or %%TABLE_i%%.
    All blocks are stored in a single list; sentinels encode their own index.
    """
    blocks: List[str] = []

    def _replacer_macro(m: re.Match) -> str:
        idx = len(blocks)
        blocks.append(m.group(0))
        return _SENTINEL_PATTERN.format(n=idx)

    def _replacer_table(m: re.Match) -> str:
        idx = len(blocks)
        blocks.append(m.group(0))
        return _TABLE_SENTINEL.format(n=idx)

    sanitised = _MACRO_RE.sub(_replacer_macro, text)
    sanitised = _TABLE_RE.sub(_replacer_table, sanitised)
    return sanitised, blocks


def _restore_macros(text: str, blocks: List[str]) -> str:
    """
    Replace %%MACRO_N%% and %%TABLE_N%% sentinels with the original blocks.
    Handles the common case where the LLM wraps the sentinel in <p>…</p> by
    replacing <p>%%SENTINEL%%</p> (with optional whitespace) directly, so the
    restored block is not surrounded by paragraph tags.
    """
    for idx, block in enumerate(blocks):
        macro_sentinel = _SENTINEL_PATTERN.format(n=idx)
        table_sentinel = _TABLE_SENTINEL.format(n=idx)

        # Try replacing <p>sentinel</p> first (LLM wraps sentinels in <p> tags)
        p_macro = re.compile(r'<p>\s*' + re.escape(macro_sentinel) + r'\s*</p>', re.IGNORECASE)
        p_table = re.compile(r'<p>\s*' + re.escape(table_sentinel) + r'\s*</p>', re.IGNORECASE)

        if p_macro.search(text):
            text = p_macro.sub(block, text)
        elif p_table.search(text):
            text = p_table.sub(block, text)
        # Then try bare sentinel (no <p> wrapping)
        elif macro_sentinel in text:
            text = text.replace(macro_sentinel, block)
        elif table_sentinel in text:
            text = text.replace(table_sentinel, block)
        else:
            # Could not find sentinel — append the block at the end so it's
            # not silently lost
            print(
                f"[llm_processor] WARN: could not restore macro block {idx} "
                f"(sentinel not found in LLM output) — appending at end"
            )
            text = text + "\n" + block

    # Final safety pass: unwrap any <p><ac:structured-macro…></ac:structured-macro></p>
    # that may still remain (e.g. if the LLM wrapped the already-restored block)
    text = re.sub(
        r'<p>\s*(<ac:structured-macro\b.*?</ac:structured-macro>)\s*</p>',
        r'\1',
        text,
        flags=re.DOTALL | re.IGNORECASE,
    )
    # Same for <p><table…></table></p>
    text = re.sub(
        r'<p>\s*(<table\b.*?</table>)\s*</p>',
        r'\1',
        text,
        flags=re.DOTALL | re.IGNORECASE,
    )
    return text
