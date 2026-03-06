"""
CLI entry point for the doc_to_confluence migration tool.

Subcommands:
  migrate  Run the full Word → Confluence migration pipeline
  parse    Parse a Word document and print the section tree (debug utility)

Usage:
  python -m doc_to_confluence migrate --doc path/to/file.docx --config path/to/config.yaml
  python -m doc_to_confluence migrate --doc file.docx --config config.yaml --dry-run --verbose
  python -m doc_to_confluence migrate --doc file.docx --config config.yaml --output-json
  python -m doc_to_confluence parse --doc path/to/file.docx
"""
import argparse
import json
import os
import sys
import traceback


def cmd_migrate(args: argparse.Namespace) -> int:
    """
    Run the full migration pipeline.
    Returns exit code: 0 for all success/skipped, 1 if any sections failed.
    """
    from .config import load_config
    from .orchestrator import MigrationOrchestrator

    if not os.path.isfile(args.doc):
        print(f"ERROR: Document not found: {args.doc}", file=sys.stderr)
        return 1
    if not os.path.isfile(args.config):
        print(f"ERROR: Config not found: {args.config}", file=sys.stderr)
        return 1

    try:
        config = load_config(args.config)
    except FileNotFoundError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    except Exception as exc:
        print(f"ERROR: Failed to load config '{args.config}': {exc}", file=sys.stderr)
        if args.verbose:
            traceback.print_exc()
        return 1

    orchestrator = MigrationOrchestrator(
        config=config,
        dry_run=args.dry_run,
        verbose=args.verbose,
    )

    try:
        report = orchestrator.run(args.doc, config_path=args.config)
    except Exception as exc:
        print(f"ERROR: Migration failed unexpectedly: {exc}", file=sys.stderr)
        if args.verbose:
            traceback.print_exc()
        return 1

    if args.output_json:
        print(json.dumps(report, indent=2, default=str))

    any_failed = any(r["status"] == "failed" for r in report["results"])
    return 1 if any_failed else 0


def cmd_parse(args: argparse.Namespace) -> int:
    """
    Parse a Word document and print the section tree to stdout.
    Useful for understanding document structure before writing a config.
    """
    from .orchestrator import _flatten_sections
    from .parser import parse_docx

    if not os.path.isfile(args.doc):
        print(f"ERROR: Document not found: {args.doc}", file=sys.stderr)
        return 1

    try:
        sections = parse_docx(args.doc)
        flat = _flatten_sections(sections)
    except Exception as exc:
        print(f"ERROR: Failed to parse document: {exc}", file=sys.stderr)
        return 1

    print(f"Parsed {len(flat)} sections from: {args.doc}\n")
    for s in flat:
        indent = "  " * max(0, s["level"] - 1)
        print(f"{indent}[{s['id']}] L{s['level']} ({s['content_type']}) | {s['title']!r}")
        if s["raw_text"]:
            preview = s["raw_text"][:120].replace("\n", " ")
            print(f"{indent}  text ({len(s['raw_text'])} chars): {preview}")
        if s["tables"]:
            print(f"{indent}  tables: {len(s['tables'])}")
        if s["children"]:
            child_titles = [c["title"] for c in s["children"][:3]]
            extra = f" (+{len(s['children']) - 3} more)" if len(s["children"]) > 3 else ""
            print(f"{indent}  children: {child_titles}{extra}")
        print()

    return 0


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="doc_to_confluence",
        description="Migrate Word document sections to Confluence pages using LLM processing.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Inspect a Word document's section structure
  python -m doc_to_confluence parse --doc spec.docx

  # Dry run: parse + LLM process but do not push to Confluence
  python -m doc_to_confluence migrate --doc spec.docx --config config.yaml --dry-run --verbose

  # Full migration
  python -m doc_to_confluence migrate --doc spec.docx --config config.yaml

  # Output migration report as JSON (for CI/CD)
  python -m doc_to_confluence migrate --doc spec.docx --config config.yaml --output-json
""",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    # ── migrate subcommand ───────────────────────────────────────────────────
    migrate_p = subparsers.add_parser(
        "migrate",
        help="Run the full Word → Confluence migration pipeline",
    )
    migrate_p.add_argument(
        "--doc",
        required=True,
        metavar="PATH",
        help="Path to the .docx file to migrate",
    )
    migrate_p.add_argument(
        "--config",
        required=True,
        metavar="PATH",
        help="Path to the migration config YAML file",
    )
    migrate_p.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="Parse and LLM-process but do NOT push to Confluence",
    )
    migrate_p.add_argument(
        "--verbose",
        action="store_true",
        default=False,
        help="Print parsed sections, LLM inputs/outputs, and final content",
    )
    migrate_p.add_argument(
        "--output-json",
        action="store_true",
        default=False,
        help="Print the full migration report as JSON to stdout",
    )
    migrate_p.set_defaults(func=cmd_migrate)

    # ── parse subcommand (debug) ─────────────────────────────────────────────
    parse_p = subparsers.add_parser(
        "parse",
        help="Parse a Word document and print sections (no LLM, no Confluence)",
    )
    parse_p.add_argument(
        "--doc",
        required=True,
        metavar="PATH",
        help="Path to the .docx file",
    )
    parse_p.set_defaults(func=cmd_parse)

    args = parser.parse_args()
    sys.exit(args.func(args))


if __name__ == "__main__":
    main()
