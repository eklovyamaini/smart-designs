# Smart Designs — Doc to Confluence & Metadata Manager

A suite of tools that migrate structured Word documents into Confluence page hierarchies and apply post-migration metadata operations at scale.

---

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Installation](#installation)
3. [Starting the App](#starting-the-app)
4. [Doc to Confluence — Migration Tool](#doc-to-confluence--migration-tool)
   - [Step 1 — Upload & Parse](#step-1--upload--parse)
   - [Step 2 — Configure Rules](#step-2--configure-rules)
   - [Step 3 — Run Migration](#step-3--run-migration)
   - [Section Defaulting Rules](#section-defaulting-rules)
5. [Metadata Manager](#metadata-manager)
   - [Find Module Pages](#find-module-pages)
   - [Operations](#operations)
   - [Review Dashboard](#review-dashboard)
6. [Configuration Files](#configuration-files)

---

## Prerequisites

| Requirement | Version | Notes |
|---|---|---|
| Python | 3.8 + | Miniconda recommended |
| Confluence | Cloud or Data Center | API token required |
| Ollama | latest | For LLM-powered sections (PlantUML, formatting) |
| PlantUML | optional | For local diagram preview |

---

## Installation

```bash
# 1. Clone the repository
git clone https://github.com/eklovyamaini/smart-designs.git
cd smart-designs

# 2. Create and activate a conda environment (recommended)
conda create -n smart-designs python=3.11
conda activate smart-designs

# 3. Install dependencies
pip install -r requirements.txt
pip install -r doc_to_confluence/requirements.txt

# 4. (Optional) Install smart_agents dependencies
pip install -r smart_agents/requirements.txt
```

### Confluence API Token

Generate a token at **Confluence → Profile → Security → API tokens** and keep it ready for the connection fields in both tools.

---

## Starting the App

```bash
./start.sh
```

This starts both services in the background:

| Service | URL | Log |
|---|---|---|
| **Doc to Confluence** | http://localhost:8001 | `/tmp/doc_to_confluence.log` |
| **Smart Agents** | http://localhost:8000 | `/tmp/smart_agents.log` |

To stop all services:

```bash
./stop.sh
```

---

## Doc to Confluence — Migration Tool

Access at **http://localhost:8001**

The tool follows a three-step wizard that parses a Word document, lets you configure per-section Confluence mapping rules, and then runs the migration with live progress streaming.

---

### Step 1 — Upload & Parse

Upload any `.docx` file using drag-and-drop or the file browser. The parser extracts every heading (H1 – H5) and builds a section tree. A badge shows the total number of sections found.

**What the parser understands:**

- Heading levels map to section depth (H3/H4/H5 are treated as siblings for flatter modules)
- Tables, images, and PlantUML code blocks inside sections are preserved
- Use-case blocks (`Use Case: <name>` paragraphs) are detected and split automatically

---

### Step 2 — Configure Rules

Configure the Confluence connection and per-section mapping rules before running.

#### Confluence Connection

| Field | Description |
|---|---|
| Base URL | e.g. `https://yourorg.atlassian.net/` |
| User (email) | Your Atlassian account email |
| API Token | Generated in Confluence security settings |
| Default Approvers | Pre-filled on every Page Properties table |

Click **Save as Defaults** to persist these settings across sessions.

#### LLM Settings

| Field | Description |
|---|---|
| Model | Ollama model name (e.g. `llama3`, `mistral`) |
| Max Workers | Parallel LLM threads (1 – 16) |
| Temperature | Creativity of LLM output (0 = deterministic) |
| PlantUML Theme | Visual theme for generated diagrams |

#### Section Rules

Each parsed section is matched against the **Section Defaulting Rules** (YAML). Matched rules auto-populate the rule row for that section — page title template, folder path, LLM tasks, and expansion options. You can override any field per-row before running.

Click **Section Defaulting Rules** to view or edit the YAML directly in the browser.

---

### Step 3 — Run Migration

| Toggle | Effect |
|---|---|
| Dry Run | Simulates migration without writing to Confluence (default: on) |
| Overwrite Existing Pages | Updates pages that already exist (disabled in dry-run) |
| Delete Existing Module Pages | Removes the module folder tree before re-migrating |

Click **Start Migration** to begin. A live progress bar and colour-coded result feed stream in real time. When complete, download a full **JSON report** for audit purposes.

---

### Section Defaulting Rules

Rules are evaluated top-to-bottom. The first match wins. Unmatched sections are skipped.

| # | Rule Name | Matches | Action |
|---|---|---|---|
| 1 | Module Header | `^Module\s*[-–]\s*(.+)` | Skip — captures `module_name` into context |
| 2 | Business Process | `Business Process` | Create page with LLM PlantUML + formatting |
| 3 | Accessing this Module | `Accessing this Module` | Append content into Business Process page |
| 4 | Screen Designs Folder | `Screen Designs` | Create folder only |
| 5 | Individual Screen | `^(S\d{3})\s*[-–]\s*(.+)` | Create screen folder, capture `screen_code` |
| 6 | Functional Description | `Functional Description` | Create page under screen folder |
| 7 | Use Cases | `Use Cases?` | Create parent page + one child page per use case (with auto-generated PlantUML diagram) |
| 8 | Screen Mockup | `Screen Mockup` | Create page, preserve images |
| 9 | Page Elements Folder | `Page Elements` | Create page, expand table rows to child pages |
| 10 | Page Element Detail | context: `inside_page_elements` | Create child pages from table rows |
| 11 | Catch-all | `.*` | Skip |

#### Template Variables

Available in `page_title` and `folder_path` fields:

| Variable | Value |
|---|---|
| `{{module_name}}` | Captured from Rule 1 (e.g. `Contract Budget Amendment`) |
| `{{screen_code}}` | Captured from Rule 5 (e.g. `S346`) |
| `{{screen_title}}` | Full screen heading (e.g. `S346 – Contract Budget Amendment`) |
| `{{original_title}}` | The section's original heading text |

#### Expand Tables to Pages (Rule 9 / 10)

When `expand_tables_to_pages: true` is set, each data row in the section's table becomes its own Confluence child page. The page title is built from the first column value using the `row_page_title` template (e.g. `"{{screen_code}} - {col_0}"`).

#### Expand Use Cases to Pages (Rule 7)

When `expand_usecases_to_pages: true` is set, each `Use Case: <name>` block within the section becomes its own Confluence child page. The LLM automatically generates a PlantUML use-case diagram for each block and embeds it alongside the description text.

---

## Metadata Manager

Access at **http://localhost:8001/metadata**

Apply post-migration metadata operations across all pages in a module hierarchy. Works in two steps: configure scope → preview → apply.

---

### Find Module Pages

Instead of pasting URLs manually, enter your **Space Key** (e.g. `DS`) and click **Find Module Pages**. The tool scans the space for pages whose titles match the module naming pattern (`{Name} - Module`) and auto-populates the Parent Page URLs textarea.

---

### Operations

Select any combination of operations using the checkboxes. All are duplicate-safe — existing metadata blocks, macros, and labels are detected before applying to avoid duplication.

#### 📋 Page Properties Table

Injects a structured metadata table at the top of each page:

| Column | Source |
|---|---|
| Author | Confluence page creator |
| Status | Default: `Draft` |
| Module | Inferred from page hierarchy |
| Page Type | Inferred from page title pattern |
| Last Updated | Page modification date |
| Approvers | Default Approvers field from connection settings |

#### 🕓 Change History Macro

Adds a collapsible native Confluence **Page History** macro at the bottom of each page, letting readers see the full revision history inline.

#### 🏷️ Auto-label Pages

Adds smart labels based on page position in the hierarchy:

| Label type | Example |
|---|---|
| Module name | `contract-budget-amendment` |
| Page type | `use-case`, `screen-design`, `functional-description` |
| Tracking label | `ds-tracked` (configurable) |

Existing labels are never duplicated.

#### 📊 Create / Update Review Dashboard

Generates (or updates) a Confluence page containing a **Page Properties Report** macro — a live filterable spreadsheet showing every page's review status, module, approvers, and more. The dashboard updates automatically as page properties change.

Configure the dashboard page title (default: `Review Dashboard`) in the sub-field that appears when this operation is checked. Space Key is reused from the Find Module Pages row — no duplicate entry needed.

**Force Re-apply** — check this box to overwrite metadata blocks on pages that already have them (useful after changing templates).

---

## Configuration Files

| File | Purpose |
|---|---|
| `doc_to_confluence/frontend/section_defaults.yaml` | Section matching rules (edit in browser or directly) |
| `doc_to_confluence/frontend/defaults.yaml` | Persisted connection settings and LLM config |
| `.claude/launch.json` | Dev server launch config for Claude Code |
| `start.sh` / `stop.sh` | Service management scripts |

---

## Architecture

```
smart-designs/
├── doc_to_confluence/          # Word → Confluence migration tool
│   ├── frontend/               # FastAPI web app (port 8001)
│   │   ├── main.py             # Routes: /, /parse, /migrate, /metadata/*
│   │   ├── templates/          # Jinja2 HTML (index.html, metadata.html)
│   │   ├── static/             # app.js, metadata.js, styles
│   │   └── section_defaults.yaml
│   ├── parser.py               # .docx → ParsedSection tree
│   ├── orchestrator.py         # Section → Confluence page mapping & creation
│   ├── llm_processor.py        # LLM task runner (PlantUML, formatting)
│   ├── confluence_client.py    # Confluence REST API wrapper
│   ├── metadata_manager.py     # Post-migration metadata operations
│   └── plantuml_renderer.py    # PlantUML → PNG rendering
├── smart_agents/               # Smart agents UI (port 8000)
├── start.sh                    # Start both services
├── stop.sh                     # Stop both services
└── requirements.txt
```
