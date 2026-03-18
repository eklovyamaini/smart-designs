"""
Configuration loading and validation for the doc_to_confluence migration tool.

Supports environment variable substitution using ${VAR_NAME} syntax in YAML values.
All credentials (API tokens) should use env-var references rather than being hardcoded.

Usage:
    from doc_to_confluence.config import load_config
    config = load_config("path/to/config.yaml")
    # config is a validated MigrationConfigModel instance
"""
import os
import re
from typing import Any, List, Literal, Optional

import yaml
from pydantic import BaseModel, Field, field_validator, model_validator


# ─── Pydantic Models ──────────────────────────────────────────────────────────

class ConfluenceTargetModel(BaseModel):
    space_key: str = ""
    parent_page_id: Optional[str] = None
    page_id: Optional[str] = None
    action: Literal["create", "update", "append"] = "create"
    folder_path: Optional[str] = None
    folder_only: bool = False
    page_title: Optional[str] = None   # explicit title override; None = use section heading
    table_rows_to_pages: bool = False  # create one page per data row in section tables
    row_page_title: Optional[str] = None  # page title template; {col_0} … filled per row
    use_case_page_title: Optional[str] = None  # page title template for per-use-case child pages; {use_case_name} is filled per use case
    """
    When True, this section is treated purely as a folder definition.
    No Confluence page is created for the section itself.
    Its folder_path (or title if no folder_path is set) becomes the
    inherited folder context for all subsequent sections that have no
    explicit folder_path of their own.
    """
    """
    Optional slash-separated folder hierarchy within the space, e.g.
    "Engineering/Backend/Services".  Each segment is treated as a Confluence
    page title.  Missing segments are created automatically as empty pages
    under the space root (or under parent_page_id if also supplied).
    The resolved leaf-folder page ID is used as the parent for the migrated
    content page, overriding parent_page_id.
    """

    @field_validator("folder_path")
    @classmethod
    def normalise_folder_path(cls, v: Optional[str]) -> Optional[str]:
        if not v:
            return None
        # Strip surrounding slashes, collapse double-slashes, remove blank segments
        parts = [p.strip() for p in v.strip("/").split("/") if p.strip()]
        return "/".join(parts) if parts else None

    @model_validator(mode="after")
    def validate_action_fields(self) -> "ConfluenceTargetModel":
        # folder_only sections skip page creation entirely — no field requirements
        if self.folder_only:
            return self
        if self.action in ("update", "append") and not self.page_id:
            raise ValueError(
                f"action='{self.action}' requires page_id to be set"
            )
        if self.action == "create" and not self.space_key:
            raise ValueError("action='create' requires space_key to be set")
        return self


class LLMConfigModel(BaseModel):
    enabled: bool = True
    tasks: List[Literal["reformat", "summarize", "plantuml_diagram", "usecase_diagrams", "confluence_format"]] = Field(
        default_factory=list
    )
    expand_tables_to_pages: bool = False  # create one Confluence page per table row (section + all subsections); each page shows the row transposed into a 2-column header/value table
    expand_usecases_to_pages: bool = False  # split "Use Case:" blocks into individual child Confluence pages, each with the use case text + its PlantUML diagram

    @field_validator("tasks")
    @classmethod
    def validate_task_order(cls, tasks: list) -> list:
        """
        confluence_format must be the last task if present.
        plantuml_diagram / usecase_diagrams must come before confluence_format.
        """
        VALID = {"reformat", "summarize", "plantuml_diagram", "usecase_diagrams", "confluence_format"}
        for t in tasks:
            if t not in VALID:
                raise ValueError(f"Unknown LLM task: '{t}'. Valid tasks: {sorted(VALID)}")
        if "confluence_format" in tasks and tasks[-1] != "confluence_format":
            raise ValueError("'confluence_format' must be the last task in the tasks list")
        return tasks


class SectionMappingModel(BaseModel):
    match: str
    match_type: Literal["heading", "custom_delimiter", "table", "regex"] = "heading"
    level: int = 0          # heading level (1=H1, 2=H2, 3=H3 …); 0 = unknown
    section_id: Optional[str] = None  # exact ParsedSection.id; when set, bypasses regex matching
    confluence: ConfluenceTargetModel
    llm: LLMConfigModel = Field(default_factory=LLMConfigModel)

    # Pre-compiled regex - not a pydantic field, set in model_validator
    _compiled_regex: Optional[re.Pattern] = None

    model_config = {"arbitrary_types_allowed": True}

    @model_validator(mode="after")
    def compile_match_regex(self) -> "SectionMappingModel":
        """
        Pre-compile match field as a case-insensitive regex.

        When section_id is set the match string is used only as a human-readable
        label — exact matching is done by ID, so we fall back to a literal
        (re.escape) pattern to avoid regex errors from titles that contain
        parentheses or other special characters (e.g. "S101 – Org (Read Only)").

        For match_type='table' the match string is ignored entirely.
        """
        try:
            if self.section_id:
                # Treat the title as a literal string, not a regex pattern
                pattern = re.compile(re.escape(self.match), re.IGNORECASE)
            else:
                pattern = re.compile(self.match, re.IGNORECASE)
            object.__setattr__(self, "_compiled_regex", pattern)
        except re.error as exc:
            raise ValueError(f"Invalid regex in match field '{self.match}': {exc}") from exc
        return self

    def matches_title(self, title: str) -> bool:
        """
        Return True if this mapping applies to the given section title.
        Uses pre-compiled case-insensitive regex search.
        """
        if self.match_type == "table":
            return False  # table matching is done by content_type, not title
        pattern = object.__getattribute__(self, "_compiled_regex")
        if pattern is None:
            return False
        return bool(pattern.search(title))


class MigrationConfigModel(BaseModel):
    llm_model: str = "gpt-oss:20b"
    llm_temperature: float = 0.1
    max_llm_workers: int = 4
    plantuml_theme: str = "cerulean"
    confluence_base_url: str
    confluence_user: str
    confluence_api_token: str
    db_logging: bool = True
    db_path: str = "doc_to_confluence/migration.db"
    sections: List[SectionMappingModel]

    @field_validator("confluence_base_url")
    @classmethod
    def strip_trailing_slash(cls, v: str) -> str:
        return v.rstrip("/")

    @field_validator("llm_temperature")
    @classmethod
    def clamp_temperature(cls, v: float) -> float:
        if not (0.0 <= v <= 2.0):
            raise ValueError("llm_temperature must be between 0.0 and 2.0")
        return v

    @field_validator("sections")
    @classmethod
    def require_at_least_one_section(cls, v: list) -> list:
        if not v:
            raise ValueError("sections list must contain at least one entry")
        return v


# ─── Public Loader ────────────────────────────────────────────────────────────

def load_config(config_path: str) -> MigrationConfigModel:
    """
    Load and validate a migration config YAML file.

    Supports ${ENV_VAR_NAME} substitution in all string values.
    If an env var is not set, the placeholder is left as-is (which will
    cause a Pydantic validation error for required fields like api_token).

    Args:
        config_path: Path to the YAML config file.

    Returns:
        Validated MigrationConfigModel instance.

    Raises:
        FileNotFoundError: if config_path does not exist.
        yaml.YAMLError: if the YAML is malformed.
        pydantic.ValidationError: if the config structure is invalid.
    """
    if not os.path.isfile(config_path):
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with open(config_path, "r", encoding="utf-8") as f:
        raw: Any = yaml.safe_load(f) or {}

    raw = _substitute_env_vars(raw)

    return MigrationConfigModel(**raw)


# ─── Internal Helpers ─────────────────────────────────────────────────────────

def _substitute_env_vars(obj: Any) -> Any:
    """
    Recursively walk a YAML-parsed structure and replace ${VAR_NAME}
    patterns with environment variable values.
    Leaves the placeholder intact if the env var is not set.
    """
    if isinstance(obj, str):
        def replacer(m: re.Match) -> str:
            return os.environ.get(m.group(1), m.group(0))
        return re.sub(r"\$\{([^}]+)\}", replacer, obj)
    if isinstance(obj, dict):
        return {k: _substitute_env_vars(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_substitute_env_vars(item) for item in obj]
    return obj
