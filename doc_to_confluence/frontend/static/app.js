/**
 * app.js – Word to Confluence Migration Frontend
 *
 * 3-step wizard:
 *   Step 1: Upload .docx → parse → display section tree
 *   Step 2: Build per-section migration rules / config
 *   Step 3: Run migration with SSE live progress
 */

'use strict';

// ── App State ─────────────────────────────────────────────────────────────────

const APP = {
  currentStep:          1,
  fileId:               null,   // UUID hex from /parse response
  filename:             null,
  sections:             [],     // flat List[ParsedSection] from /parse
  pendingConfig:        null,   // config dict assembled in step 2
  lastReport:           null,   // MigrationReport from /migrate complete event
  sectionDefaultsConfig: null,  // full parsed config (context_vars + rules)
  sectionDefaultRules:   [],    // convenience alias for config.rules
};

// ── DOM helpers ───────────────────────────────────────────────────────────────

const $  = id  => document.getElementById(id);
const $q = sel => document.querySelector(sel);
const $a = sel => [...document.querySelectorAll(sel)];

// ── Escape HTML ───────────────────────────────────────────────────────────────

function esc(str) {
  if (str === null || str === undefined) return '';
  return String(str)
    .replace(/&/g,  '&amp;')
    .replace(/</g,  '&lt;')
    .replace(/>/g,  '&gt;')
    .replace(/"/g,  '&quot;')
    .replace(/'/g,  '&#039;');
}

// ═══════════════════════════════════════════════════════════════════════════════
// STEP NAVIGATION
// ═══════════════════════════════════════════════════════════════════════════════

function showStep(n) {
  for (let i = 1; i <= 3; i++) {
    const panel = $(`step-${i}`);
    if (!panel) continue;
    panel.classList.toggle('active',  i === n);
    panel.classList.toggle('hidden',  i !== n);

    const dot = document.querySelector(`.step-dot[data-step="${i}"]`);
    if (dot) {
      dot.classList.remove('active', 'completed');
      if (i < n) dot.classList.add('completed');
      else if (i === n) dot.classList.add('active');
    }
  }
  APP.currentStep = n;
  window.scrollTo({ top: 0, behavior: 'smooth' });
}

// ═══════════════════════════════════════════════════════════════════════════════
// STEP 1 — Upload & Parse
// ═══════════════════════════════════════════════════════════════════════════════

function initStep1() {
  const zone      = $('upload-zone');
  const fileInput = $('file-input');
  const browseBtn = $('browse-btn');
  const nextBtn   = $('step1-next');
  const expandBtn = $('expand-all-btn');

  // Prevent zone click from double-triggering when clicking the input itself
  browseBtn.addEventListener('click', e => { e.stopPropagation(); fileInput.click(); });
  // Only open file picker when clicking the zone itself (not child elements after parse)
  zone.addEventListener('click', e => {
    // Ignore if the click originated from the browse button or file input
    if (e.target === fileInput || e.target === browseBtn) return;
    // Don't re-open picker if we already have a parsed file
    if (APP.fileId) return;
    fileInput.click();
  });

  // Drag & drop
  zone.addEventListener('dragover', e => { e.preventDefault(); zone.classList.add('dragover'); });
  ['dragleave', 'dragend'].forEach(ev => zone.addEventListener(ev, () => zone.classList.remove('dragover')));
  zone.addEventListener('drop', e => {
    e.preventDefault();
    zone.classList.remove('dragover');
    const file = e.dataTransfer.files[0];
    if (file) handleFileSelected(file);
  });

  // File input change
  fileInput.addEventListener('change', () => {
    if (fileInput.files.length) handleFileSelected(fileInput.files[0]);
  });

  // Next button
  nextBtn.addEventListener('click', () => {
    renderRules(APP.sections);
    recomputeFolderPaths();
    showStep(2);
  });

  // Expand / collapse all sections in the tree
  let allExpanded = false;
  expandBtn.addEventListener('click', () => {
    allExpanded = !allExpanded;
    $a('.section-item').forEach(el => {
      el.classList.toggle('expanded', allExpanded);
      const t = el.querySelector('.section-toggle');
      if (t) t.textContent = allExpanded ? '▼' : '▶';
    });
    expandBtn.textContent = allExpanded ? 'Collapse All' : 'Expand All';
  });
}

async function handleFileSelected(file) {
  if (!file.name.toLowerCase().endsWith('.docx')) {
    showUploadStatus('error', 'Only .docx files are supported.');
    return;
  }

  showUploadStatus('loading', `Uploading and parsing "${file.name}"…`);
  $('step1-next').disabled = true;
  $('sections-panel').classList.add('hidden');

  const fd = new FormData();
  fd.append('file', file);

  try {
    const resp = await fetch('/parse', { method: 'POST', body: fd });
    const data = await resp.json();

    if (!resp.ok) {
      showUploadStatus('error', data.error || 'Upload failed.');
      return;
    }

    APP.fileId   = data.file_id;
    APP.filename = data.filename;
    APP.sections = data.sections || [];

    showUploadStatus('success', `Parsed ${APP.sections.length} sections from "${data.filename}"`);
    renderSectionsTree(APP.sections);
    $('sections-panel').classList.remove('hidden');
    $('step1-next').disabled = false;
    $('step1-hint').textContent = `${APP.sections.length} section${APP.sections.length !== 1 ? 's' : ''} ready`;

    // Show a "change document" link so the user can deliberately swap files
    let changeLink = $('change-doc-link');
    if (!changeLink) {
      changeLink = document.createElement('button');
      changeLink.id = 'change-doc-link';
      changeLink.className = 'link-btn';
      changeLink.style.marginLeft = '0.75rem';
      changeLink.textContent = 'Change document';
      changeLink.addEventListener('click', () => {
        APP.fileId = null;  // re-enable zone click
        fileInput.value = '';
        fileInput.click();
      });
      $('upload-status').after(changeLink);
    }

  } catch (err) {
    showUploadStatus('error', `Network error: ${err.message}`);
  }
}

function showUploadStatus(type, msg) {
  const el = $('upload-status');
  el.className = `upload-status status-${type}`;
  const prefix = { loading: '⏳ ', success: '✓ ', error: '✕ ' }[type] || '';
  el.textContent = prefix + msg;
  el.classList.remove('hidden');
}

// ── Sections Tree ─────────────────────────────────────────────────────────────

const LEVEL_BADGE  = { 0: ['badge-h0','TBL'], 1: ['badge-h1','H1'], 2: ['badge-h2','H2'], 3: ['badge-h3','H3'] };
const TYPE_BADGE   = { text: ['badge-text','text'], table: ['badge-table','table'], custom: ['badge-custom','custom'] };

function renderSectionsTree(sections) {
  const container = $('sections-tree');
  container.innerHTML = '';
  $('section-count').textContent = sections.length;

  sections.forEach(section => {
    const item = document.createElement('div');
    item.className = 'section-item';
    item.dataset.sectionId = section.id;
    item.dataset.level = section.level;

    const chars      = (section.raw_text || '').length;
    const tableCount = (section.tables || []).length;
    const imageCount = (section.images || []).length;
    const preview    = (section.raw_text || '').slice(0, 250).trim();
    const [lvlCls, lvlTxt] = LEVEL_BADGE[section.level] ?? ['badge-neutral', `L${section.level}`];
    const [typCls, typTxt] = TYPE_BADGE[section.content_type] ?? ['badge-neutral', section.content_type];
    const meta = [];
    if (chars > 0) meta.push(chars.toLocaleString() + ' chars');
    if (tableCount) meta.push(`${tableCount} table${tableCount > 1 ? 's' : ''}`);
    if (imageCount) meta.push(`${imageCount} image${imageCount > 1 ? 's' : ''}`);
    if (!meta.length) meta.push('no content');

    item.innerHTML = `
      <div class="section-item-header">
        <span class="section-toggle">▶</span>
        <span class="section-id">${esc(section.id)}</span>
        <span class="badge ${lvlCls}">${lvlTxt}</span>
        <span class="badge ${typCls}">${typTxt}</span>
        <span class="section-title">${esc(section.title)}</span>
        <span class="section-meta">${meta.join(' · ')}</span>
      </div>
      ${preview ? `<div class="section-preview">${esc(preview)}${chars > 250 ? '…' : ''}</div>` : ''}
    `;

    item.querySelector('.section-item-header').addEventListener('click', () => {
      item.classList.toggle('expanded');
      item.querySelector('.section-toggle').textContent = item.classList.contains('expanded') ? '▼' : '▶';
    });

    container.appendChild(item);
  });
}

// ═══════════════════════════════════════════════════════════════════════════════
// STEP 2 — Build Rules
// ═══════════════════════════════════════════════════════════════════════════════

function initStep2() {
  $('step2-back').addEventListener('click', () => showStep(1));

  $('step2-next').addEventListener('click', () => {
    const config = collectConfig();
    if (!config) return;
    APP.pendingConfig = config;
    showStep(3);
  });

  $('download-config-btn').addEventListener('click', async () => {
    const config = collectConfig();
    if (!config) return;
    await downloadConfigYaml(config);
  });

  $('save-defaults-btn')?.addEventListener('click', saveDefaults);
}

function buildSectionPreviewHTML(section) {
  const chars      = (section.raw_text || '').length;
  const tableCount = (section.tables || []).length;
  const imageCount = (section.images || []).length;
  const preview    = (section.raw_text || '').slice(0, 300).trim();

  let html = `<div class="rule-preview-box">`;

  if (preview) {
    html += `<div class="rule-preview-text">${esc(preview)}${chars > 300 ? '…' : ''}</div>`;
  }

  if (tableCount) {
    // Show a mini preview of the first table (up to 3 rows × 4 cols)
    const tbl = section.tables[0];
    const rows = (tbl.rows || []).slice(0, 3);
    if (rows.length) {
      html += `<div class="rule-preview-table-wrap">`;
      html += `<span class="rule-preview-table-label">${tableCount} table${tableCount > 1 ? 's' : ''} — preview of first:</span>`;
      html += `<table class="rule-preview-table"><tbody>`;
      rows.forEach((row, ri) => {
        html += `<tr>`;
        row.slice(0, 4).forEach(cell => {
          const tag = (ri === 0 && tbl.header_row) ? 'th' : 'td';
          html += `<${tag}>${esc(cell)}</${tag}>`;
        });
        if (row.length > 4) html += `<td class="rule-preview-more">+${row.length - 4}</td>`;
        html += `</tr>`;
      });
      if ((tbl.rows || []).length > 3) {
        html += `<tr><td colspan="5" class="rule-preview-more">+${tbl.rows.length - 3} more rows…</td></tr>`;
      }
      html += `</tbody></table></div>`;
    }
  }

  if (imageCount) {
    // Show image thumbnails (base64 data is in section.images[i].data_b64)
    html += `<div class="rule-preview-images">`;
    html += `<span class="rule-preview-table-label">${imageCount} embedded image${imageCount > 1 ? 's' : ''}:</span>`;
    html += `<div class="rule-preview-image-row">`;
    section.images.slice(0, 4).forEach((img, i) => {
      const src = img.data_b64
        ? `data:${img.content_type || 'image/png'};base64,${img.data_b64}`
        : '';
      if (src) {
        html += `<img class="rule-preview-thumb" src="${src}" alt="${esc(img.filename || 'image ' + (i+1))}" title="${esc(img.filename || '')}" loading="lazy">`;
      } else {
        html += `<span class="rule-preview-img-placeholder">🖼 ${esc(img.filename || 'image')}</span>`;
      }
    });
    if (imageCount > 4) {
      html += `<span class="rule-preview-more">+${imageCount - 4} more</span>`;
    }
    html += `</div></div>`;
  }

  if (!preview && !tableCount && !imageCount) {
    html += `<span class="rule-preview-empty">(no text content)</span>`;
  }

  html += `</div>`;
  return html;
}

/**
 * Expand {{variable}} placeholders in a string using the provided context.
 * {{original_title}} is always available from the section title.
 */
function _interpolate(str, ctx) {
  if (typeof str !== 'string') return str;
  return str.replace(/\{\{(\w+)\}\}/g, (_, key) => (ctx[key] ?? ''));
}

/**
 * Evaluate section_defaults.yaml rules for one section.
 * MUTATES ctx in-place when a capture block fires.
 * Returns the interpolated `apply` object (or null if no match).
 *
 * Rule evaluation order:
 *   1. Check match.title  (regex re.search, case-insensitive, optional)
 *   2. Check match.level  (exact level integer, optional)
 *   3. Check match.context (all key=value pairs must match ctx, optional)
 *   4. First fully-matching rule wins → run its capture, return its apply.
 *
 * Capture values:
 *   "$0"  → full section title
 *   "$1"  → first regex capture group from match.title
 *   "$2"  → second regex capture group, etc.
 *   false / true / "" → literal boolean / empty string (resets)
 */
function _evalSectionRules(section, ctx) {
  const rules = (APP.sectionDefaultsConfig?.rules) || (APP.sectionDefaultRules) || [];
  const title = (section.title || '').trim();
  // Make original_title available in template interpolation
  ctx.original_title = title;

  for (const rule of rules) {
    const m = rule.match || {};

    // ── title regex condition ───────────────────────────────────
    let reMatch = null;
    if (m.title) {
      const re = new RegExp(m.title, 'i');
      reMatch = title.match(re);
      if (!reMatch) continue;   // title doesn't match → try next rule
    }

    // ── level condition ─────────────────────────────────────────
    if (m.level !== undefined && m.level !== null) {
      if (Number(section.level) !== Number(m.level)) continue;
    }

    // ── context condition ───────────────────────────────────────
    if (m.context) {
      let contextOk = true;
      for (const [k, v] of Object.entries(m.context)) {
        if (ctx[k] !== v) { contextOk = false; break; }
      }
      if (!contextOk) continue;
    }

    // ── Match found → resolve captures ─────────────────────────
    if (rule.capture) {
      for (const [k, raw] of Object.entries(rule.capture)) {
        if (raw === false || raw === true) {
          ctx[k] = raw;
        } else if (typeof raw === 'string' && raw.startsWith('$') && reMatch) {
          const idx = parseInt(raw.slice(1), 10);
          ctx[k] = (reMatch[idx] ?? '').trim();
        } else {
          ctx[k] = raw ?? '';
        }
      }
    }

    // ── Interpolate apply values with updated context ───────────
    const rawApply = rule.apply || {};
    const apply = {};
    for (const [k, v] of Object.entries(rawApply)) {
      apply[k] = (typeof v === 'string') ? _interpolate(v, ctx) : v;
    }

    return apply;
  }

  return null; // no rule matched
}

function renderRules(sections) {
  const container = $('rules-container');
  container.innerHTML = '';

  // ── Initialise context state for rule evaluation ──────────────────────────
  // Context persists across sections (module_name, screen_code, etc.)
  const ruleCtx = Object.assign(
    {},
    APP.sectionDefaultsConfig?.context_vars || {},
  );

  // ── Bulk-apply toolbar ────────────────────────────────────────────────────
  // Discover heading levels present in the document
  const levelsPresent = [...new Set(sections.map(s => s.level))].sort();
  if (levelsPresent.length > 0) {
    const toolbar = document.createElement('div');
    toolbar.className = 'bulk-apply-toolbar';
    toolbar.innerHTML = `
      <span class="bulk-apply-label">Apply settings to all sections at level:</span>
      <div class="bulk-apply-btns">
        ${levelsPresent.map(lvl => {
          const [cls, txt] = LEVEL_BADGE[lvl] ?? ['badge-neutral', `L${lvl}`];
          const count = sections.filter(s => s.level === lvl).length;
          return `<button type="button" class="btn btn-sm btn-ghost bulk-level-btn"
                    data-level="${lvl}"
                    title="Copy settings from the first ${txt} row to all other ${txt} rows">
                    <span class="badge ${cls}">${txt}</span>
                    <span class="bulk-btn-label">&nbsp;× ${count}</span>
                  </button>`;
        }).join('')}
      </div>
      <span class="bulk-apply-hint">Copies settings from the first row of that level to all others</span>
    `;

    // Wire up level buttons — they run AFTER all rows have been rendered,
    // so we attach the listener via event delegation from the toolbar.
    toolbar.addEventListener('click', e => {
      const btn = e.target.closest('.bulk-level-btn');
      if (!btn) return;
      const level = parseInt(btn.dataset.level, 10);
      bulkApplyLevel(level, btn);
    });

    container.appendChild(toolbar);
  }

  // Tracks the heading level of the most recent "Page Elements" ancestor (−1 = none).
  // Used to default expand_tables_to_pages for the section and all its subsections.
  let pageElemAncestorLevel = -1;

  sections.forEach((section, idx) => {
    // Evaluate rules with shared context (ruleCtx is MUTATED by captures).
    // Must happen before innerHTML so template can use isFolderSection.
    const sectionApply    = _evalSectionRules(section, ruleCtx);
    const isFolderSection = !!(sectionApply && sectionApply.folder_only);
    const isSkipped       = !!(sectionApply && sectionApply.skip);

    // ── Page Elements ancestor tracking ──────────────────────────────────────
    // When we encounter a same-level or shallower heading we've exited the subtree.
    if (pageElemAncestorLevel >= 0 && section.level <= pageElemAncestorLevel) {
      pageElemAncestorLevel = -1;
    }
    if (/page\s*elements?/i.test(section.title || '')) {
      pageElemAncestorLevel = section.level;
    }
    const underPageElements = pageElemAncestorLevel >= 0;

    const [lvlCls, lvlTxt] = LEVEL_BADGE[section.level] ?? ['badge-neutral', `L${section.level}`];
    const chars      = (section.raw_text || '').length;
    const tableCount = (section.tables || []).length;
    const meta       = [chars.toLocaleString() + ' chars'];
    if (tableCount) meta.push(`${tableCount} table${tableCount > 1 ? 's' : ''}`);

    const detail = document.createElement('details');
    detail.className = 'rule-row';
    detail.dataset.sectionId = section.id;
    // Open first 3 rows by default
    if (idx < 3) detail.setAttribute('open', '');

    detail.innerHTML = `
      <summary class="rule-summary">
        <input type="checkbox" class="rule-enable" checked title="Include in migration">
        <span class="badge ${lvlCls}">${lvlTxt}</span>
        <span class="rule-title">${esc(section.title)}</span>
        <span class="rule-meta">${section.content_type} · ${meta.join(' · ')}</span>
      </summary>
      <div class="rule-body">

        <!-- Section content preview (always shown) -->
        <details class="rule-preview-details" open>
          <summary class="rule-preview-summary">Section Content Preview</summary>
          ${buildSectionPreviewHTML(section)}
        </details>

        <!-- Confluence Target -->
        <fieldset class="rule-fieldset">
          <legend>Confluence Target</legend>

          <!-- Title override (pre-filled by rules; editable by user) -->
          <div class="field-group rule-page-title-wrap">
            <label>
              Title Override
              <span class="field-hint">leave blank to use the section heading</span>
            </label>
            <input type="text" class="rule-page-title"
                   placeholder="leave blank to use section heading">
          </div>

          <!-- Folder Only toggle -->
          <div class="folder-only-row">
            <label class="toggle-label folder-only-label">
              <input type="checkbox" class="rule-folder-only">
              <span class="toggle-text">Folder Only</span>
            </label>
            <span class="folder-only-hint">
              No page created — section title (or Folder Path) becomes the folder context
              inherited by following sections
            </span>
          </div>

          <!-- Normal target fields (hidden when Folder Only is on) -->
          <div class="rule-target-fields">
            <div class="field-group">
              <label>Action</label>
              <select class="rule-action">
                <option value="create" selected>create</option>
                <option value="update">update</option>
                <option value="append">append</option>
              </select>
            </div>
            <div class="field-group rule-space-key-wrap">
              <label>Space Key</label>
              <input type="text" class="rule-space-key" placeholder="PROJ">
            </div>
            <div class="field-group rule-folder-path-wrap">
              <label>
                Folder Path
                <span class="field-hint">e.g. Engineering/Backend/Services (created if missing)</span>
              </label>
              <input type="text" class="rule-folder-path" placeholder="Parent/Child/Subfolder (optional)">
            </div>
            <div class="field-group rule-parent-id-wrap">
              <label>Parent Page ID <span class="field-hint">(optional — overridden by Folder Path)</span></label>
              <input type="text" class="rule-parent-id" placeholder="123456">
            </div>
            <div class="field-group rule-page-id-wrap" style="display:none">
              <label>Page ID</label>
              <input type="text" class="rule-page-id" placeholder="654321 (required)">
            </div>
          </div>
        </fieldset>

        <!-- LLM Processing -->
        <fieldset class="rule-fieldset full-width llm-fieldset">
          <legend>LLM Processing</legend>
          <div class="llm-toggle-row">
            <label class="toggle-label">
              <input type="checkbox" class="rule-llm-enabled" checked>
              <span class="toggle-text">Enable LLM</span>
            </label>
          </div>
          <div class="llm-tasks-group">
            <label class="task-chip">
              <input type="checkbox" class="rule-task" value="reformat"> reformat
            </label>
            <label class="task-chip">
              <input type="checkbox" class="rule-task" value="summarize"> summarize
            </label>
            <label class="task-chip">
              <input type="checkbox" class="rule-task" value="plantuml_diagram" checked> plantuml_diagram
            </label>
            <label class="task-chip" title="Generate a PlantUML diagram after each Use Case: block in this section">
              <input type="checkbox" class="rule-task" value="usecase_diagrams"> usecase_diagrams
              <span class="task-hint">(per use case)</span>
            </label>
            <label class="task-chip chip-last">
              <input type="checkbox" class="rule-task" value="confluence_format" checked> confluence_format
              <span class="task-hint">(always last)</span>
            </label>
          </div>
        </fieldset>

        <!-- Table Expansion -->
        <fieldset class="rule-fieldset full-width">
          <legend>Table Expansion</legend>
          <label class="task-chip" title="Create one Confluence page per table row (section + subsections); each page shows the row as a transposed header / value table">
            <input type="checkbox" class="rule-expand-tables-to-pages"> Expand tables to pages
            <span class="task-hint">(section + subsections)</span>
          </label>
        </fieldset>

        <!-- Per-section push action -->
        <div class="rule-push-row">
          <button type="button" class="btn btn-push rule-push-btn">
            &#9654; Push to Confluence
          </button>
          <button type="button" class="btn btn-copy-similar rule-copy-btn" title="Copy these settings to all other sections at the same heading level">
            &#10697; Copy to similar
          </button>
          <span class="rule-push-status"></span>
        </div>

        ${isFolderSection ? `
        <!-- Subtree push: NOT dimmed by folder-only (lives outside rule-push-row) -->
        <div class="rule-subtree-push-row">
          <button type="button" class="btn btn-subtree-push rule-subtree-push-btn">
            ${section.level === 1
              ? '&#9193;&nbsp;Push entire module'
              : '&#9193;&nbsp;Push all under screen'}
          </button>
          <span class="rule-subtree-push-status"></span>
        </div>` : ''}

      </div>
    `;

    // ── Folder Only toggle
    const folderOnlyChk   = detail.querySelector('.rule-folder-only');
    const targetFields    = detail.querySelector('.rule-target-fields');
    const pushRow         = detail.querySelector('.rule-push-row');
    const llmFieldset     = detail.querySelector('.rule-fieldset.llm-fieldset');

    folderOnlyChk.addEventListener('change', () => {
      const isFolderOnly = folderOnlyChk.checked;
      targetFields.style.opacity  = isFolderOnly ? '0.45' : '';
      targetFields.style.pointerEvents = isFolderOnly ? 'none' : '';
      if (llmFieldset) {
        llmFieldset.style.opacity       = isFolderOnly ? '0.45' : '';
        llmFieldset.style.pointerEvents = isFolderOnly ? 'none' : '';
      }
      if (pushRow) {
        pushRow.style.opacity       = isFolderOnly ? '0.45' : '';
        pushRow.style.pointerEvents = isFolderOnly ? 'none' : '';
      }
      // Add a visual badge on the summary
      let badge = detail.querySelector('.folder-only-badge');
      if (isFolderOnly && !badge) {
        badge = document.createElement('span');
        badge.className = 'folder-only-badge badge badge-folder-only';
        badge.textContent = 'FOLDER ONLY';
        detail.querySelector('.rule-summary').appendChild(badge);
      } else if (!isFolderOnly && badge) {
        badge.remove();
      }
      // Recompute all folder paths whenever folder-only selection changes
      recomputeFolderPaths();
    });

    // Mark folder-path input as user-typed when the user edits it manually
    const folderPathInput = detail.querySelector('.rule-folder-path');
    if (folderPathInput) {
      folderPathInput.addEventListener('input', () => {
        if (folderPathInput.value.trim()) {
          folderPathInput.dataset.userFolder = '1';
          folderPathInput.dataset.computedFolder = '';
          folderPathInput.classList.remove('folder-path-computed');
          folderPathInput.title = '';
        } else {
          // User cleared the field — allow recomputation
          folderPathInput.dataset.userFolder = '';
          recomputeFolderPaths();
        }
      });
    }

    // ── Action dropdown: show/hide conditional fields
    const actionSel    = detail.querySelector('.rule-action');
    const spaceWrap    = detail.querySelector('.rule-space-key-wrap');
    const folderWrap   = detail.querySelector('.rule-folder-path-wrap');
    const parentWrap   = detail.querySelector('.rule-parent-id-wrap');
    const pageWrap     = detail.querySelector('.rule-page-id-wrap');

    function syncActionFields() {
      const v = actionSel.value;
      const isCreate = v === 'create';
      spaceWrap.style.display  = isCreate ? '' : 'none';
      folderWrap.style.display = isCreate ? '' : 'none';
      parentWrap.style.display = isCreate ? '' : 'none';
      pageWrap.style.display   = isCreate ? 'none' : '';
    }
    actionSel.addEventListener('change', syncActionFields);

    // ── Pre-fill Space Key from global default (if not already set)
    const spaceKeyInput = detail.querySelector('.rule-space-key');
    if (spaceKeyInput && !spaceKeyInput.value) {
      const defaultKey = getDefaultSpaceKey();
      if (defaultKey) spaceKeyInput.value = defaultKey;
    }

    // ── Enable checkbox: toggle disabled appearance
    const enableChk = detail.querySelector('.rule-enable');
    enableChk.addEventListener('change', () => {
      detail.classList.toggle('rule-disabled', !enableChk.checked);
    });

    // ── LLM enabled toggle: dim task chips (scoped to LLM fieldset only)
    const llmChk = detail.querySelector('.rule-llm-enabled');
    llmChk.addEventListener('change', () => {
      const llmFs = detail.querySelector('.llm-fieldset');
      const chips = llmFs ? llmFs.querySelectorAll('.task-chip') : [];
      const tasks = llmFs ? llmFs.querySelectorAll('.rule-task') : [];
      chips.forEach(c => c.classList.toggle('chip-disabled', !llmChk.checked));
      tasks.forEach(t => { t.disabled = !llmChk.checked; });
    });

    // ── Per-section "Push to Confluence" button
    const pushBtn    = detail.querySelector('.rule-push-btn');
    const pushStatus = detail.querySelector('.rule-push-status');
    pushBtn.addEventListener('click', () => pushSection(section, detail, pushBtn, pushStatus));

    // ── "Copy to similar" button
    const copyBtn = detail.querySelector('.rule-copy-btn');
    copyBtn.addEventListener('click', () => copyRuleToSimilar(detail, section));

    // ── Subtree "Push all" button (present only on module/screen rows)
    const subtreePushBtn    = detail.querySelector('.rule-subtree-push-btn');
    const subtreePushStatus = detail.querySelector('.rule-subtree-push-status');
    if (subtreePushBtn) {
      subtreePushBtn.addEventListener('click', () =>
        pushSubtree(section, detail, subtreePushBtn, subtreePushStatus)
      );
    }

    // ── Apply rule defaults (listeners are already wired above) ──────────────
    if (sectionApply) {
      // skip: disable this row (excluded from migration)
      if (isSkipped) {
        const enableChkEl = detail.querySelector('.rule-enable');
        if (enableChkEl) {
          enableChkEl.checked = false;
          detail.classList.add('rule-disabled');
        }
      }

      // page_title override
      const pageTitleInput = detail.querySelector('.rule-page-title');
      if (pageTitleInput && sectionApply.page_title) {
        pageTitleInput.value = sectionApply.page_title;
      }

      // folder_path — mark as rule-set so recomputeFolderPaths skips it
      const folderPathInput = detail.querySelector('.rule-folder-path');
      if (folderPathInput && sectionApply.folder_path !== undefined) {
        folderPathInput.value               = sectionApply.folder_path;
        folderPathInput.dataset.userFolder  = '1';  // prevent auto-overwrite
        folderPathInput.dataset.computedFolder = '';
        folderPathInput.classList.remove('folder-path-computed');
        folderPathInput.title = 'Set by section defaulting rule';
      }

      // folder_only
      if (isFolderSection) {
        folderOnlyChk.checked = true;
        folderOnlyChk.dispatchEvent(new Event('change'));
      }

      // llm_enabled
      if (sectionApply.llm_enabled === false) {
        const llmChkEl = detail.querySelector('.rule-llm-enabled');
        if (llmChkEl) {
          llmChkEl.checked = false;
          llmChkEl.dispatchEvent(new Event('change'));
        }
      }

      // llm_tasks (only if not folder_only and list is explicitly provided)
      if (!isFolderSection && Array.isArray(sectionApply.llm_tasks)) {
        const taskSet = new Set(sectionApply.llm_tasks);
        detail.querySelectorAll('.rule-task').forEach(chk => {
          chk.checked = taskSet.has(chk.value);
        });
      }

      // table_rows_to_pages — store as data attributes for buildSectionMapping
      if (sectionApply.table_rows_to_pages) {
        detail.dataset.tableRowsToPages = '1';
        if (sectionApply.row_page_title) {
          detail.dataset.rowPageTitle = sectionApply.row_page_title;
        }
      }

      // expand_tables_to_pages — check the chip checkbox if rule sets it
      if (sectionApply.expand_tables_to_pages) {
        const expandChk = detail.querySelector('.rule-expand-tables-to-pages');
        if (expandChk) expandChk.checked = true;
      }
    }

    // Default expand_tables_to_pages to true for "Page Elements" sections and subsections
    // (applies when no rule has already set it)
    if (underPageElements && !sectionApply?.expand_tables_to_pages) {
      const expandChk = detail.querySelector('.rule-expand-tables-to-pages');
      if (expandChk) expandChk.checked = true;
    }

    container.appendChild(detail);
  });
}

// ── Folder Path Auto-computation ───────────────────────────────────────────────

/**
 * Walk all enabled rule rows in DOM order, simulate the level-aware folder
 * stack (same logic as orchestrator.py), and update every row's Folder Path
 * input to show the computed inherited path.
 *
 * Rules:
 *  - Folder-only rows: show the FULL stack path they contribute to
 *    (e.g. "Module A/Business Process") as their folder path.
 *  - Non-folder-only rows: show the inherited folder path that will be used
 *    when their page is created.
 *  - If a row has a MANUALLY typed folder path (data-user-folder="1"), that
 *    value is left untouched and used as-is (not overwritten).
 */
function recomputeFolderPaths() {
  const allRows = [...$a('.rule-row')];   // rule-row is the <details> element
  const stack = [];   // [{level, name}]

  allRows.forEach(row => {
    const enableChk    = row.querySelector('.rule-enable');
    if (enableChk && !enableChk.checked) return;

    const folderOnlyChk = row.querySelector('.rule-folder-only');
    const isFolderOnly  = folderOnlyChk?.checked || false;
    const folderInput   = row.querySelector('.rule-folder-path');
    const sectionId     = row.dataset.sectionId;
    const sec           = APP.sections.find(s => s.id === sectionId);
    const level         = sec?.level || 1;

    if (isFolderOnly) {
      // Determine the folder name for this row:
      // prefer manual input, else fall back to section title
      const userTyped = (folderInput?.dataset.userFolder === '1' && folderInput.value.trim())
        ? folderInput.value.trim()
        : null;
      const folderName = userTyped || sec?.title || '';

      // Pop entries strictly deeper than current level
      while (stack.length && stack[stack.length - 1].level > level) stack.pop();
      stack.push({ level, name: folderName });

      const fullPath = stack.map(e => e.name).join('/');

      // Show the full path the folder-only row creates
      if (folderInput && folderInput.dataset.userFolder !== '1') {
        folderInput.value = fullPath;
        folderInput.dataset.computedFolder = '1';
        folderInput.classList.add('folder-path-computed');
        folderInput.title = 'Auto-computed from heading hierarchy';
      }

    } else {
      // Content row: show the inherited folder path (what the page will be created under)
      const inheritedPath = stack.map(e => e.name).join('/');

      if (folderInput && folderInput.dataset.userFolder !== '1') {
        if (inheritedPath) {
          folderInput.value = inheritedPath;
          folderInput.dataset.computedFolder = '1';
          folderInput.classList.add('folder-path-computed');
          folderInput.title = 'Inherited from folder-only sections above';
        } else {
          // No folder context — clear computed value
          if (folderInput.dataset.computedFolder === '1') {
            folderInput.value = '';
            folderInput.dataset.computedFolder = '';
            folderInput.classList.remove('folder-path-computed');
            folderInput.title = '';
          }
        }
      }
    }
  });
}

// ── Config Collection ──────────────────────────────────────────────────────────

const TASK_ORDER = ['reformat', 'summarize', 'plantuml_diagram', 'usecase_diagrams', 'confluence_format'];

/** Read the centralized Confluence credentials from the global settings panel. */
function getGlobalCredentials() {
  return {
    base_url:  ($('global-base-url')?.value  || '').trim(),
    user:      ($('global-user')?.value      || '').trim(),
    api_token: ($('global-api-token')?.value || '').trim(),
  };
}

/** Return the current default space key from the global settings panel. */
function getDefaultSpaceKey() {
  return ($('global-space-key')?.value || '').trim();
}

/**
 * Fetch defaults.yaml values from the server and pre-fill all global
 * settings inputs.  Called once on DOMContentLoaded.
 */
async function loadDefaults() {
  try {
    const resp = await fetch('/defaults');
    if (!resp.ok) return;
    const d = await resp.json();

    if (d.confluence_base_url)  $('global-base-url').value   = d.confluence_base_url;
    if (d.confluence_user)      $('global-user').value        = d.confluence_user;
    // api_token: only pre-fill with a non-empty value that isn't an unresolved ${...}
    if (d.confluence_api_token && !d.confluence_api_token.startsWith('${')) {
      $('global-api-token').value = d.confluence_api_token;
    }
    if (d.default_space_key)       $('global-space-key').value  = d.default_space_key;
    if (d.llm_model)               $('llm-model').value         = d.llm_model;
    if (d.llm_temperature != null) $('llm-temperature').value   = d.llm_temperature;
    if (d.plantuml_theme)          $('plantuml-theme').value    = d.plantuml_theme;

    // Backfill space key into any rule rows already rendered (timing race:
    // user can navigate to Step 2 before this async fetch completes)
    if (d.default_space_key) {
      $a('.rule-space-key').forEach(input => {
        if (!input.value.trim()) input.value = d.default_space_key;
      });
    }

    // Show badge if any value was loaded
    const hasAny = d.confluence_base_url || d.confluence_user ||
                   d.default_space_key   || d.llm_model;
    if (hasAny) {
      $('defaults-source-badge')?.classList.remove('hidden');
    }
  } catch (_) {
    // Silently ignore — defaults are optional
  }
}

/**
 * Save the current global settings back to defaults.yaml via POST /defaults.
 */
async function saveDefaults() {
  const btn    = $('save-defaults-btn');
  const status = $('save-defaults-status');

  btn.disabled = true;
  btn.innerHTML = '<span class="spinner"></span> Saving…';
  status.className = 'save-defaults-status';
  status.textContent = '';

  try {
    const resp = await fetch('/defaults', {
      method:  'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        confluence_base_url:  ($('global-base-url')?.value   || '').trim(),
        confluence_user:      ($('global-user')?.value       || '').trim(),
        confluence_api_token: ($('global-api-token')?.value  || '').trim(),
        default_space_key:    ($('global-space-key')?.value  || '').trim(),
        llm_model:            ($('llm-model')?.value         || '').trim() || 'gpt-oss:20b',
        llm_temperature:      parseFloat($('llm-temperature')?.value) || 0.1,
        plantuml_theme:       ($('plantuml-theme')?.value    || '').trim() || 'cerulean',
      }),
    });

    if (resp.ok) {
      status.className = 'save-defaults-status save-defaults-ok';
      status.textContent = '✓ Saved to defaults.yaml';
      $('defaults-source-badge')?.classList.remove('hidden');
      setTimeout(() => { status.textContent = ''; }, 4000);
    } else {
      const err = await resp.json().catch(() => ({}));
      status.className = 'save-defaults-status save-defaults-fail';
      status.textContent = `✕ ${err.error || 'Save failed'}`;
    }
  } catch (err) {
    status.className = 'save-defaults-status save-defaults-fail';
    status.textContent = `✕ ${err.message}`;
  } finally {
    btn.disabled = false;
    btn.innerHTML = '&#128190; Save as Defaults';
  }
}

// ── Section Default Rules (section_defaults.yaml) ─────────────────────────────

/**
 * Load section_defaults.yaml rules from the server.
 * Stores parsed rules in APP.sectionDefaultRules and populates the YAML editor.
 */
async function loadSectionDefaultRules() {
  try {
    const resp = await fetch('/section-defaults');
    if (!resp.ok) return;
    const d = await resp.json();
    // Store full config (context_vars + rules) and convenience alias
    APP.sectionDefaultsConfig = d.config || null;
    APP.sectionDefaultRules   = d.rules  || [];
    const editor = $('section-rules-editor');
    if (editor) editor.value = d.raw_yaml || '';
    _updateSectionRulesBadge();
  } catch (_) {
    // Silently ignore — rules are optional
  }
}

function _updateSectionRulesBadge() {
  const badge = $('section-rules-badge');
  if (!badge) return;
  const count = (APP.sectionDefaultRules || []).length;
  badge.textContent = count ? `${count} rule${count !== 1 ? 's' : ''}` : '';
}

/**
 * Save the textarea content to section_defaults.yaml, reload rules,
 * and re-render current rule rows if step 2 is active.
 */
async function saveSectionDefaultRules() {
  const btn    = $('section-rules-save-btn');
  const status = $('section-rules-status');
  const editor = $('section-rules-editor');
  if (!editor) return;

  btn.disabled = true;
  status.className = 'section-rules-status';
  status.textContent = '…';

  try {
    const resp = await fetch('/section-defaults', {
      method:  'POST',
      headers: { 'Content-Type': 'application/json' },
      body:    JSON.stringify({ raw_yaml: editor.value }),
    });
    const d = await resp.json();
    if (resp.ok) {
      APP.sectionDefaultsConfig = d.config || null;
      APP.sectionDefaultRules   = d.rules  || [];
      _updateSectionRulesBadge();
      status.className = 'section-rules-status section-rules-ok';
      const msg = `✓ Saved — ${d.rules?.length ?? 0} rule${d.rules?.length !== 1 ? 's' : ''} active`;
      status.textContent = msg;
      // Re-render rule rows so the new defaults take effect immediately
      if (APP.sections.length && APP.currentStep === 2) {
        renderRules(APP.sections);
        recomputeFolderPaths();
      }
      // Auto-close modal after a short success delay
      setTimeout(() => {
        $('section-rules-modal')?.close();
        status.textContent = '';
        status.className = 'section-rules-status';
      }, 1500);
    } else {
      status.className = 'section-rules-status section-rules-fail';
      status.textContent = `✕ ${d.error || 'Save failed'}`;
    }
  } catch (err) {
    status.className = 'section-rules-status section-rules-fail';
    status.textContent = `✕ ${err.message}`;
  } finally {
    btn.disabled = false;
  }
}

function initSectionRulesEditor() {
  const modal      = $('section-rules-modal');
  const openBtn    = $('section-rules-open-btn');
  const closeBtn   = $('section-rules-close-btn');
  const cancelBtn  = $('section-rules-cancel-btn');

  // Open modal
  openBtn?.addEventListener('click', () => {
    modal?.showModal();
    // Focus the editor so user can start typing immediately
    setTimeout(() => $('section-rules-editor')?.focus(), 50);
  });

  // Close via × button
  closeBtn?.addEventListener('click', () => modal?.close());

  // Close via Cancel button
  cancelBtn?.addEventListener('click', () => modal?.close());

  // Close by clicking the backdrop (outside the dialog box)
  modal?.addEventListener('click', (e) => {
    if (e.target === modal) modal.close();
  });

  // Tab key indents with 2 spaces in the YAML editor
  $('section-rules-editor')?.addEventListener('keydown', (e) => {
    if (e.key === 'Tab') {
      e.preventDefault();
      const el = e.target;
      const start = el.selectionStart;
      const end   = el.selectionEnd;
      el.value = el.value.slice(0, start) + '  ' + el.value.slice(end);
      el.selectionStart = el.selectionEnd = start + 2;
    }
  });

  // Save
  $('section-rules-save-btn')?.addEventListener('click', saveSectionDefaultRules);
}

/**
 * Build a section mapping object from a rule row element + section data.
 * Returns null (and alerts) if validation fails.
 */
function buildSectionMapping(row, section) {
  const folderOnly = row.querySelector('.rule-folder-only')?.checked || false;
  const action     = row.querySelector('.rule-action').value;
  // Fall back to the global default space key if the row field is empty
  const rowSpaceKey = (row.querySelector('.rule-space-key')?.value || '').trim();
  const spaceKey    = rowSpaceKey || getDefaultSpaceKey();
  const parentId   = (row.querySelector('.rule-parent-id')?.value    || '').trim() || null;
  const pageId     = (row.querySelector('.rule-page-id')?.value      || '').trim() || null;

  // For folder-only rows: the orchestrator builds the full path from the stack,
  // so we only send the leaf name (last path segment).
  // For content rows: send the full inherited path so the page is created in
  // the right place even when pushSection is called in isolation.
  const rawFolderPath = (row.querySelector('.rule-folder-path')?.value || '').trim();
  let folderPath = rawFolderPath || null;
  if (folderOnly && rawFolderPath) {
    // Strip to leaf name only — orchestrator handles the rest via the stack
    const parts = rawFolderPath.split('/');
    folderPath = parts[parts.length - 1].trim() || rawFolderPath;
  }

  // folder_only sections skip page creation — no field validation required
  if (!folderOnly) {
    if (action === 'create' && !spaceKey) {
      alert(`Section "${section.title}":\nSpace Key is required for action = create.\nSet a Default Space Key in the global settings, or fill it in this rule.`);
      row.setAttribute('open', '');
      return null;
    }
    if ((action === 'update' || action === 'append') && !pageId) {
      alert(`Section "${section.title}":\nPage ID is required for action = ${action}.`);
      row.setAttribute('open', '');
      return null;
    }
  }

  const llmEnabled         = row.querySelector('.rule-llm-enabled').checked;
  const checkedVals        = new Set([...row.querySelectorAll('.rule-task:checked')].map(c => c.value));
  const tasks              = TASK_ORDER.filter(t => checkedVals.has(t));
  const matchType          = section.content_type === 'table' ? 'table' : 'heading';
  const expandTablesToPages = row.querySelector('.rule-expand-tables-to-pages')?.checked || false;

  const pageTitleOverride = (row.querySelector('.rule-page-title')?.value || '').trim() || null;
  const tableRowsToPages  = row.dataset.tableRowsToPages === '1';
  const rowPageTitle      = (row.dataset.rowPageTitle || '').trim() || null;

  return {
    match:      section.title,
    match_type: matchType,
    level:      section.level || 0,
    // section_id ties this mapping to the exact ParsedSection by its stable ID,
    // preventing multiple sections with the same heading text (e.g. many
    // "Screen Mockup" sections) from all resolving to the first match.
    section_id: section.id || null,
    confluence: {
      space_key:           spaceKey,
      folder_path:         folderPath,
      parent_page_id:      parentId,
      page_id:             pageId,
      action:              action,
      folder_only:         folderOnly,
      page_title:          pageTitleOverride,   // null = use section heading
      table_rows_to_pages: tableRowsToPages,
      row_page_title:      rowPageTitle,
    },
    llm: { enabled: llmEnabled, tasks, expand_tables_to_pages: expandTablesToPages },
  };
}

function collectConfig(enabledOnly = true) {
  const llmModel      = ($('llm-model').value      || '').trim() || 'gpt-oss:20b';
  const llmTemp       = parseFloat($('llm-temperature').value);
  const plantumlTheme = ($('plantuml-theme').value || '').trim() || 'cerulean';
  const creds         = getGlobalCredentials();

  if (!creds.base_url) {
    alert('Please fill in the Confluence Base URL in the global settings.');
    return null;
  }

  const rows     = $a('.rule-row');
  const sections = [];

  for (const row of rows) {
    const enabledChk = row.querySelector('.rule-enable');
    if (enabledOnly && (!enabledChk || !enabledChk.checked)) continue;

    const sectionId = row.dataset.sectionId;
    const section   = APP.sections.find(s => s.id === sectionId);
    if (!section) continue;

    const mapping = buildSectionMapping(row, section);
    if (!mapping) return null;
    sections.push(mapping);
  }

  if (sections.length === 0) {
    alert('No sections are enabled.\nEnable at least one section rule to continue.');
    return null;
  }

  return {
    llm_model:            llmModel,
    llm_temperature:      isNaN(llmTemp) ? 0.1 : llmTemp,
    plantuml_theme:       plantumlTheme,
    confluence_base_url:  creds.base_url,
    confluence_user:      creds.user,
    confluence_api_token: creds.api_token,
    db_logging:           true,
    db_path:              'doc_to_confluence/migration.db',
    sections,
  };
}

/**
 * Scan all rule rows that appear before `currentRow` in the DOM and build
 * an ordered list of preceding section mappings that are:
 *   - enabled (rule-enable checked)
 *   - marked as folder_only
 *
 * Returns an array of folder-only mapping objects, in DOM order.
 * These are prepended to the single-section config sent to /migrate so that
 * the StreamingOrchestrator can propagate the inherited_folder context
 * correctly before it processes the target section.
 */
function getPrecedingFolderOnlyMappings(currentRow) {
  const allRows    = $a('.rule-row');
  const currentIdx = allRows.indexOf(currentRow);
  const result     = [];

  for (let i = 0; i < currentIdx; i++) {
    const row = allRows[i];

    // Must be enabled
    const enableChk = row.querySelector('.rule-enable');
    if (!enableChk || !enableChk.checked) continue;

    // Must be folder_only
    const folderOnlyChk = row.querySelector('.rule-folder-only');
    if (!folderOnlyChk || !folderOnlyChk.checked) continue;

    const sectionId = row.dataset.sectionId;
    const sec       = APP.sections.find(s => s.id === sectionId);
    if (!sec) continue;

    const folderPath = (row.querySelector('.rule-folder-path')?.value || '').trim() || null;
    const spaceKey   = (row.querySelector('.rule-space-key')?.value   || '').trim();

    result.push({
      match:      sec.title,
      match_type: sec.content_type === 'table' ? 'table' : 'heading',
      level:      sec.level || 0,
      section_id: sec.id || null,
      confluence: {
        space_key:      spaceKey,
        folder_path:    folderPath,
        parent_page_id: null,
        page_id:        null,
        action:         'create',   // irrelevant — folder_only skips page creation
        folder_only:    true,
      },
      llm: { enabled: false, tasks: [] },
    });
  }

  return result;
}

/**
 * Push a single section to Confluence inline, without leaving Step 2.
 * Shows a spinner → success/fail status directly on the rule row.
 *
 * If any preceding rows are marked "Folder Only", they are prepended to the
 * sections list so the orchestrator computes the correct inherited_folder
 * context before processing the target section.
 */
async function pushSection(section, row, pushBtn, pushStatus) {
  const creds = getGlobalCredentials();
  if (!creds.base_url) {
    alert('Please fill in the Confluence Base URL in the global settings at the top.');
    return;
  }

  const mapping = buildSectionMapping(row, section);
  if (!mapping) return;

  const dryRun = false; // Dry Run removed — always push live

  const llmModel      = ($('llm-model').value      || '').trim() || 'gpt-oss:20b';
  const llmTemp       = parseFloat($('llm-temperature').value);
  const plantumlTheme = ($('plantuml-theme').value || '').trim() || 'cerulean';

  // Collect any preceding folder_only rows so the orchestrator can propagate
  // inherited_folder context before it processes this section.
  const precedingFolderMappings = getPrecedingFolderOnlyMappings(row);

  const config = {
    llm_model:            llmModel,
    llm_temperature:      isNaN(llmTemp) ? 0.1 : llmTemp,
    plantuml_theme:       plantumlTheme,
    confluence_base_url:  creds.base_url,
    confluence_user:      creds.user,
    confluence_api_token: creds.api_token,
    db_logging:           true,
    db_path:              'doc_to_confluence/migration.db',
    // Prepend folder-only context rows, then the actual target mapping
    sections:             [...precedingFolderMappings, mapping],
  };

  // ── UI: loading state
  pushBtn.disabled = true;
  pushBtn.innerHTML = '<span class="spinner"></span>';
  pushStatus.className = 'rule-push-status push-status-loading';
  pushStatus.textContent = dryRun ? 'Dry run…' : 'Pushing…';

  try {
    const resp = await fetch('/migrate', {
      method:  'POST',
      headers: { 'Content-Type': 'application/json' },
      body:    JSON.stringify({ file_id: APP.fileId, dry_run: dryRun, config }),
    });

    if (!resp.ok) {
      const err = await resp.json().catch(() => ({}));
      throw new Error(err.error || `Server error ${resp.status}`);
    }

    // Drain SSE stream to get the result.
    // There may be multiple section_result events if preceding folder-only rows
    // were prepended — we want the LAST one (the actual target section).
    const reader  = resp.body.getReader();
    const decoder = new TextDecoder();
    let buffer    = '';
    let result    = null;

    while (true) {
      const { value, done } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, { stream: true });
      const parts = buffer.split('\n\n');
      for (let i = 0; i < parts.length - 1; i++) {
        const raw = parts[i].trim();
        if (!raw.startsWith('data:')) continue;
        try {
          const event = JSON.parse(raw.slice(5).trim());
          // Always update — keeps the last section_result (our target section)
          if (event.type === 'section_result') result = event.result;
          // complete: use the last result in the report (our target section)
          if (event.type === 'complete' && event.report?.results?.length) {
            result = event.report.results[event.report.results.length - 1];
          }
          if (event.type === 'error') throw new Error(event.message || 'Unknown error');
        } catch (e) {
          if (e.message && !e.message.startsWith('Unexpected')) throw e;
        }
      }
      buffer = parts[parts.length - 1];
    }
    reader.releaseLock();

    // ── UI: show result
    if (result?.status === 'success') {
      pushStatus.className = 'rule-push-status push-status-ok';
      const label = dryRun ? '✓ Dry run OK' : '✓ Pushed';
      const link  = result.confluence_page_url
        ? ` <a href="${esc(result.confluence_page_url)}" target="_blank" rel="noopener">View →</a>`
        : '';
      pushStatus.innerHTML = label + link;
    } else if (result?.status === 'skipped') {
      pushStatus.className = 'rule-push-status push-status-skip';
      pushStatus.textContent = `⚠ Skipped — no section matched "${section.title}"`;
    } else {
      pushStatus.className = 'rule-push-status push-status-fail';
      pushStatus.textContent = `✕ Failed: ${result?.error || 'unknown error'}`;
    }

  } catch (err) {
    pushStatus.className = 'rule-push-status push-status-fail';
    pushStatus.textContent = `✕ ${err.message}`;
  } finally {
    pushBtn.disabled = false;
    pushBtn.innerHTML = '&#9654; Push to Confluence';
  }
}

/**
 * Copy all configurable settings from `sourceRow` to every other rule row
 * whose section is at the same heading level as `sourceSection`.
 *
 * Settings copied:
 *   - Folder Only toggle
 *   - Action (create / update / append)
 *   - Space Key
 *   - Parent Page ID / Page ID
 *   - LLM Enabled toggle
 *   - LLM task checkboxes (reformat, summarize, plantuml_diagram, usecase_diagrams, confluence_format)
 *
 * Settings intentionally NOT copied (section-specific):
 *   - Folder Path  (each section has its own path in the hierarchy)
 *   - Enable/disable checkbox  (user may want different sections enabled)
 *
 * After copying, all updated rows flash briefly to give visual feedback and
 * `recomputeFolderPaths()` is called to refresh the computed folder paths.
 */
function copyRuleToSimilar(sourceRow, sourceSection) {
  const targetLevel = sourceSection.level;

  // ── Read source values ────────────────────────────────────────────────────
  const srcFolderOnly  = sourceRow.querySelector('.rule-folder-only')?.checked    || false;
  const srcAction      = sourceRow.querySelector('.rule-action')?.value            || 'create';
  const srcSpaceKey    = (sourceRow.querySelector('.rule-space-key')?.value        || '').trim();
  const srcParentId    = (sourceRow.querySelector('.rule-parent-id')?.value        || '').trim();
  const srcPageId      = (sourceRow.querySelector('.rule-page-id')?.value          || '').trim();
  const srcLlmEnabled        = sourceRow.querySelector('.rule-llm-enabled')?.checked              || false;
  const srcExpandTablesToPages = sourceRow.querySelector('.rule-expand-tables-to-pages')?.checked || false;
  const srcTaskValues  = new Set(
    [...sourceRow.querySelectorAll('.rule-task:checked')].map(c => c.value)
  );

  // ── Iterate all rule rows ─────────────────────────────────────────────────
  let updatedCount = 0;

  $a('.rule-row').forEach(row => {
    if (row === sourceRow) return;   // skip self

    const sectionId = row.dataset.sectionId;
    const sec = APP.sections.find(s => s.id === sectionId);
    if (!sec || sec.level !== targetLevel) return;  // different level — skip

    // ── Apply: Folder Only ──────────────────────────────────────────────────
    const folderOnlyChk = row.querySelector('.rule-folder-only');
    if (folderOnlyChk && folderOnlyChk.checked !== srcFolderOnly) {
      folderOnlyChk.checked = srcFolderOnly;
      folderOnlyChk.dispatchEvent(new Event('change'));   // trigger UI dim logic
    }

    // ── Apply: Action ───────────────────────────────────────────────────────
    const actionSel = row.querySelector('.rule-action');
    if (actionSel) {
      actionSel.value = srcAction;
      actionSel.dispatchEvent(new Event('change'));       // trigger show/hide fields
    }

    // ── Apply: Space Key ────────────────────────────────────────────────────
    const spaceKeyInp = row.querySelector('.rule-space-key');
    if (spaceKeyInp) spaceKeyInp.value = srcSpaceKey;

    // ── Apply: Parent Page ID / Page ID ────────────────────────────────────
    const parentIdInp = row.querySelector('.rule-parent-id');
    if (parentIdInp) parentIdInp.value = srcParentId;

    const pageIdInp = row.querySelector('.rule-page-id');
    if (pageIdInp) pageIdInp.value = srcPageId;

    // ── Apply: LLM Enabled ──────────────────────────────────────────────────
    const llmChk = row.querySelector('.rule-llm-enabled');
    if (llmChk && llmChk.checked !== srcLlmEnabled) {
      llmChk.checked = srcLlmEnabled;
      llmChk.dispatchEvent(new Event('change'));          // trigger chip dim logic
    }

    // ── Apply: LLM Tasks ────────────────────────────────────────────────────
    row.querySelectorAll('.rule-task').forEach(chk => {
      chk.checked = srcTaskValues.has(chk.value);
    });

    // ── Apply: Expand Tables to Pages ───────────────────────────────────────
    const expandChk = row.querySelector('.rule-expand-tables-to-pages');
    if (expandChk) expandChk.checked = srcExpandTablesToPages;

    // ── Flash feedback ──────────────────────────────────────────────────────
    row.classList.add('rule-row-flash');
    setTimeout(() => row.classList.remove('rule-row-flash'), 900);

    updatedCount++;
  });

  // Refresh folder paths after potential folder-only changes
  recomputeFolderPaths();

  // ── Toast feedback on the source row's push-status span ──────────────────
  const pushStatus = sourceRow.querySelector('.rule-push-status');
  if (pushStatus) {
    const prev = pushStatus.innerHTML;
    const prevClass = pushStatus.className;
    pushStatus.className = 'rule-push-status push-status-ok';
    pushStatus.textContent = updatedCount > 0
      ? `✓ Copied to ${updatedCount} similar section${updatedCount !== 1 ? 's' : ''} (level ${targetLevel})`
      : `⚠ No other sections at this level`;
    setTimeout(() => {
      pushStatus.className = prevClass;
      pushStatus.innerHTML = prev;
    }, 3500);
  }
}

/**
 * Find the first rule row for `level`, then apply its settings to all other
 * rows at the same level.  Called from the bulk-apply toolbar.
 */
function bulkApplyLevel(level, triggerBtn) {
  // Find the first row at this level
  const allRows = $a('.rule-row');
  let firstRow = null;
  for (const row of allRows) {
    const sec = APP.sections.find(s => s.id === row.dataset.sectionId);
    if (sec && sec.level === level) { firstRow = row; break; }
  }

  if (!firstRow) return;

  const firstSec = APP.sections.find(s => s.id === firstRow.dataset.sectionId);
  if (!firstSec) return;

  // Delegate to the per-row copy function using the first row as source
  copyRuleToSimilar(firstRow, firstSec);

  // Brief button flash to confirm action
  triggerBtn.classList.add('bulk-btn-flash');
  setTimeout(() => triggerBtn.classList.remove('bulk-btn-flash'), 800);
}

/**
 * Return all rule rows that form the subtree rooted at `rootRow`:
 *   - rootRow itself
 *   - every subsequent row whose section level is STRICTLY DEEPER than rootRow's level,
 *     stopping as soon as a row at the same or shallower level is encountered.
 *
 * Disabled rows are included so the orchestrator can handle folder_only context
 * correctly; the orchestrator will skip them if they are not enabled.
 */
function getSubtreeRows(rootRow) {
  const allRows   = $a('.rule-row');
  const rootIdx   = allRows.indexOf(rootRow);
  if (rootIdx === -1) return [];

  const rootSec   = APP.sections.find(s => s.id === rootRow.dataset.sectionId);
  const rootLevel = rootSec?.level ?? 1;
  const result    = [];

  for (let i = rootIdx; i < allRows.length; i++) {
    const row = allRows[i];
    const sec = APP.sections.find(s => s.id === row.dataset.sectionId);
    if (!sec) continue;
    // Stop when we hit a row at the same or shallower level (after the root itself)
    if (i > rootIdx && sec.level <= rootLevel) break;
    result.push(row);
  }
  return result;
}

/**
 * Push the entire subtree rooted at `rootRow` to Confluence in a single
 * /migrate call.  Shows live progress in `statusEl`.
 *
 * Includes:
 *   - All ancestor folder-only rows (for correct folder stack context)
 *   - rootRow itself (the screen/module folder)
 *   - All child rows in document order (sub-folders and content pages)
 *
 * Disabled rows inside the subtree are skipped; ancestor context rows are
 * always included regardless of their enable checkbox.
 */
async function pushSubtree(rootSection, rootRow, btn, statusEl) {
  const creds = getGlobalCredentials();
  if (!creds.base_url) {
    alert('Please fill in the Confluence Base URL in the global settings.');
    return;
  }

  // Collect subtree rows (root + all descendants)
  const subtreeRows = getSubtreeRows(rootRow);

  // Build mappings; skip disabled rows but include folder-only ones
  const subtreeMappings = [];
  for (const row of subtreeRows) {
    const sec = APP.sections.find(s => s.id === row.dataset.sectionId);
    if (!sec) continue;
    const enableChk = row.querySelector('.rule-enable');
    if (enableChk && !enableChk.checked) continue;
    const mapping = buildSectionMapping(row, sec);
    if (!mapping) return;   // validation failed — stop
    subtreeMappings.push(mapping);
  }

  if (subtreeMappings.length === 0) {
    statusEl.className = 'rule-subtree-push-status push-status-skip';
    statusEl.textContent = '⚠ No enabled sections found in subtree';
    return;
  }

  // Prepend ancestor folder-only rows so orchestrator has correct context
  const precedingFolders = getPrecedingFolderOnlyMappings(rootRow);
  const allMappings = [...precedingFolders, ...subtreeMappings];

  const llmModel      = ($('llm-model').value      || '').trim() || 'gpt-oss:20b';
  const llmTemp       = parseFloat($('llm-temperature').value);
  const plantumlTheme = ($('plantuml-theme').value || '').trim() || 'cerulean';

  const config = {
    llm_model:            llmModel,
    llm_temperature:      isNaN(llmTemp) ? 0.1 : llmTemp,
    plantuml_theme:       plantumlTheme,
    confluence_base_url:  creds.base_url,
    confluence_user:      creds.user,
    confluence_api_token: creds.api_token,
    db_logging:           true,
    db_path:              'doc_to_confluence/migration.db',
    sections:             allMappings,
  };

  // ── UI: loading state ──────────────────────────────────────────────────────
  const isModule      = rootSection.level === 1;
  const originalLabel = btn.innerHTML;
  btn.disabled        = true;
  btn.innerHTML       = '<span class="spinner"></span>';
  statusEl.className  = 'rule-subtree-push-status push-status-loading';
  statusEl.textContent = `Pushing ${subtreeMappings.length} section${subtreeMappings.length !== 1 ? 's' : ''}…`;

  try {
    const resp = await fetch('/migrate', {
      method:  'POST',
      headers: { 'Content-Type': 'application/json' },
      body:    JSON.stringify({ file_id: APP.fileId, dry_run: false, config }),
    });

    if (!resp.ok) {
      const err = await resp.json().catch(() => ({}));
      throw new Error(err.error || `Server error ${resp.status}`);
    }

    // ── Stream SSE events ──────────────────────────────────────────────────
    const reader  = resp.body.getReader();
    const decoder = new TextDecoder();
    let buffer    = '';
    let doneCount = 0;
    let failCount = 0;
    const targetCount = subtreeMappings.length;

    while (true) {
      const { value, done } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, { stream: true });
      const parts = buffer.split('\n\n');
      for (let i = 0; i < parts.length - 1; i++) {
        const raw = parts[i].trim();
        if (!raw.startsWith('data:')) continue;
        try {
          const event = JSON.parse(raw.slice(5).trim());
          if (event.type === 'section_result') {
            // Count only the subtree results (not the prepended folder context)
            const resultIdx = (event.index ?? 0) - precedingFolders.length;
            if (resultIdx >= 0) {
              doneCount++;
              if (event.result?.status === 'failed') failCount++;
              statusEl.textContent =
                `Pushing… ${doneCount}/${targetCount} — ${event.result?.section_title || ''}`;
            }
          }
          if (event.type === 'complete') {
            const results = (event.report?.results || []).slice(precedingFolders.length);
            doneCount  = results.filter(r => r.status !== 'skipped').length;
            failCount  = results.filter(r => r.status === 'failed').length;
          }
          if (event.type === 'error') throw new Error(event.message || 'Unknown error');
        } catch (e) {
          if (e.message && !e.message.startsWith('Unexpected')) throw e;
        }
      }
      buffer = parts[parts.length - 1];
    }
    reader.releaseLock();

    // ── Final status ──────────────────────────────────────────────────────
    const successCount = doneCount - failCount;
    if (failCount === 0) {
      statusEl.className = 'rule-subtree-push-status push-status-ok';
      statusEl.textContent =
        `✓ ${successCount} section${successCount !== 1 ? 's' : ''} pushed`;
    } else {
      statusEl.className = 'rule-subtree-push-status push-status-fail';
      statusEl.textContent = `${successCount} pushed · ${failCount} failed`;
    }

  } catch (err) {
    statusEl.className = 'rule-subtree-push-status push-status-fail';
    statusEl.textContent = `✕ ${err.message}`;
  } finally {
    btn.disabled  = false;
    btn.innerHTML = originalLabel;
  }
}

async function downloadConfigYaml(config) {
  const btn = $('download-config-btn');
  btn.disabled = true;
  btn.innerHTML = '<span class="spinner"></span> Generating…';

  try {
    const resp = await fetch('/build-config', {
      method:  'POST',
      headers: { 'Content-Type': 'application/json' },
      body:    JSON.stringify({ config }),
    });

    if (!resp.ok) {
      const err = await resp.json().catch(() => ({}));
      alert(`Config validation failed:\n${err.error || resp.statusText}`);
      return;
    }

    const blob = await resp.blob();
    const url  = URL.createObjectURL(blob);
    const a    = Object.assign(document.createElement('a'), { href: url, download: 'migration_config.yaml' });
    a.click();
    URL.revokeObjectURL(url);
  } catch (err) {
    alert(`Download failed: ${err.message}`);
  } finally {
    btn.disabled = false;
    btn.innerHTML = '&#8615; Download Config YAML';
  }
}

// ═══════════════════════════════════════════════════════════════════════════════
// STEP 3 — Run & Results
// ═══════════════════════════════════════════════════════════════════════════════

function initStep3() {
  $('step3-back').addEventListener('click',     () => showStep(2));
  $('step3-back-top').addEventListener('click', () => showStep(2));

  // Dry-run / overwrite / pre-delete coupling:
  // Both overwrite and pre-delete are disabled while dry-run is active
  // (dry-run skips all writes, so deletions would be meaningless and dangerous).
  $('dry-run-toggle').addEventListener('change', () => {
    const isDryRun = $('dry-run-toggle').checked;
    if (isDryRun) {
      $('overwrite-toggle').checked   = false;
      $('overwrite-toggle').disabled  = true;
      $('pre-delete-toggle').checked  = false;
      $('pre-delete-toggle').disabled = true;
    } else {
      $('overwrite-toggle').disabled  = false;
      $('pre-delete-toggle').disabled = false;
    }
  });

  $('run-migration-btn').addEventListener('click', () => {
    const config = APP.pendingConfig;
    if (!config) {
      alert('No configuration found. Go back and configure the rules first.');
      return;
    }
    runMigration(
      config,
      $('dry-run-toggle').checked,
      $('overwrite-toggle').checked,
      $('pre-delete-toggle').checked,
    );
  });

  // Sync toggle disabled states on initial page load
  // (dry-run starts checked, so overwrite and pre-delete must start disabled)
  (function syncTogglesOnLoad() {
    const isDryRun = $('dry-run-toggle').checked;
    $('overwrite-toggle').disabled  = isDryRun;
    $('pre-delete-toggle').disabled = isDryRun;
    if (isDryRun) {
      $('overwrite-toggle').checked  = false;
      $('pre-delete-toggle').checked = false;
    }
  })();

  $('download-report-btn').addEventListener('click', () => {
    if (!APP.lastReport) return;
    const blob = new Blob([JSON.stringify(APP.lastReport, null, 2)], { type: 'application/json' });
    const url  = URL.createObjectURL(blob);
    Object.assign(document.createElement('a'), { href: url, download: 'migration_report.json' }).click();
    URL.revokeObjectURL(url);
  });
}

async function runMigration(config, dryRun, overwrite = false, preDelete = true) {
  const feed      = $('results-feed');
  const progPanel = $('progress-panel');
  const summary   = $('summary-panel');
  const runBtn    = $('run-migration-btn');
  const backBtns  = [$('step3-back'), $('step3-back-top')];

  // Reset UI
  feed.innerHTML = '';
  feed.classList.remove('hidden');
  progPanel.classList.remove('hidden');
  summary.classList.add('hidden');
  APP.lastReport = null;

  $('progress-bar').style.width = '0%';
  $('progress-label').textContent = 'Connecting…';

  runBtn.disabled = true;
  runBtn.innerHTML = '<span class="spinner"></span> Running…';
  backBtns.forEach(b => b.disabled = true);

  const totalMappings = config.sections.length;
  let completedCount  = 0;

  try {
    const resp = await fetch('/migrate', {
      method:  'POST',
      headers: { 'Content-Type': 'application/json' },
      body:    JSON.stringify({ file_id: APP.fileId, dry_run: dryRun, overwrite, pre_delete: preDelete, config }),
    });

    if (!resp.ok) {
      const err = await resp.json().catch(() => ({}));
      addErrorItem(err.error || `Server error ${resp.status}`);
      return;
    }

    // ── SSE stream reading
    const reader  = resp.body.getReader();
    const decoder = new TextDecoder();
    let buffer    = '';

    while (true) {
      const { value, done } = await reader.read();
      if (done) break;

      buffer += decoder.decode(value, { stream: true });

      // Split on double newline (SSE event boundary)
      const parts = buffer.split('\n\n');
      for (let i = 0; i < parts.length - 1; i++) {
        const raw = parts[i].trim();
        if (!raw.startsWith('data:')) continue;
        try {
          const event = JSON.parse(raw.slice(5).trim());
          completedCount = handleSSEEvent(event, totalMappings, completedCount);
        } catch {
          // malformed JSON chunk — skip
        }
      }
      buffer = parts[parts.length - 1]; // keep incomplete tail
    }

    // Process any remaining complete event in buffer
    if (buffer.trim().startsWith('data:')) {
      try {
        const event = JSON.parse(buffer.trim().slice(5).trim());
        handleSSEEvent(event, totalMappings, completedCount);
      } catch {}
    }

    reader.releaseLock();

  } catch (err) {
    addErrorItem(`Stream error: ${err.message}`);
  } finally {
    runBtn.disabled = false;
    runBtn.innerHTML = '&#9654; Run Again';
    backBtns.forEach(b => b.disabled = false);
    if (!$('progress-label').textContent.startsWith('Done') &&
        !$('progress-label').textContent.startsWith('Migration')) {
      $('progress-label').textContent = 'Done';
    }
  }
}

/**
 * Handle a single SSE event.
 * Returns the updated completedCount.
 */
function handleSSEEvent(event, totalMappings, completedCount) {
  switch (event.type) {

    case 'section_start':
      $('progress-label').textContent =
        `Processing: "${esc(event.mapping_match)}" (${event.index + 1}/${event.total})`;
      break;

    case 'section_result': {
      completedCount++;
      const pct = totalMappings > 0 ? Math.round((completedCount / totalMappings) * 100) : 0;
      $('progress-bar').style.width = `${pct}%`;
      renderResultItem(event.result);
      break;
    }

    case 'complete':
      APP.lastReport = event.report;
      $('progress-bar').style.width = '100%';
      $('progress-label').textContent = 'Migration complete ✓';
      renderSummary(event.report);
      $('summary-panel').classList.remove('hidden');
      break;

    case 'error':
      addErrorItem(event.message || 'Unknown error from server');
      break;

    default:
      break;
  }
  return completedCount;
}

function renderResultItem(result) {
  const feed  = $('results-feed');
  const item  = document.createElement('div');
  const status = result.status || 'skipped';

  const badgeMap = {
    success: ['badge-ok',   'OK'],
    failed:  ['badge-fail', 'FAIL'],
    skipped: ['badge-skip', 'SKIP'],
  };
  const [badgeCls, badgeTxt] = badgeMap[status] ?? ['badge-neutral', status.toUpperCase()];

  item.className = `result-item result-${status}`;

  let html = `
    <span class="result-badge badge ${badgeCls}">${badgeTxt}</span>
    <div class="result-content">
      <div class="result-title">${esc(result.section_title || result.mapping_match)}</div>
      <div class="result-match">match: "${esc(result.mapping_match)}" · action: ${esc(result.action)}</div>
  `;

  if (result.confluence_page_url) {
    html += `<a href="${esc(result.confluence_page_url)}" class="result-link" target="_blank" rel="noopener noreferrer">
      View page in Confluence →
    </a>`;
  }

  if (result.error) {
    html += `<div class="result-error">${esc(result.error)}</div>`;
  }

  html += `</div>`;
  item.innerHTML = html;
  feed.appendChild(item);
  item.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
}

function addErrorItem(message) {
  const feed = $('results-feed');
  feed.classList.remove('hidden');
  const item = document.createElement('div');
  item.className = 'result-item result-failed';
  item.innerHTML = `
    <span class="result-badge badge badge-fail">ERR</span>
    <div class="result-content">
      <div class="result-error">${esc(message)}</div>
    </div>
  `;
  feed.appendChild(item);
}

function renderSummary(report) {
  const results = report.results || [];
  $('stat-total').textContent   = results.length;
  $('stat-success').textContent = results.filter(r => r.status === 'success').length;
  $('stat-failed').textContent  = results.filter(r => r.status === 'failed').length;
  $('stat-skipped').textContent = results.filter(r => r.status === 'skipped').length;
}

// ═══════════════════════════════════════════════════════════════════════════════
// INIT
// ═══════════════════════════════════════════════════════════════════════════════

document.addEventListener('DOMContentLoaded', () => {
  initStep1();
  initStep2();
  initStep3();
  initSectionRulesEditor();
  showStep(1);
  // Load saved defaults and section defaulting rules from server
  loadDefaults();
  loadSectionDefaultRules();
});
