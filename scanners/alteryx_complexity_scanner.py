import os
import glob
import xml.etree.ElementTree as ET
import pandas as pd
import zipfile
import re
from collections import Counter

workflow_folder = r"\\filesrv01.ssg.petsmart.com\npatil\V2\Downloads"

records = []        # detailed per-tool records
complexity = {}     # workflow base name → complexity metrics
source_target = {}  # workflow base name → {"sources": Counter, "targets": Counter}
macro_info = {}     # workflow base name → macro summary string

print("=" * 60)
print("🚀 STARTING ALTERYX CONNECTION SCANNER (Source/Target)")
print("=" * 60)
print(f"📁 Scanning folder: {workflow_folder}")
print(f"📁 Folder exists: {os.path.exists(workflow_folder)}")
print("=" * 60)

# -----------------------------------------------
# Plugin lookup — maps plugin string → (role, category_hint)
#   role: "Source" or "Target"
#   category_hint: used when file-extension detection isn't possible
# -----------------------------------------------

# INPUT plugins  (Source)
INPUT_PLUGINS = {
    "AlteryxBasePluginsGui.DbFileInput.DbFileInput":       "DB/File Input",
    "AlteryxBasePluginsGui.DynamicInput.DynamicInput":     "Dynamic Input",
    "AlteryxBasePluginsGui.TextInput.TextInput":           "Text Input",
    "AlteryxBasePluginsGui.MapInput.MapInput":             "Map Input",
    "AlteryxBasePluginsGui.DirectoryBrowse.DirectoryBrowse": "Directory Browse",
    "AlteryxConnectorGui.SharePointInput.SharePointInput": "SharePoint Input",
    "AlteryxConnectorGui.SalesforceInput.SalesforceInput": "Salesforce Input",
    "AlteryxConnectorGui.DownloadTool.DownloadTool":       "API/Download",
    "AlteryxBasePluginsGui.Download.Download":             "API/Download",
}

# OUTPUT plugins (Target)
OUTPUT_PLUGINS = {
    "AlteryxBasePluginsGui.DbFileOutput.DbFileOutput":     "DB/File Output",
    "AlteryxBasePluginsGui.DynamicOutput.DynamicOutput":   "Dynamic Output",
    "AlteryxBasePluginsGui.Render.Render":                 "Render",
    "PortfolioPluginsGui.ComposerRender.PortfolioComposerRender": "Render",
    "AlteryxBasePluginsGui.EmailTool.EmailTool":           "Email",
    "PortfolioPluginsGui.Email.Email":                     "Email",
    "AlteryxConnectorGui.SharePointOutput.SharePointOutput": "SharePoint Output",
    "AlteryxConnectorGui.SalesforceOutput.SalesforceOutput": "Salesforce Output",
}

# Combine for quick lookup:  plugin_str → ("Source"/"Target", hint)
PLUGIN_ROLES = {}
for p, hint in INPUT_PLUGINS.items():
    PLUGIN_ROLES[p] = ("Source", hint)
for p, hint in OUTPUT_PLUGINS.items():
    PLUGIN_ROLES[p] = ("Target", hint)

# -----------------------------------------------
# App / Interface tool plugins (for App Tools column)
# -----------------------------------------------
APP_TOOL_PLUGINS = {
    "Tab", "QuestionTextBox", "RadioButtonGroup", "NumericUpDown",
    "DropDown", "ListBox", "CheckBox", "Date", "FileBrowse",
    "Action", "Condition", "ControlParam", "MacroInput", "MacroOutput",
}

ADVANCED_TOOLS = {
    "JupyterCode", "RunCommand", "Python", "R",
    "Condition", "BlockUntilDone", "MacroInput",
    "MacroOutput", "ControlParam"
}

# -----------------------------------------------
# COMPLEXITY SCORING  (unchanged from original)
# -----------------------------------------------
# SQL Complexity scoring for individual queries
# -----------------------------------------------
def compute_sql_complexity(sql_text):
    """Score a single SQL query's complexity based on its structure."""
    if not sql_text:
        return 0, "Simple"
    sql = sql_text.lower()
    score = 0

    # JOINs — each join adds integration complexity
    joins = len(re.findall(r'\bjoin\b', sql))
    if joins >= 5:       score += 3
    elif joins >= 2:     score += 2
    elif joins >= 1:     score += 1

    # CTEs (WITH ... AS) — indicates layered logic
    ctes = len(re.findall(r'\bwith\b\s+\w+\s+as\s*\(', sql))
    score += min(ctes * 2, 4)

    # Subqueries — nested SELECT inside parentheses
    subqueries = len(re.findall(r'\(\s*select\b', sql))
    score += min(subqueries, 3)

    # GROUP BY + HAVING — aggregation logic
    if 'group by' in sql:  score += 1
    if 'having' in sql:    score += 1

    # CASE WHEN — conditional logic
    cases = len(re.findall(r'\bcase\b', sql))
    score += min(cases, 2)

    # Window functions (ROW_NUMBER, RANK, LEAD, LAG, OVER)
    window_fns = len(re.findall(r'\b(row_number|rank|dense_rank|ntile|lead|lag|first_value|last_value)\b', sql))
    if 'over(' in sql.replace(' ', '') or 'over (' in sql:
        window_fns = max(window_fns, 1)
    score += min(window_fns * 2, 4)

    # UNION / UNION ALL — multi-result-set queries
    unions = len(re.findall(r'\bunion\b', sql))
    score += min(unions, 2)

    # Query length as a proxy for overall complexity
    lines = sql.count('\n') + 1
    if lines >= 50:      score += 2
    elif lines >= 20:    score += 1

    # Assign label
    if score <= 2:    label = "Simple"
    elif score <= 5:  label = "Moderate"
    elif score <= 10: label = "Complex"
    else:             label = "Very Complex"

    return score, label

# -----------------------------------------------
# WORKFLOW COMPLEXITY SCORING  (10-point scale)
# -----------------------------------------------
# Dimension 1 — Tool Count                (max 3 pts)
#   1–20   tools  → 1 pt
#   21–75  tools  → 2 pts
#   76+    tools  → 3 pts
#
# Dimension 2 — Output Destination        (max 2 pts)
#   Tiered — highest output category present wins:
#     Heavy  (SQL Server, Oracle, Snowflake, SharePoint)  → 2 pts
#     Medium (Email, Render)                               → 1 pt
#     Light  (Excel, CSV, YXDB, Network File) or None     → 0 pts
#
# Dimension 3 — DB Connections            (max 2 pts)
#   0 DB tools   → 0 pts
#   1 DB tool    → 1 pt
#   2+ DB tools  → 2 pts
#
# Dimension 4 — Advanced Features         (max 1 pt)
#   Has macros OR has Python/R/Jupyter/RunCommand → 1 pt
#
# Dimension 5 — SQL Complexity            (max 2 pts)
#   Max SQL score across all queries in workflow:
#   0–2      → 0 pts  (simple / no SQL)
#   3–7      → 1 pt   (moderate)
#   8+       → 2 pts  (complex / very complex)
#
# Total score capped at 10
# 1–2  → Very Low
# 3–4  → Low
# 5–6  → Medium
# 7–8  → High
# 9–10 → Complex
# -----------------------------------------------

# Output type tiers for Dimension 2
HEAVY_OUTPUT_TYPES = {
    "SQL (Oracle)", "SQL (Snowflake)", "SQL (SQL Server)",
    "SQL (ODBC)", "SQL (PostgreSQL)", "SQL (MySQL)",
    "SQL (Teradata)", "SQL (Redshift)", "SQL (BigQuery)",
    "SQL (Alias)", "SQL",
    "SharePoint Output",
    "Dynamic Output",
}

MEDIUM_OUTPUT_TYPES = {
    "Email",
    "Render", "Render - Excel", "Render - PDF", "Render - HTML",
    "Salesforce Output",
}

def compute_output_tier(output_type_counter):
    """Tiered scoring: highest output category present wins.
       Returns 2 (heavy), 1 (medium), or 0 (light/none)."""
    has_heavy = any(t in HEAVY_OUTPUT_TYPES for t in output_type_counter)
    has_medium = any(t in MEDIUM_OUTPUT_TYPES for t in output_type_counter)
    if has_heavy:  return 2
    if has_medium: return 1
    return 0

def compute_complexity(tool_count, db_count, has_macros, has_advanced, max_sql_score=0, output_tier=0):
    score = 0

    # Dimension 1: Tool count (max 3)
    if tool_count <= 20:       score += 1
    elif tool_count <= 75:     score += 2
    else:                      score += 3

    # Dimension 2: Output destination tier (max 2)
    score += output_tier  # already 0, 1, or 2

    # Dimension 3: DB connections (max 2)
    if db_count >= 2:          score += 2
    elif db_count == 1:        score += 1

    # Dimension 4: Advanced features (max 1)
    if has_macros or has_advanced:
        score += 1

    # Dimension 5: SQL complexity (max 2)
    if max_sql_score >= 8:     score += 2
    elif max_sql_score >= 3:   score += 1

    score = min(score, 10)

    if score <= 2:    label = "Very Low"
    elif score <= 4:  label = "Low"
    elif score <= 6:  label = "Medium"
    elif score <= 8:  label = "High"
    else:             label = "Complex"

    return score, label

# -----------------------------------------------
# Classify a <File> / connection value into a
# human-friendly category label
# -----------------------------------------------
def classify_io_category(raw_value, plugin_hint):
    """Return a short category like 'SQL (Oracle)', 'Excel', 'CSV', 'YXDB', etc."""

    if not raw_value:
        return plugin_hint  # fallback to plugin hint (e.g. "Browse", "Email")

    val = raw_value.lower().strip()

    # --- File extension-based (check FIRST — works for local, network, UNC paths) ---
    if re.search(r'\.(xlsx|xls|xlsm)(\b|$|["\s])', val):
        return "Excel"
    if re.search(r'\.csv(\b|$|["\s])', val):
        return "CSV"
    if re.search(r'\.yxdb(\b|$|["\s])', val):
        return "YXDB"
    if re.search(r'\.txt(\b|$|["\s])', val):
        return "Text"
    if re.search(r'\.json(\b|$|["\s])', val):
        return "JSON"
    if re.search(r'\.parquet(\b|$|["\s])', val):
        return "Parquet"
    if re.search(r'\.pdf(\b|$|["\s])', val):
        return "PDF"
    if re.search(r'\.html?(\b|$|["\s])', val):
        return "HTML"

    # --- aka: alias patterns (common on Alteryx Server) ---
    #     e.g. aka:svc_Oracle_PHNX_18, aka:svc_Snowflake_ODBC64_All
    if val.startswith("aka:"):
        aka_name = val[4:]  # strip "aka:"
        if "oracle" in aka_name:
            return "SQL (Oracle)"
        if "snowflake" in aka_name or "snowfl" in aka_name:
            return "SQL (Snowflake)"
        if "mssql" in aka_name or "sqlserver" in aka_name or "sql_server" in aka_name:
            return "SQL (SQL Server)"
        if "postgres" in aka_name or "_pg_" in aka_name:
            return "SQL (PostgreSQL)"
        if "mysql" in aka_name:
            return "SQL (MySQL)"
        if "teradata" in aka_name:
            return "SQL (Teradata)"
        if "redshift" in aka_name:
            return "SQL (Redshift)"
        if "bigquery" in aka_name or "_bq_" in aka_name:
            return "SQL (BigQuery)"
        if "odbc" in aka_name:
            return "SQL (ODBC)"
        # Generic SQL alias — still a database connection
        return "SQL (Alias)"

    # --- Database patterns ---
    if "snowflake" in val or "snowbl:" in val:
        return "SQL (Snowflake)"
    if "oracle" in val and ("oci" in val or "tns" in val or "svc" in val or "dsn" in val):
        return "SQL (Oracle)"
    if "sqlnative" in val or "sql server" in val or "mssql" in val:
        return "SQL (SQL Server)"
    if "postgres" in val:
        return "SQL (PostgreSQL)"
    if "mysql" in val:
        return "SQL (MySQL)"
    if "teradata" in val:
        return "SQL (Teradata)"
    if "redshift" in val:
        return "SQL (Redshift)"
    if "odbc" in val or "dsn=" in val:
        return "SQL (ODBC)"
    if "|||" in val:
        # triple-pipe = connection|||table_or_query — inspect the connection part
        conn_part = val.split("|||")[0].strip()
        if "oracle" in conn_part:       return "SQL (Oracle)"
        if "snowflake" in conn_part:    return "SQL (Snowflake)"
        if "sql" in conn_part:          return "SQL (SQL Server)"
        return "SQL"

    # --- Network / dynamic paths (file ext already checked above) ---
    if raw_value.startswith("\\\\") or raw_value.startswith("//"):
        return "Network File"
    if raw_value.startswith("%"):
        return "Dynamic Path"

    # --- Fallback to plugin hint ---
    return plugin_hint

# -----------------------------------------------
# Extract the "attachment type" from Email tool config
# -----------------------------------------------
def get_email_attachment_type(config_elem):
    """Try to determine what kind of attachment an Email tool sends."""
    if config_elem is None:
        return ""
    # Look for attachment paths in common config fields
    for tag in ['Attachment', 'Body', 'From', 'To']:
        elem = config_elem.find(f'.//{tag}')
        if elem is not None and elem.text:
            val = elem.text.lower()
            if ".xlsx" in val or ".xls" in val:
                return " (Excel attachment)"
            if ".csv" in val:
                return " (CSV attachment)"
            if ".pdf" in val:
                return " (PDF attachment)"
    return ""

# -----------------------------------------------
# Extract Render output format
# -----------------------------------------------
def get_render_format(config_elem):
    """Determine what format a Render tool outputs (Excel, PDF, etc.)."""
    if config_elem is None:
        return ""
    # Check for output file path
    file_elem = config_elem.find('.//File')
    pcxml = config_elem.find('.//pcxml')
    temp_file = config_elem.find('.//TempFile')

    for elem in [file_elem, pcxml, temp_file]:
        if elem is not None and elem.text:
            val = elem.text.lower()
            if ".xlsx" in val or ".xls" in val:
                return " - Excel"
            if ".pdf" in val:
                return " - PDF"
            if ".html" in val:
                return " - HTML"

    # Check OutputMode or similar attributes
    for child in config_elem.iter():
        txt = (child.text or "").lower()
        for attr_val in child.attrib.values():
            txt += attr_val.lower()
        if "excel" in txt or "xlsx" in txt:
            return " - Excel"
        if "pdf" in txt:
            return " - PDF"
    return ""

# -----------------------------------------------
# Classify what a macro handles based on its
# file path, annotation, or inner config hints
# -----------------------------------------------
def classify_macro_type(macro_path, node):
    """Return a short label like 'SQL', 'Excel', 'Email', etc.
    
    Checks (in order): macro filename → annotation → config XML → field lineage.
    """
    mp = macro_path.lower()

    # Check macro file name / path for clues
    keyword_map = [
        (["orchestrator", "uipath", "rpa", "robot", "automat"], "RPA/Orchestrator"),
        (["oracle", "phnx", "atla"], "Oracle"),
        (["snowflake", "snowfl"], "Snowflake"),
        (["sql", "database", "db", "query", "odbc"], "SQL"),
        (["excel", "xlsx", "xls"], "Excel"),
        (["csv"], "CSV"),
        (["yxdb"], "YXDB"),
        (["email", "smtp", "mail", "attachment"], "Email"),
        (["api", "download", "http", "rest"], "API"),
        (["sftp", "ftp", "scp"], "FTP/SFTP"),
        (["sharepoint"], "SharePoint"),
        (["salesforce", "sfdc"], "Salesforce"),
        (["json", "xml"], "Data Format"),
        (["parquet"], "Parquet"),
        (["render", "report", "pdf", "reporting"], "Report"),
        (["calendar", "fiscal", "date"], "Calendar"),
        (["directory", "folder", "makedir"], "File System"),
        (["cleanse", "clean", "parse", "regex", "format", "preformat"], "Data Prep"),
        (["count", "countrecord"], "Utility"),
        (["union"], "Utility"),
        (["user", "auth", "login", "credential"], "User/Auth"),
        (["error", "log", "logging", "validation"], "Logging"),
    ]
    for keywords, label in keyword_map:
        if any(kw in mp for kw in keywords):
            return label

    # Check annotation text on the node for hints
    annotation = node.find('.//Annotation')
    if annotation is not None:
        ann_elem = annotation.find('.//Name')
        ann_text = ""
        if ann_elem is not None and ann_elem.text:
            ann_text = ann_elem.text.lower()
        else:
            ann_default = annotation.find('.//DefaultAnnotationText')
            if ann_default is not None and ann_default.text:
                ann_text = ann_default.text.lower()
        if ann_text:
            for keywords, label in keyword_map:
                if any(kw in ann_text for kw in keywords):
                    return label

    # Check configuration for any connection strings
    config = node.find('.//Configuration')
    if config is not None:
        config_text = ET.tostring(config, encoding='unicode', method='text').lower()
        for keywords, label in keyword_map:
            if any(kw in config_text for kw in keywords):
                return label

    # Check Field lineage — Field source attributes often reveal
    # what connections flow through a macro (e.g. "File: aka:svc_Oracle_PHNX_18|||...")
    io_keyword_map = [
        (["aka:svc_oracle", "oracle", "phnx", "atla"], "Oracle"),
        (["aka:svc_snowflake", "snowflake"], "Snowflake"),
        (["aka:svc_sqlnative", "sql server", "mssql"], "SQL Server"),
        (["aka:", "odbc", "dsn="], "SQL"),
        ([".xlsx", ".xls"], "Excel"),
        ([".csv"], "CSV"),
        ([".yxdb"], "YXDB"),
    ]
    for field in node.iter('Field'):
        src = (field.get('source', '') or '').lower()
        if src:
            for keywords, label in io_keyword_map:
                if any(kw in src for kw in keywords):
                    return label

    return "General"

# -----------------------------------------------
# Process a single XML root
# -----------------------------------------------
def process_root(root, workflow_name, package_name, source_type, has_macros_in_pkg=False):

    all_nodes = list(root.iter('Node'))
    tool_count = len(all_nodes)
    plugin_names = []
    for node in all_nodes:
        gui = node.find('.//GuiSettings')
        if gui is not None:
            short = gui.get('Plugin', '').split('.')[-1]
            if short:
                plugin_names.append(short)

    unique_types  = len(set(plugin_names))
    db_count      = len(set(p for p in plugin_names if p in {
                        "DbFileInput","DbFileOutput","DynamicInput","DynamicOutput"}))
    has_advanced  = any(p in ADVANCED_TOOLS for p in plugin_names)

    # --- SQL Complexity: score each SQL query, track the max and build summary ---
    sql_scores = []
    sql_details = []
    for node in all_nodes:
        gui = node.find('.//GuiSettings')
        if gui is None:
            continue
        plugin = gui.get('Plugin', '')
        if not any(x in plugin for x in ['DbFile', 'Dynamic']):
            continue
        config = node.find('.//Configuration')
        if config is None:
            continue
        file_elem = config.find('.//File')
        if file_elem is None or not file_elem.text:
            continue
        raw = file_elem.text.strip()
        if '|||' not in raw:
            continue
        _, sql_text = raw.split('|||', 1)
        sql_text = sql_text.strip()
        if not sql_text:
            continue
        sq_score, sq_label = compute_sql_complexity(sql_text)
        sql_scores.append(sq_score)
        sql_details.append((sq_score, sq_label))

    max_sql_score = max(sql_scores) if sql_scores else 0

    # Build SQL summary: "3 queries (1 Very Complex, 1 Complex, 1 Simple)"
    if sql_details:
        label_counter = Counter(lbl for _, lbl in sql_details)
        parts = []
        # Order: Very Complex > Complex > Moderate > Simple
        for lbl in ["Very Complex", "Complex", "Moderate", "Simple"]:
            if lbl in label_counter:
                parts.append(f"{label_counter[lbl]} {lbl}")
        sql_summary = f"{len(sql_details)} Queries ({', '.join(parts)})"
    else:
        sql_summary = "None"

    # App / Interface tools
    app_tool_counter = Counter()
    for p in plugin_names:
        if p in APP_TOOL_PLUGINS:
            app_tool_counter[p] += 1

    # App tools summary string
    def app_counter_to_summary(counter):
        if not counter:
            return "None"
        parts = []
        for cat, count in counter.most_common():
            parts.append(f"{count} {cat}")
        return ", ".join(parts)

    app_tools_summary = app_counter_to_summary(app_tool_counter)

    wf_base = os.path.splitext(workflow_name)[0].lower()

    # ------ Macro detection ------
    macro_count = 0
    all_macro_io = Counter()  # aggregated internal connections across all macros
    seen_macro_paths = set()  # deduplicate by path

    # METHOD 1: Check each Node for macro references
    for node in all_nodes:
        gui = node.find('.//GuiSettings')
        if gui is None:
            continue
        plugin = gui.get('Plugin', '')

        is_macro = False
        macro_path = ""

        # Check if plugin name indicates a macro
        if "Macro" in plugin or plugin.endswith(".yxmc"):
            is_macro = True
            macro_path = plugin

        # Check EngineSettings Macro="..." attribute (KEY discovery from real files)
        engine = node.find('.//EngineSettings')
        if engine is not None:
            macro_attr = engine.get('Macro', '')
            if macro_attr and '.yxmc' in macro_attr.lower():
                is_macro = True
                macro_path = macro_attr

        # Check for macro file path in configuration
        config = node.find('.//Configuration')
        if config is not None:
            for tag in ['MacroFilePath', 'Macro', 'File', 'FileName']:
                elem = config.find(f'.//{tag}')
                if elem is not None and elem.text and '.yxmc' in elem.text.lower():
                    is_macro = True
                    macro_path = elem.text.strip()
                    break
            for attr_val in config.attrib.values():
                if '.yxmc' in attr_val.lower():
                    is_macro = True
                    macro_path = attr_val
                    break

        # Check GuiSettings for macro path in attrs
        if not is_macro:
            for attr_val in gui.attrib.values():
                if '.yxmc' in attr_val.lower():
                    is_macro = True
                    macro_path = attr_val
                    break

        if is_macro and macro_path:
            # Deduplicate by normalized path
            norm_path = macro_path.lower().replace("\\\\", "\\").strip()
            if norm_path not in seen_macro_paths:
                seen_macro_paths.add(norm_path)
                macro_count += 1

                # --- Extract macro's internal connections from MetaInfo fields ---
                seen_io_for_this_macro = set()
                for field in node.iter('Field'):
                    src_attr = field.get('source', '')
                    if not src_attr:
                        continue
                    src_lower = src_attr.lower()
                    if 'file:' in src_lower:
                        file_part = src_attr.split('File:', 1)[-1].strip()
                        cat = classify_io_category(file_part, "")
                        if cat and cat not in ("", "DB/File Input", "DB/File Output"):
                            # Deduplicate per-macro so same connection type
                            # from repeated fields only counts once per macro
                            if cat not in seen_io_for_this_macro:
                                seen_io_for_this_macro.add(cat)
                                all_macro_io[cat] += 1

    # METHOD 2: Check <Dependency> tags at workflow level (IsMacro or .yxmc path)
    for dep in root.iter('Dependency'):
        dep_path = dep.get('Path', '')
        is_macro_dep = dep.get('IsMacro', 'False').lower() == 'true'
        if not is_macro_dep and '.yxmc' in dep_path.lower():
            is_macro_dep = True
        if is_macro_dep and dep_path:
            norm_path = dep_path.lower().replace("\\\\", "\\").strip()
            if norm_path not in seen_macro_paths:
                seen_macro_paths.add(norm_path)
                macro_count += 1

    # METHOD 3: Check top-level EngineSettings (workflow-level macro refs)
    for eng in root.iter('EngineSettings'):
        macro_attr = eng.get('Macro', '')
        if macro_attr and '.yxmc' in macro_attr.lower():
            norm_path = macro_attr.lower().replace("\\\\", "\\").strip()
            if norm_path not in seen_macro_paths:
                seen_macro_paths.add(norm_path)
                macro_count += 1

    # Build macro summary: "5 Macros (2 SQL (Oracle), 1 SQL (Snowflake), 1 Excel)"
    if macro_count > 0:
        macro_word = "Macro" if macro_count == 1 else "Macros"
        if all_macro_io:
            io_parts = ", ".join(f"{c} {cat}" for cat, c in all_macro_io.most_common())
            macro_summary = f"{macro_count} {macro_word} ({io_parts})"
        else:
            macro_summary = f"{macro_count} {macro_word}"
    else:
        macro_summary = "None"

    macro_info[wf_base] = macro_summary

    # --- Has macros flag for complexity scoring ---
    has_macros_detected = macro_count > 0 or has_macros_in_pkg

    # ------ Source / Target aggregation ------
    src_counter = Counter()
    tgt_counter = Counter()

    for node in all_nodes:
        gui = node.find('.//GuiSettings')
        if gui is None:
            continue
        plugin = gui.get('Plugin', '')
        if plugin not in PLUGIN_ROLES:
            continue

        role, hint = PLUGIN_ROLES[plugin]
        config = node.find('.//Configuration')

        # Get raw file/connection value
        raw_value = ""
        if config is not None:
            file_elem = config.find('.//File')
            if file_elem is not None and file_elem.text:
                raw_value = file_elem.text.strip()
            elif config.find('.//FileName') is not None:
                fn = config.find('.//FileName')
                if fn is not None and fn.text:
                    raw_value = fn.text.strip()
            # Render tool uses <OutputFile> instead of <File>
            if not raw_value:
                of = config.find('.//OutputFile')
                if of is not None and of.text:
                    raw_value = of.text.strip()

        # Determine category
        category = classify_io_category(raw_value, hint)

        # Special handling for Render and Email
        if hint == "Render":
            fmt = get_render_format(config)
            category = f"Render{fmt}" if fmt else "Render"
        elif hint == "Email":
            att = get_email_attachment_type(config)
            category = f"Email{att}" if att else "Email"
        elif hint == "Dynamic Input" or hint == "Dynamic Output":
            # Try to detect what format the dynamic tool handles
            sub_cat = classify_io_category(raw_value, "")
            if sub_cat and sub_cat != hint:
                category = f"Dynamic Input - {sub_cat}" if role == "Source" else f"Dynamic Output - {sub_cat}"
            else:
                category = hint

        # Accumulate
        if role == "Source":
            src_counter[category] += 1
        else:
            tgt_counter[category] += 1

        # Also keep detailed records
        connection = raw_value.split("|||")[0].strip() if "|||" in raw_value else raw_value
        table_or_sql = raw_value.split("|||", 1)[1].strip() if "|||" in raw_value else ""
        records.append({
            "Workflow Name":   workflow_name,
            "Package Name":    package_name,
            "Role":            role,       # "Source" or "Target"
            "Category":        category,
            "Tool Type":       hint,
            "Connection":      connection,
            "Table / SQL":     table_or_sql,
            "Source Type":     source_type,
        })

    # Build summary strings: "2 SQL, 1 Excel, 5 CSV"
    def io_counter_to_summary(counter):
        if not counter:
            return "None"
        parts = []
        for cat, count in counter.most_common():
            parts.append(f"{count} {cat}")
        return ", ".join(parts)

    source_target[wf_base] = {
        "Source": io_counter_to_summary(src_counter),
        "Target": io_counter_to_summary(tgt_counter),
        "App Tools": app_tools_summary,
    }

    # --- Compute output destination tier for complexity scoring ---
    output_tier = compute_output_tier(tgt_counter)
    output_tier_label = {0: "Light", 1: "Medium", 2: "Heavy"}.get(output_tier, "Light")

    # --- Final complexity scoring (after all dimensions are known) ---
    score, label = compute_complexity(
        tool_count, db_count, has_macros_detected, has_advanced, max_sql_score, output_tier
    )

    complexity[wf_base] = {
        "Workflow Name":       workflow_name,
        "Package Name":        package_name,
        "Source Type":         source_type,
        "Tool Count":          tool_count,
        "DB Connection Count": db_count,
        "Output Tier":         output_tier_label,
        "Macro Count":         macro_count,
        "Has Advanced Tools":  has_advanced,
        "SQL Complexity":      sql_summary,
        "Max SQL Score":       max_sql_score,
        "Complexity Score":    score,
        "Complexity Level":    label,
    }

    print(f"    ✅ Tools: {tool_count} | Output: {output_tier_label} | Score: {score} ({label})")
    print(f"       Sources:   {io_counter_to_summary(src_counter)}")
    print(f"       Targets:   {io_counter_to_summary(tgt_counter)}")
    print(f"       Macros:    {macro_summary}")
    print(f"       App Tools: {app_tools_summary}")

# -----------------------------------------------
# Helper: scan a single XML file
# -----------------------------------------------
def scan_xml_file(filepath, workflow_name, package_name, source_type):
    try:
        tree = ET.parse(filepath)
        process_root(tree.getroot(), workflow_name, package_name, source_type)
    except Exception as e:
        print(f"    ❌ ERROR: {e}")

# -----------------------------------------------
# Helper: scan inner files of a zip
# -----------------------------------------------
def scan_zip(filepath, source_type_label):
    package_name = os.path.basename(filepath)
    print(f"\n  Processing Package: {package_name}")
    try:
        with zipfile.ZipFile(filepath, 'r') as z:
            all_inner      = z.namelist()
            has_macros     = any(f.endswith('.yxmc') for f in all_inner)
            inner_workflows = [f for f in all_inner
                               if f.endswith(('.yxmd', '.yxwz', '.yxmc'))
                               and '_externals' not in f]
            print(f"    📄 Inner workflows: {len(inner_workflows)} | Has macros: {has_macros}")
            for inner_file in inner_workflows:
                workflow_name = os.path.basename(inner_file)
                print(f"    → {workflow_name}")
                with z.open(inner_file) as f:
                    try:
                        tree = ET.parse(f)
                        process_root(tree.getroot(), workflow_name, package_name,
                                     source_type_label, has_macros_in_pkg=has_macros)
                    except Exception as e:
                        print(f"    ❌ ERROR in {workflow_name}: {e}")
    except Exception as e:
        print(f"    ❌ ERROR opening package: {e}")

# -----------------------------------------------
# Part 1: .yxmd Workflow Files
# -----------------------------------------------
print("\n📂 PART 1: SCANNING .yxmd WORKFLOW FILES")
print("-" * 60)
yxmd_files = glob.glob(workflow_folder + "\\**\\*.yxmd", recursive=True)
yxmd_files = [f for f in yxmd_files if "$RECYCLE.BIN" not in f and "$R" not in os.path.basename(f)]
print(f"📄 Total .yxmd files found: {len(yxmd_files)}")

for i, filepath in enumerate(yxmd_files, 1):
    filename = os.path.basename(filepath)
    print(f"\n  [{i}/{len(yxmd_files)}] {filename}")
    scan_xml_file(filepath, filename, "", "Workflow (.yxmd)")

print(f"\n✅ PART 1 COMPLETE | Records: {len(records)} | Workflows: {len(complexity)}")

# -----------------------------------------------
# Part 2: .yxzp Package Files
# -----------------------------------------------
print("\n📦 PART 2: SCANNING .yxzp PACKAGE FILES")
print("-" * 60)
yxzp_files = glob.glob(workflow_folder + "\\**\\*.yxzp", recursive=True)
yxzp_files = [f for f in yxzp_files if "$RECYCLE.BIN" not in f and "$R" not in os.path.basename(f)]
print(f"📦 Total .yxzp files found: {len(yxzp_files)}")

for i, filepath in enumerate(yxzp_files, 1):
    print(f"\n  [{i}/{len(yxzp_files)}]", end="")
    scan_zip(filepath, "Package (.yxzp)")

print(f"\n✅ PART 2 COMPLETE | Records: {len(records)} | Workflows: {len(complexity)}")

# -----------------------------------------------
# Part 3: .yxwz Analytic App Files
# -----------------------------------------------
print("\n🔧 PART 3: SCANNING .yxwz ANALYTIC APP FILES")
print("-" * 60)
yxwz_files = glob.glob(workflow_folder + "\\**\\*.yxwz", recursive=True)
yxwz_files = [f for f in yxwz_files if "$RECYCLE.BIN" not in f and "$R" not in os.path.basename(f)]
print(f"🔧 Total .yxwz files found: {len(yxwz_files)}")

for i, filepath in enumerate(yxwz_files, 1):
    filename = os.path.basename(filepath)
    print(f"\n  [{i}/{len(yxwz_files)}] {filename}")
    scan_xml_file(filepath, filename, "", "Analytic App (.yxwz)")

print(f"\n✅ PART 3 COMPLETE | Records: {len(records)} | Workflows: {len(complexity)}")

# -----------------------------------------------
# Part 4: Save Results
# -----------------------------------------------
print("\n💾 PART 4: SAVING RESULTS")
print("-" * 60)

CUSTOM_OUTPUT_FOLDER = r"\\filesrv01.ssg.petsmart.com\npatil\V2\My Documents\Alteryx Database connection"
output_folder = CUSTOM_OUTPUT_FOLDER if CUSTOM_OUTPUT_FOLDER else os.path.abspath(os.getcwd())

def save_file(df, filename):
    path = os.path.join(output_folder, filename)
    print(f"💾 Saving: {path}")
    try:
        os.makedirs(output_folder, exist_ok=True)
        if filename.endswith('.csv'):
            df.to_csv(path, index=False)
        else:
            df.to_excel(path, index=False)
        print(f"✅ Saved: {path}")
        return path
    except Exception as e:
        print(f"❌ Failed: {e}")
        fallback = os.path.join(os.path.expanduser("~"), filename)
        try:
            if filename.endswith('.csv'):
                df.to_csv(fallback, index=False)
            else:
                df.to_excel(fallback, index=False)
            print(f"✅ Saved to fallback: {fallback}")
            return fallback
        except Exception as e2:
            print(f"❌ Fallback failed: {e2}")
            return None

# --- Save detailed connections CSV ---
if records:
    conn_df = pd.DataFrame(records).drop_duplicates()
    save_file(conn_df, "Alteryx_Connections_Detail.csv")
    print(f"\n📊 CONNECTIONS BREAKDOWN BY CATEGORY:")
    print(conn_df['Category'].value_counts().to_string())
else:
    print("⚠️  No connections found.")

# --- Save complexity + source/target Excel ---
if complexity:
    comp_df = pd.DataFrame(complexity.values())

    # Merge in Source / Target / Macros / App Tools summary columns
    comp_df["_key"] = comp_df["Workflow Name"].apply(lambda x: os.path.splitext(x)[0].lower())
    comp_df["Source"] = comp_df["_key"].map(lambda k: source_target.get(k, {}).get("Source", "None"))
    comp_df["Target"] = comp_df["_key"].map(lambda k: source_target.get(k, {}).get("Target", "None"))
    comp_df["Macros"] = comp_df["_key"].map(lambda k: macro_info.get(k, "None"))
    comp_df["App Tools"] = comp_df["_key"].map(lambda k: source_target.get(k, {}).get("App Tools", "None"))
    comp_df.drop(columns=["_key"], inplace=True)

    # Reorder columns so Source/Target/Macros/App Tools are prominent
    col_order = [
        "Workflow Name", "Package Name", "Source Type",
        "Source", "Target", "Macros", "App Tools",
        "SQL Complexity",
        "Tool Count", "DB Connection Count", "Output Tier",
        "Macro Count", "Has Advanced Tools", "Max SQL Score",
        "Complexity Score", "Complexity Level",
    ]
    comp_df = comp_df[[c for c in col_order if c in comp_df.columns]]
    comp_df = comp_df.sort_values("Complexity Score", ascending=False).reset_index(drop=True)

    save_file(comp_df, "Alteryx_Complexity.xlsx")

    print(f"\n📊 COMPLEXITY BREAKDOWN:")
    print("-" * 40)
    print(comp_df['Complexity Level'].value_counts().to_string())

    print(f"\n👀 TOP 10 WORKFLOWS (Source → Target → Macros):")
    print("-" * 80)
    top10 = comp_df[['Workflow Name', 'Source', 'Target', 'Macros',
                      'Complexity Score', 'Complexity Level']].head(10)
    print(top10.to_string(index=False))
else:
    print("⚠️  No workflows found.")

print("\n" + "=" * 60)
print("🏁 SCAN COMPLETE!")
print("=" * 60)
