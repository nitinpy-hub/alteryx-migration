# alteryx-migration
Python-based tools for Alteryx Server → Alteryx One migration. Includes complexity scanner, DB connection extractor &amp; macro dependency scanner.
# 🔄 Alteryx Migration Tools

Python-based automation tools developed to support the 
**Alteryx Server → Alteryx One** migration project at enterprise scale.

---

## 🎯 Purpose
- Scan & analyze Alteryx workflows before migration
- Score workflow complexity to prioritize migration effort
- Extract database connections & macro dependencies automatically

---

## 🛠️ Tech Stack
- **Language:** Python
- **Libraries:** xml.etree.ElementTree, os, openpyxl, pandas
- **Platform:** Alteryx Server, Alteryx One
- **Output:** Excel Reports (.xlsx)

---

## 📂 Tools Included

| Tool | File | Description |
|---|---|---|
| Complexity Scanner | `alteryx_complexity_scanner.py` | Scores workflows on 5 dimensions |
| DB Connection Extractor | `db_connection_extractor.py` | Extracts all DB aliases & connection types |
| Macro Dependency Scanner | `macro_dependency_scanner.py` | Deep scans macro usage across workflows |

---

## 📊 Complexity Scoring System (10-Point Scale)

| Dimension | Max Score |
|---|---|
| Tool Count | 2 |
| Output Destination | 2 |
| DB Connections | 2 |
| Advanced Features | 2 |
| SQL Complexity | 2 |
| **Total** | **10** |

### Score Interpretation:
| Score | Category |
|---|---|
| 1 - 3 | 🟢 Low Complexity |
| 4 - 6 | 🟡 Medium Complexity |
| 7 - 8 | 🟠 High Complexity |
| 9 - 10 | 🔴 Very High Complexity |
