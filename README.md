# alteryx-migration
Python-based tools for Alteryx Server → Databricks migration. Includes complexity scanner, DB connection extractor &amp; macro dependency scanner.
# 🔄 Alteryx Migration Tools

Python-based automation tools developed to support the 
**Alteryx Server → Databricks** migration project at enterprise scale.

---

## 🎯 Purpose
- Scan & analyze Alteryx workflows before migration
- Score workflow complexity to prioritize migration effort
- Extract database connections & macro dependencies automatically

---

## 🛠️ Tech Stack
- **Language:** Python
- **Libraries:** xml.etree.ElementTree, os, openpyxl, pandas
- **Platform:** Alteryx Server, Databricks
- **Output:** Excel Reports (.xlsx)

---

## 📂 Tools Included

| Tool | File | Description |
|---|---|---|
| Complexity Scanner | `alteryx_complexity_scanner.py` | Scores workflows on 5 dimensions |
| Source Target Scanner | `source_target_scanner.py` | Scans source and target connections |
| Log Frequency Analyzer | `Alteryx_log_frequency_analysis.py` | Analyzes workflow execution logs |
| Gallery Extractor | `Alteryx_gallery_extract.py` | Extracts workflow list from Alteryx Gallery |
| Server Details | `Alteryx_Server_details_MongoDB.py` | Fetches server metadata from MongoDB |

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
