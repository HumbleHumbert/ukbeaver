# ukbeaver 🦫

![ukbeaver mascot](assets/ukbeaver.png)

**A lightweight toolkit for working with UK Biobank (UKB) tabular and imaging data**

[![PyPI version](https://img.shields.io/pypi/v/ukbeaver.svg)](https://pypi.org/project/ukbeaver/)
[![Python versions](https://img.shields.io/pypi/pyversions/ukbeaver.svg)](https://pypi.org/project/ukbeaver/)
[![License](https://img.shields.io/pypi/l/ukbeaver.svg)](https:yourusername//github.com/HumbleHumbert/ukbeaver/blob/main/LICENSE)

UK Biobank (UKB) provides one of the world’s largest biomedical datasets, containing extensive **tabular information** (phenotypes, biomarkers, questionnaires) and **imaging data** (MRI, retinal scans, X-rays). While rich in potential, accessing and organizing UKB data can be cumbersome due to its **complex file structures, field IDs, and modality-specific formats**.

**ukbeaver** is designed to streamline this process. It provides a convenient interface to:

- 🗂 **Access and organize tabular data** — Handle field IDs, instances, and arrays with ease.
- 🖼 **Work with imaging data** — Load and manage different modalities without manual overhead.
- 🔎 **Query efficiently** — Simplify the process of extracting subsets of data for analysis.
- ⚡ **Integrate with existing workflows** — Built to be lightweight, flexible, and compatible with Python data science tools.

With `ukbeaver`, researchers can focus on **analysis and discovery**, instead of wrestling with data preprocessing.

---

## 🚀 Getting Started

### Installation

You can install `ukbeaver` using standard pip or modern package managers like `uv`.

#### Option 1: Fast installation with `uv` (Recommended)
We recommend using **[uv](https://github.com/astral-sh/uv)** for lightning-fast dependency management and installation.

1. **Install uv** (if you haven't already):
   [👉 **Click here for uv installation instructions**](https://docs.astral.sh/uv/getting-started/installation/)

2. **Add ukbeaver to your project**:
   ```bash
   uv add ukbeaver
   ```

#### Option 2: Standard pip
```bash
pip install ukbeaver
```

---

### Minimal Example

Here is how to quickly load and filter phenotype data:

```python
from ukbeaver.data.tabular import Phenotype

# Initialize with a UKB-style tab-delimited file
ph = Phenotype("pheno_table.tsv")

# Load everything into a DataFrame
df, field_map = ph.get_df()

# Select specific field IDs (e.g., height or age)
df_50, _ = ph.get_df(fids=["50"])

# Select only instance 0 (baseline assessment)
df_i0, _ = ph.get_df(ins=0)
```

---

## ⚠️ Data Access Disclaimer

**Access to UK Biobank data requires an approved UKB project application.**

`ukbeaver` is a tool designed to assist researchers in handling data they have already legally obtained. It **does not** bypass access restrictions, provide data, or manage credentials. Please ensure you comply with all UK Biobank Material Transfer Agreements (MTA) when using this tool.


















