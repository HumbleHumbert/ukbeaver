# ukbeaver 🦫

![ukbeaver mascot](assets/ukbeaver.png)

**A lightweight toolkit for working with UK Biobank (UKB) tabular and imaging data**

UK Biobank (UKB) provides one of the world’s largest biomedical datasets, containing extensive **tabular information** (phenotypes, biomarkers, questionnaires, etc.) and **imaging data** (MRI, retinal scans, X-rays, etc.). While rich in potential, accessing and organizing UKB data can be cumbersome due to its **complex file structures, field IDs, and modality-specific formats**.

**ukbeaver** is designed to streamline this process. It provides a convenient interface to:

- 🗂 **Access and organize tabular data** — handle field IDs, instances, and arrays with ease.  
- 🖼 **Work with imaging data** — load and manage different modalities without manual overhead.  
- 🔎 **Query efficiently** — simplify the process of extracting subsets of data for analysis.  
- ⚡ **Integrate with existing workflows** — built to be lightweight, flexible, and compatible with Python data science tools.  

With ukbeaver, researchers can focus on **analysis and discovery**, instead of wrestling with data preprocessing.  

---

## 🚀 Getting Started  

### Installation  
```bash
pip install ukbeaver
```

### Minimal Example  

```python
from ukbeaver.tabular import Phenotype

# init with a UKB-style tab-delimited file
ph = Phenotype("pheno_table.tsv")

# load everything
df, field_map = ph.get_df()

# select specific field IDs
df_50, _ = ph.get_df(fids=["50"])

# select only instance 0
df_i0, _ = ph.get_df(ins=0)
```

---

⚠️ **Note**: Access to UK Biobank data requires an approved UKB project application. `ukbeaver` does not bypass these restrictions; it only assists in handling datasets you are authorized to use.