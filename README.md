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
import ukbeaver as ub

# Load a tabular dataset
df = ub.load_tabular("ukb12345.csv")

# Query specific fields by field ID
subset = ub.query(df, fields=[50, 21001, 3063], instance=2)

# Load imaging data (e.g., brain MRI)
mri_img = ub.load_image("ukb_mri/12345/T1.nii.gz")

print(subset.head())
print(mri_img.shape)
```

---

⚠️ **Note**: Access to UK Biobank data requires an approved UKB project application. `ukbeaver` does not bypass these restrictions; it only assists in handling datasets you are authorized to use.