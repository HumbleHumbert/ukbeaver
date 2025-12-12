# 
rule get_data:
    output:
        "resources/data/cars.tsv"
    conda:
        "envs/download.yaml"
    log:
        notebook="logs/get_data.ipynb"
    notebook:
        "notebooks/get_data.py.ipynb"
