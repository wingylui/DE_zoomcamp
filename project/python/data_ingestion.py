# %%
import pandas as pd

# %%

# variables
year = {{ vars.year }}

# %%
# dataset location
dataset_url = {
                "birth-rate"        : "https://github.com/wingylui/DE_zoomcamp/raw/refs/heads/main/project/dataset/birth-rate.csv.gz",
                "life-expectancy"   : "https://github.com/wingylui/DE_zoomcamp/raw/refs/heads/main/project/dataset/life-expectancy.csv.gz",
                "migrant-total"     : "https://github.com/wingylui/DE_zoomcamp/raw/refs/heads/main/project/dataset/migrant-total.csv.gz",
                "refugee-population":"https://github.com/wingylui/DE_zoomcamp/raw/refs/heads/main/project/dataset/refugee-population.csv.gz"
}

# %%
for file_name in dataset_url.keys():
    url = dataset_url[file_name]
    
    # reading each the dataset 
    df = pd.read_csv(url, compression = "gzip")
    filter_df = df.loc[(df["Year"] == year) & (df["Code"].notna()), :] # filter the year thats required

    if len(filter_df) > 0 :
        filter_df.to_csv(f"{file_name}_{year}.csv") # output csv files which ready to upload
    else:
        pass


