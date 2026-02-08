# Download dataset (Kaggle)

Dataset link:
https://www.kaggle.com/datasets/sowmihari/returns-management

## Manual (recommended)
1) Sign in to Kaggle
2) Download the dataset zip
3) Unzip
4) Put the main CSV into:
   data/raw/returns_management.csv

If the CSV has a different name:
- Either rename it to returns_management.csv
- Or create a .env file and set RAW_CSV_NAME=<your_file.csv>

## Kaggle API option
pip install kaggle
# Put kaggle.json in ~/.kaggle/kaggle.json and chmod 600
kaggle datasets download -d sowmihari/returns-management -p data/raw --unzip
