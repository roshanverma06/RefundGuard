# RefundGuard ETL - Amazon-style Return & Tampering Analytics

## Story
I once received an online order where the original product appeared to be replaced with a used item.
This project models how an Amazon-scale platform could detect suspicious return/replacement/tampering patterns using data.

## Dataset (external)
Kaggle: https://www.kaggle.com/datasets/sowmihari/returns-management

## Outputs
- SQLite warehouse: refund_guard.db
- Curated CSV exports for Tableau: data/exports/
- Report: reports/summary.md

## Run
python3 -m refund_guard.cli run-all
