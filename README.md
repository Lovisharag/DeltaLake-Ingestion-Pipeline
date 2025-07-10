# Delta Lake Data Ingestion Project

This project demonstrates a complete Delta Lake data ingestion pipeline using Databricks.

## 🔧 Features

- ✅ Generate fake data (Name, Address, Email) using Faker.
- ✅ Append data to a Delta table using DeltaTable API.
- ✅ Track table versions using timestamps.
- ✅ Auto-schedule the pipeline to run every 5 minutes.
- ✅ Send HTML email summary after each data append.

## 📦 Technologies Used

- Databricks (Azure)
- Delta Lake
- Apache Spark
- Python (Faker, Pandas)
- SMTP (Gmail)

## 📁 Files Included

- `GenerateFakeDataDelta.ipynb` - Jupyter-style notebook with full pipeline
- `generate_data.py` - Raw Python script
- `table_summary.html` - Sample HTML table sent in email

## 🔄 Scheduling

Pipeline is triggered every 5 minutes using a Databricks Job with a CRON expression.

## 📬 Email Notification

After every append, the latest 5 rows are sent to the configured email in HTML format.

## ✅ Status

**Complete & Working**

