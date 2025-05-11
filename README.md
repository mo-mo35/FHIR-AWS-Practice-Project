# FHIR-AWS-Practice-Project
## Pipeline

This project builds a cloud-based analytics pipeline using AWS HealthLake, S3, Glue, Athena, and Jupyter. It extracts FHIR data, transforms it with SQL, and analyzes it via Python notebooks.

## Overview

- Export synthetic FHIR data from HealthLake to S3
- Crawl and catalog the data with AWS Glue
- Query with Athena using SQL
- Analyze and visualize results in Jupyter using `awswrangler` and `pandas`

## Setup

### 1. Infrastructure

Deploy AWS resources using the provided script:

```bash
cd infra
chmod +x reproduce_pipeline.sh
./reproduce_pipeline.sh
```
### 2. Queries

From here I wrote some basic queries to create tables I thought may be useful in a real-world setting. These are saved under /athena_queries. 

### 3. Visualizations

I used Jupyter Notebooks to create some prototype visualizations before exporting my tables as CSV files to use PowerBI to create a final dashboard. The notebook and csv files are under /notebooks and the prototype graphs and final dashboard are under /visualizations
