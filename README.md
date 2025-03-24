# Marketing Analytics Pipeline

## 📌 Description
This repository contains an end-to-end data analytics pipeline for digital marketing campaigns, leveraging AWS and Airflow (MWAA) for orchestration. The project enables data integration, storage, transformation, and visualization from various advertising platforms such as Meta, Google Ads, DV360, Snapchat, and TikTok.

## 📂 Repository Structure
```
marketing-analytics-pipeline/
│── dags/                     # Airflow (MWAA) DAGs for orchestration
│── data/                     # Sample or test data
│── scripts/                   # Data generation and ETL scripts
│── config/                    # Configurations and credentials (DO NOT include real credentials)
│── infrastructure/            # Infrastructure as Code (IaC)
│── notebooks/                 # Jupyter Notebooks for exploratory analysis
│── sql/                       # SQL queries for analysis
│── reports/                   # Generated reports and dashboards
│── visualization/             # Visualization scripts with QuickSight or Power BI
│── docs/                      # Project documentation
│── tests/                     # Unit and integration tests
│── .gitignore                 # Files to ignore in Git
│── requirements.txt           # Python dependencies
│── Dockerfile                 # Docker setup
│── setup.py                   # Custom package installation
```

## 🚀 Technologies Used
- **AWS Lambda, S3, Glue, Athena** for data storage and processing.
- **Airflow (MWAA)** for workflow orchestration.
- **Redshift, Snowflake** for structured data storage.
- **Power BI, QuickSight** for visualization and data analysis.
- **APIs from Meta, Google Ads, DV360, Snapchat, TikTok** for data ingestion.
- **Python, SQL** for data manipulation and transformation.

## 📊 Workflow
1. **Data Ingestion**: Fetching campaign data via APIs.
2. **Storage**: Saving data in S3 and analytical databases.
3. **Transformation**: Cleaning and structuring using Glue/Athena.
4. **Loading**: Inserting data into Redshift/Snowflake for analysis.
5. **Visualization**: Creating dashboards in Power BI/QuickSight.

## 🛠 Installation & Setup
### 📌 Prerequisites
- Python 3.8+
- AWS CLI configured
- Docker (optional, for local development)
- Airflow (if running locally instead of MWAA)

### 📥 Installation
```bash
# Clone the repository
git clone https://github.com/your_user/marketing-analytics-pipeline.git
cd marketing-analytics-pipeline

# Install dependencies
pip install -r requirements.txt
```

### 🚀 Running Locally
```bash
# Run a test DAG in Airflow
python scripts/generate_meta_data.py
```

## 🤝 Contributions
Contributions are welcome! To propose changes, create a *pull request* with a detailed description.

## 📜 License
This project is licensed under MIT. See [LICENSE](LICENSE) for more details.

---
🚀 Ready to turn data into strategic decisions! 🚀

´´´bash
instalar docker
docker ps -al
docker build .
docker image ls 
sh init_docker.sh
´´´
