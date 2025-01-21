# Netherlands Rental & Airbnb Data Pipeline

## **Overview**

This project is a data engineering pipeline designed to analyze rental and Airbnb data for Amsterdam properties. By combining multiple datasets and leveraging the Medallion Architecture, the pipeline provides insights into potential revenue by postal code and rental type. The pipeline also handles real-time data ingestion and enriches datasets using geographic information.

---

## **Why This Pipeline?**

### **Purpose**
The pipeline addresses the needs of real estate investors who want to identify profitable areas in Amsterdam for long-term or short-term property rentals. By unifying Airbnb, rental, and geographic datasets, it provides a comprehensive analysis that answers key business questions:
- Which postal codes generate the highest rental revenue?
- Should properties in specific postal codes be rented short-term (via Airbnb) or long-term?

### **Challenges Addressed**
1. **Data Quality**
   - Zip codes in Airbnb data are incomplete or messy. The pipeline enriches missing or malformed zip codes using latitude and longitude.
   - Missing review scores are imputed based on average scores for a specific postal code and room type.

2. **Scalability**
   - The pipeline processes large datasets using Apache Spark, enabling high performance for current and future data volumes.
   - Streaming capabilities allow real-time ingestion of rental data.

3. **Actionable Insights**
   - Visualizations highlight average revenue by postal code and room type distribution to support data-driven decisions.
   - Enriched datasets serve as a foundation for advanced modeling and forecasting.

4. **Maintainability**
   - Modular design ensures that individual pipeline components (e.g., data ingestion, geographic enrichment) can be updated independently.
   - Built using Delta Lake for versioning and efficient data storage.

---

## **Pipeline Features**

### **1. Modular Design**
The pipeline is divided into the following stages:
- **Bronze Layer**: Raw data ingestion.
- **Silver Layer**: Cleaned and enriched datasets.
- **Gold Layer**: Aggregated insights ready for analysis or visualization.

### **2. Real-Time Data Ingestion**
Rental data is ingested in a streaming fashion to ensure the pipeline can handle updates as they arrive.

### **3. Geographic Enrichment**
Missing zip codes in Airbnb data are enriched using a geojson dataset of Dutch postal codes. This ensures accurate spatial analysis.

### **4. Data Quality Enforcement**
- Missing values for review scores are imputed using averages specific to postal codes and room types.
- Data validation ensures consistent formatting for fields like room type and zip code.

### **5. Insights and Visualization**
- Generates visualizations to analyze average revenue by postal code.
- Highlights distribution of room types across Amsterdam.

---

## **Directory Structure**

```plaintext
assessment-rent-airbnb/
│
├── .github/workflows/                # GitHub Actions workflows for CI/CD
│   └── ci_cd_pipeline.yml            # CI/CD pipeline configuration
│
├── data/                             # Dataset folder
│   ├── geo/                          # GeoJSON and related files
│   │   ├── amsterdam_areas.geojson   # GeoJSON file for Amsterdam areas
│   │   ├── post_codes.geojson        # GeoJSON file for postcodes
│   ├── airbnb.csv                    # Airbnb data in CSV format
│   ├── rentals.json                  # Rental data in JSON format
│
├── docs/                             # Documentation and diagrams
│   ├── data_flow_diagram.png         # Data flow diagram
│   ├── cicd_diagram.png              # CI/CD process diagram
│   └── methodology.md                # Methodology explanation
│
├── notebooks/                        # Jupyter notebooks for analysis
│   └── pipeline.ipynb                # Notebook showcasing the data pipeline
│
├── scratch/                          # Temporary or exploratory scripts/notebooks
│   └── exploratory_data_analysis.ipynb
│
├── src/                              # Source code for the data pipeline
│   └── data_pipeline/
│       ├── __init__.py               # Package initialization
│       ├── cleaning.py               # Data cleaning logic
│       ├── dlt_pipeline.py           # Delta Live Tables (DLT) pipeline
│       ├── enrichment.py             # Enrichment functions
│       ├── geo_enrichment.py         # Geospatial enrichment logic
│       ├── ingestion.py              # Data ingestion logic
│       ├── stream_ingestion.py       # Streaming data ingestion
│       ├── transformations.py        # Transformation functions
│       ├── utils.py                  # Utility functions
│       └── visualization.py          # Visualization functions
│
├── tests/                            # Unit tests for the pipeline
│   ├── conftest.py                   # Shared fixtures for tests
│   ├── test_cleaning.py              # Tests for cleaning module
│   ├── test_enrichment.py            # Tests for enrichment module
│   ├── test_geo_enrichment.py        # Tests for geospatial enrichment
│   ├── test_ingestion.py             # Tests for ingestion module
│   ├── test_transformations.py       # Tests for transformation functions
│   ├── test_visualization.py         # Tests for visualization module
│
├── venv/                             # Virtual environment folder (optional)
├── .coverage                         # Coverage report
├── .gitignore                        # Ignored files in Git
├── .pre-commit-config.yaml           # Pre-commit hooks configuration
├── generate_diagrams.py              # Script to generate diagrams
├── LICENSE                           # License for the project
├── main.py                           # Main script to execute the pipeline
├── README.md                         # Project documentation (this file)
└── requirements.txt                  # Python dependencies
