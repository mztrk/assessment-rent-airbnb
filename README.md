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
case-rent-airbnb/
├── data/
│   ├── airbnb.csv
│   ├── rentals.json
│   ├── geo/
│   │   ├── post_codes.geojson
│   │   ├── amsterdam_areas.geojson
├── src/
│   ├── data_pipeline/
│   │   ├── cleaning.py
│   │   ├── enrichment.py
│   │   ├── ingestion.py
│   │   ├── transformations.py
│   │   ├── utils.py
├── dlt_pipeline.py
├── geo_enrichment.py
├── stream_ingestion.py
├── visualization.py
├── tests/
│   ├── test_dlt_pipeline.py
│   ├── test_geo_enrichment.py
│   ├── test_stream_ingestion.py
│   ├── test_visualization.py
├── README.md
├── docs/
│   ├── architecture_diagram.png
│   ├── data_flow_diagram.png
│   ├── methodology.md
