---

### **docs/methodology.md**

```markdown
# Pipeline Methodology

## **1. Medallion Architecture**

The pipeline follows the Medallion Architecture to structure and process data systematically.

### Bronze Layer
- **Purpose**: Ingest raw data as-is.
- **Sources**:
  - `airbnb.csv` (Airbnb rental data).
  - `rentals.json` (Long-term rental data).
  - `post_codes.geojson` (Geographic dataset).

### Silver Layer
- **Purpose**: Clean and enrich data.
- **Key Transformations**:
  - Standardizing zip codes.
  - Imputing missing review scores.
  - Transforming room types into consistent categories.

### Gold Layer
- **Purpose**: Aggregate data for insights and visualizations.
- **Key Metrics**:
  - Average revenue per postal code.
  - Distribution of room types.

---

## **2. Data Quality Assurance**

### Expectations
1. **Zip Code Validity**
   Validate that all zip codes follow the Dutch format (`#### XX`).

2. **Review Scores**
   Impute missing review scores using averages for the same zip code and room type.

3. **Data Consistency**
   Ensure all room types are mapped to a standardized set of categories.

---

## **3. Geographic Enrichment**
Using the `geo/post_codes.geojson` dataset:
- Match latitude and longitude in the Airbnb dataset to enrich missing zip codes.
- Validate geographic consistency across datasets.

---

## **4. Streaming Ingestion**
- Ingest rental data from `rentals.json` as a streaming source.
- Store data incrementally in Delta Lake for efficient query and updates.

---

## **5. Visualizations**
- **Average Revenue by Postal Code**: Identifies the most profitable areas.
- **Room Type Distribution**: Highlights preferences and trends in property usage.

---

## **6. Future Proofing**
1. **Delta Live Tables**: Automate data quality checks and pipeline workflows.
2. **External APIs**: Use APIs to fill in missing data dynamically.
3. **Real-Time Updates**: Update Gold layer tables as new data streams arrive.
