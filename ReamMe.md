# Microsoft Fabric SCD Type 2 Implementation Using PySpark and Delta MERGE

## 📖 Overview

This project demonstrates how to implement **Slowly Changing Dimension (SCD) Type 2** in **Microsoft Fabric Lakehouse** using **PySpark**, **Delta Lake**, **Delta MERGE**, and **Watermark-Based Incremental Loading**.

The solution captures historical changes in customer records while preserving previous versions for auditing and reporting purposes.

---

## 🎯 Business Requirement

A source system continuously sends customer data updates.

The solution must:

- Process only incremental records
- Detect changed customer attributes
- Preserve historical versions of records
- Mark previous versions as inactive
- Create new active versions
- Maintain a watermark table for incremental processing

---

## 🛠️ Technologies Used

| Technology | Purpose |
|------------|---------|
| Microsoft Fabric | Data Engineering Platform |
| Lakehouse | Data Storage Layer |
| PySpark | Data Processing |
| Delta Lake | ACID Transactions |
| Delta MERGE | SCD Type 2 Implementation |
| SQL | Data Manipulation |
| Watermark Table | Incremental Loading |
| Fabric Notebook | Development Environment |

---

## 🏗️ Architecture

```text
Source CSV Files
        │
        ▼
Read Watermark
        │
        ▼
Incremental Filter
(last_modified > watermark)
        │
        ▼
Identify Changed Records
        │
        ▼
Create Staging View
        │
        ▼
Delta MERGE
        │
        ▼
Dimension Table (SCD Type 2)
        │
        ▼
Update Watermark
```

---

## 📂 Project Structure

```text
fabric-scd2-incremental-load
│
├── Notebooks
│   ├── Create_Source_Data
│   ├── Read_Watermark
│   ├── Incremental_Load
│   ├── Create_Staging_View
│   ├── SCD2_Merge
│   └── Update_Watermark
│
├── Data
│   ├── customer_initial.csv
│   └── customer_incremental.csv
│
└── README.md
```

---

## 📊 Source Data

### Initial Load

| CustomerID | CustomerName | City | Country |
|------------|-------------|------|----------|
| 101 | John | New York | USA |
| 102 | Mary | Chicago | USA |
| 103 | David | Boston | USA |

---

### Incremental Load

| CustomerID | CustomerName | City | Country |
|------------|-------------|------|----------|
| 101 | John | Chicago | USA |
| 104 | Ahmed | Dubai | UAE |

Customer **101** moved from **New York** to **Chicago**.

Customer **104** is a new customer.

---

## 🗄️ Watermark Table

The watermark table stores the latest processed timestamp.

### Create Watermark Table

```sql
CREATE TABLE control.water
(
    table_name STRING,
    last_update TIMESTAMP
);
```

### Initial Watermark Record

```sql
INSERT INTO control.water
VALUES
(
    'customer',
    TIMESTAMP('1900-01-01 00:00:00')
);
```

---

## 📥 Read Watermark

```python
watermark_date = spark.sql("""
SELECT last_update
FROM control.water
WHERE table_name='customer'
""").collect()[0][0]
```

---

## 📈 Incremental Load Logic

Only newly arrived or modified records are extracted.

```python
incremental_df = source_df.filter(
    source_df.last_modified > watermark_date
)
```

---

## 🏛️ Dimension Table Structure

```sql
CREATE TABLE sil.dim_customer
(
    CustomerID INT,
    CustomerName STRING,
    City STRING,
    Country STRING,
    effective_start_date TIMESTAMP,
    effective_end_date TIMESTAMP,
    is_flag INT,
    last_modified TIMESTAMP
);
```

---

## 🔄 SCD Type 2 Logic

### Active Record

```text
Is_Flag = 1
```

### Historical Record

```text
Is_Flag = 0
```

### Record Expiration

```text
effective_end_date = CURRENT_TIMESTAMP()
```

### New Version Creation

```text
effective_start_date = CURRENT_TIMESTAMP()
```

---

## ⚙️ Create Staging View

```sql
CREATE OR REPLACE TEMP VIEW stg_source AS

SELECT
    CustomerID AS mergeKey,
    CustomerID,
    CustomerName,
    City,
    Country,
    CURRENT_TIMESTAMP() AS effective_start_date,
    TIMESTAMP('9999-12-31 00:00:00') AS effective_end_date,
    1 AS is_flag,
    CURRENT_TIMESTAMP() AS last_modified

FROM inc_customer;
```

---

## 🚀 Delta MERGE Statement

```sql
MERGE INTO sil.dim_customer AS t
USING stg_source AS s

ON t.CustomerID = s.mergeKey
AND t.is_flag = 1

WHEN MATCHED
AND (
       t.CustomerName <> s.CustomerName
    OR t.City <> s.City
    OR t.Country <> s.Country
)
THEN UPDATE SET
    t.effective_end_date = CURRENT_TIMESTAMP(),
    t.is_flag = 0

WHEN NOT MATCHED
THEN INSERT
(
    CustomerID,
    CustomerName,
    City,
    Country,
    effective_start_date,
    effective_end_date,
    is_flag,
    last_modified
)
VALUES
(
    s.CustomerID,
    s.CustomerName,
    s.City,
    s.Country,
    s.effective_start_date,
    s.effective_end_date,
    s.is_flag,
    s.last_modified
);
```

---

## 📌 Watermark Update

After successful processing, update the watermark.

```python
spark.sql(f"""
UPDATE control.water
SET last_update = TIMESTAMP('{max_date}')
WHERE table_name='customer'
""")
```

---

## ✅ Example Output

### Before Change

| CustomerID | CustomerName | City | Is_Flag |
|------------|-------------|------|---------|
| 101 | John | New York | 1 |

---

### After Change

| CustomerID | CustomerName | City | Is_Flag |
|------------|-------------|------|---------|
| 101 | John | New York | 0 |
| 101 | John | Chicago | 1 |

---

## 🔍 Key Concepts Demonstrated

- Incremental Data Loading
- Watermark-Based Processing
- Delta Lake MERGE
- Slowly Changing Dimension Type 2
- Historical Data Tracking
- Microsoft Fabric Lakehouse
- PySpark Transformations
- SQL-Based Data Processing
- Delta Table Management

---

## 📈 Benefits

- Preserves historical records
- Supports auditing requirements
- Eliminates full reloads
- Improves performance
- Enables point-in-time analysis
- Scalable for large datasets
- Production-ready design

---

## 🔮 Future Enhancements

- Structured Streaming
- Auto Loader Style Ingestion
- Metadata-Driven Framework
- CI/CD Integration
- GitHub Actions Deployment
- Azure DevOps Pipeline Deployment
- Data Quality Validation Rules
- Error Handling Framework

---

## 👨‍💻 Author

**Syed Syed**

**Skills:** Microsoft Fabric, Power BI, SQL, PySpark, Data Engineering

---

## ⭐ Project Highlights

✅ Microsoft Fabric Lakehouse

✅ PySpark Implementation

✅ Delta Lake MERGE

✅ Watermark Incremental Load

✅ Slowly Changing Dimension Type 2 (SCD2)

✅ Historical Data Preservation

✅ Production-Ready Data Engineering Pattern



