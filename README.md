# E-Commerce Customer Behavior Analytics Using Big Data Batch Processing

## 1. Architecture Diagram

## 2. Project Description

This project focuses on the e-commerce domain and aims to analyze historical customer behavior data from an online multi-category store.

The system was built as a Big Data Batch Processing Pipeline using Hadoop HDFS, Apache Spark, and Streamlit. The pipeline processes large-scale e-commerce event data containing product views, cart additions, and purchase transactions to generate business insights.

The workflow begins by storing the dataset in HDFS. Apache Spark then performs batch processing to aggregate and analyze customer behavior, product performance and sales metrics. The processed results are saved as output files and visualized through a Streamlit dashboard, allowing users to explore the findings more easily.

The system will transforms raw e-commerce event logs into meaningful business insights such as revenue contribution, customer engagement patterns and conversion performance.

## 3. Problem Statement

E-commerce platforms generate large volumes of customer interaction data, including product views, cart additions, and purchases. Analyzing these historical records can help businesses understand customer behavior, identify high-performing products, and evaluate sales performance.

This project aims to build a batch processing pipeline that analyzes historical e-commerce behavior data using HDFS and Spark SQL. The processed analytical tables are stored back into HDFS and visualized through a Streamlit dashboard to support business decision-making.
The system is designed to answer the following business questions:

**Batch Insights**

**1. Which product categories generate the highest purchase revenue?**

This helps businesses identify their most profitable categories and prioritize inventory, promotions, and marketing efforts.

**2. Which brands contribute the most to total purchase revenue?**

This allows businesses to understand which brands are driving customer purchases and revenue growth

**3. What are the most viewed product categories?**

This helps identify which categories attract the most customer attention, even if they do not necessarily lead to purchases.

**4. What is the conversion rate from product views to purchases?**

This metric measures how effectively customer interest is converted into actual transactions.

**5. How do customer actions progress across the funnel: view → cart → purchase?**

This reveals where customers drop off in the purchasing journey and helps businesses optimize each stage of the funnel.

## 4. Dataset Description

This project uses the E-Commerce Behavior Data from Multi-Category Store dataset available on Kaggle.

Dataset Source

[Kaggle Dataset - E-Commerce Behavior Data from Multi-Category Store](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store?select=2019-Oct.csv)

The dataset contains historical customer interaction records collected from a large online store. Each record represents a customer event, such as viewing a product, adding it to a cart or completing a purchase.

For this project, a sampled version of the dataset is used to ensure efficient processing on a local machine.

**Dataset Characteristics**
- Domain: E-Commerce
- Data Type: Customer Behavior Events
- Data Explorer: 2019-Oct.csv
- Format: CSV
- Original Dataset Size: 5.67 GB (More than 286 million events across multiple months)

**Key Fields Used**

| Field | Description |
|---------|-------------|
| event_time | Timestamp when the event occurred |
| event_type | Type of customer action (view, cart, purchase) |
| product_id | Unique product identifier |
| category_id | Product category identifier |
| category_code | Product category name |
| brand | Product brand |
| price | Product price |
| user_id | Unique customer identifier |
| user_session | Session identifier for customer activity |

## 5. How to Run

### Prerequisites
Before running this project, make sure this applications are installed : 
- Python 3.10 or above
- Docker Dekstop
- Git
- Visual Studio Code 

Check Python : 

```bash
python --version
```

Or : 

```bash
python3 --version
```

### Windows Users

Before editing or committing files, make sure all project files use LF instead of CRLF.

In Visual Studio Code:

1. Open any file.
2. Look at the bottom-right corner.
3. If it shows `CRLF`, click it.
4. Select `LF`.
5. Save the file.

Recommended files:

- docker-compose.yml
- Dockerfile
- *.py
- *.sh
- README.md

Using LF prevents compatibility issues when running Hadoop, Spark, and Docker containers.

### STEP 1 - Clone the Repository
Open Terminal (Mac/Linux) or PowerShell (Windows).


Run : 

```bash
git clone https://github.com/KeweKiwi/BDP-FinalProject-Group-1
```

Verify the files exist: 

```bash
ls
```

Expected Output:

```bash
docker-compose.yml
README.md
data/
jobs/
dashboard/
```

Once the repository is pulled, create new branch

```bash
git checkout -b your-name
```

Make sure you're in the project directory:

```bash
cd bdp-finalproject-group-1
source venv/bin/activate
```

Expected Output:

```bash
Terminal berada di folder bdp_finalproject dan prompt virtual environment aktif, biasanya ditandai dengan prefix seperti (venv).
```

### STEP 2 - Start Hadoop and Spark Services

Make sure Docker Desktop is already running.

Start all services:

```bash
docker compose up -d
```

Expected output:

```bash
Docker compose starts three containers:
bdp-namenode
bdp-datanode
bdp-spark
```

Wait 1–2 minutes until all containers finish starting.

**Open new terminal**

Check container status:

```bash
docker ps
```

Expected output:

```bash
bdp-namenode
bdp-datanode
bdp-spark
```

All containers should show:

```bash
STATUS: Up
```

### STEP 3 - Check DataNode Health

Check DataNode connect to NameNode

```bash
docker exec -it bdp-namenode hdfs dfsadmin -report
```

Expected Output:

```bash
live datanodes (1)
```

### STEP 4 - Open HDFS Web UI

Browser:

```bash
http://localhost:9870
```

Expected Output:

```bash
Hadoop NameNode UI Status = active
```

### STEP 5 - Verify HDFS is Running

Open the Hadoop NameNode container: 

```bash
docker exec -it namenode bash
```

Check HDFS:

```bash
hdfs dfs -ls /
```

If HDFS is working, you should see folders such as:

```bash
/tmp
/user
```

Exit:
```bash
exit
```

### STEP 6 - Download the Dataset

Download the dataset from Kaggle: [eCommerce behavior data](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store?select=2019-Oct.csv)

Download: 

```bash
2019-Oct.csv
```

Place the file inside:

```bash
data/
```

Example:

```bash
project/
└── data/
    └── 2019-Oct.csv
```

### STEP 7 - Copy Dataset to NameNode Container

```bash
docker cp data/2019-Oct.csv bdp-namenode:/2019-Oct.csv
docker exec -it bdp-namenode ls -lh /2019-Oct.csv
```

Expected Output:
```bash
File /2019-Oct.csv 5.3 GB
```

### STEP 8 - Create a Raw Folder in HDFS

Create a folder for this project: 

```bash
docker exec -it bdp-namenode hdfs dfs -mkdir -p /user/bdp/raw
```

Verify:

```bash
docker exec -it namenode hdfs dfs -ls /user
```

Expected Output:

```bash
bdp/raw
```

### STEP 9 - Upload Dataset into HDFS

Upload to HDFS:

```bash
docker exec -it bdp-namenode hdfs dfs -put -f /2019-Oct.csv /user/bdp/raw/2019-Oct.csv
```

Verify Upload:

```bash
docker exec -it bdp-namenode hdfs dfs -ls -h /user/bdp/raw
```
Expected:

```bash
Found 1 items
-rw-r--r--   3 root supergroup      5.3 G ... /user/bdp/raw/2019-Oct.csv
```

### STEP 10 - Verify Spark Container

Run:

```bash
docker exec -it bdp-spark /opt/spark/bin/spark-submit --version
```
Expected: 

```bash
Spark version 3.5.1
```

This confirms the dataset is stored successfully inside HDFS

### STEP 11 - Run Spark SQL Batch Job

Spark runs inside the bdp-spark container so it shares the same Docker network as HDFS. This is why the HDFS path in the script uses hdfs://namenode:9000 instead of localhost.

```bash
docker exec -it bdp-spark /opt/spark/bin/spark-submit /app/jobs/batch_hdfs_sql_analysis.py
```

Expected output:

```bash
Spark job starts reading the raw dataset from HDFS, creates temporary views,
executes SQL queries, saves processed tables back to HDFS,
and generates local CSV files for the dashboard.
```

This confirms Spark successfully connected to HDFS, processed the dataset, and wrote the output to the designated locations.

### STEP 12 - Expected Spark Console Output

Expected:

```bash
=== Dataset Schema ===
root
 |-- event_time: timestamp
 |-- event_type: string
 |-- product_id: integer
 |-- category_id: long
 |-- category_code: string
 |-- brand: string
 |-- price: double
 |-- user_id: integer
 |-- user_session: string
 
=== Event Type Count Table ===
+----------+------------+
|event_type|total_events|
+----------+------------+
|view      |40779399    |
|cart      |926516      |
|purchase  |742849      |
+----------+------------+
 
=== Query 1: Category Revenue Table ===
Top category:
electronics.smartphone
total_revenue = $157,049,623.37
purchase_count = 338,018
 
=== Query 2: Brand Revenue Table ===
Top brand:
apple
total_revenue = $111,209,268.82
purchase_count = 142,873
 
=== Query 3: Most Viewed Category Table ===
Top viewed category:
electronics.smartphone
view_count = 10,619,448
 
=== Query 4: Conversion Rate Table ===
+-----------+---------------+--------------------------+
|total_views|total_purchases|conversion_rate_percentage|
+-----------+---------------+--------------------------+
|40779399   |742849         |1.82                      |
+-----------+---------------+--------------------------+
 
=== Query 5: Funnel Summary Table ===
view      40,779,399
cart         926,516
purchase     742,849
 
=== Batch SQL Processing Completed ===
```


### STEP 13 - Verify Processed Tables in HDFS

This command proves that Spark SQL successfully created analytical tables and saved them back to HDFS.

```bash
docker exec -it bdp-namenode hdfs dfs -ls -h /user/bdp/processed
```

Expected output: 

```bash
event_type_count_table
category_revenue_table
brand_revenue_table
most_viewed_category_table
conversion_rate_table
funnel_summary_table
```

### STEP 14 - Verify Local Dashboard CSV Output

Local CSV is created so that Streamlit running on the host can read the analysis results easily.

```bash
ls -lh output/dashboard
```

Expected Output:

```bash
event_type_count_table.csv
category_revenue_table.csv
brand_revenue_table.csv
most_viewed_category_table.csv
conversion_rate_table.csv
funnel_summary_table.csv
```

### STEP 15 - Requirements

Dashboard/requirements.txt should contain:

```bash
streamlit
pandas
plotly
```

This file ensures that the dashboard can be run in the new environment after the repository is cloned.

### STEP 16 - Run Streamlit Dashboard

Run the Streamlit dashboard after the Spark SQL batch processing is complete and the output files inside output/dashboard/*.csv are available.

Install the dashboard dependencies:

```bash
pip install -r dashboard/requirements.txt
```

Launch the dashboard: 

```bash
streamlit run dashboard/app.py
```

Open the dashboard in your browser:

```bash
http://localhost:8501
```

Expected Output:
FOTOOOO!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

### STEP 14 - Stop the Project

To stop the dashboard:

```bash
CTRL + C
```

To stop all Docker Services:

```bash
docker compose down
```

Verify:

```bash
docker ps
```

No project containers should remain running.


## 6. Expected Output

## 7. Findings & Conclusion

## 8. Known Limitations
