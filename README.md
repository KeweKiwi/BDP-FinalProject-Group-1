# E-Commerce Customer Behavior Analytics Using Big Data Batch Processing

## 1. Architecture Diagram

## 2. Project Description

This project focuses on the e-commerce domain and aims to analyze historical customer behavior data from an online multi-category store.

The system was built as a Big Data Batch Processing Pipeline using Hadoop HDFS, Apache Spark, and Streamlit. The pipeline processes large-scale e-commerce event data containing product views, cart additions, and purchase transactions to generate business insights.

The workflow begins by storing the dataset in HDFS. Apache Spark then performs batch processing to aggregate and analyze customer behavior, product performance and sales metrics. The processed results are saved as output files and visualized through a Streamlit dashboard, allowing users to explore the findings more easily.

The system will transforms raw e-commerce event logs into meaningful business insights such as revenue contribution, customer engagement patterns and conversion performance.

## 3. Problem Statement

E-commerce businesses collect massive amounts of customers interaction data every day. However, raw event data alone provides little value unless it is transformed into actionable insights that support business decision making.

This project aims to answer the following business questions:

**Batch Insights**

**1. Which product categories generate the highest purchase revenue?**

This helps businesses identify their most profitable categories and prioritize inventory, promotions and marketing efforts

**2. Which brands contribute the most to overall sales revenue?**

This allows businesses to understand which brands are driving customer purchases and revenue growth

**3. Which product categories receive the highest number of views?**

This helps identify products that attract customer attention, even if they do not necessarily lead to purchases.

**4. What is the conversion rate from product views to purchases?**

This metric measures how effectively customer interest is converted into actual transactions.

**5. What customer behavior patterns can be observed from historical e-commerce activities?**

By analyzing views, cart additions, and purchases, businesses can better understand the customer purchasing journey.

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
- Sample Dataset Size: 100,000 records (used in this project)

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

### STEP 2 - Start Hadoop and Spark Services

Make sure Docker Desktop is already running.

Start all services:

```bash
docker compose up -d
```

Wait 1–2 minutes until all containers finish starting.

Check container status:

```bash
docker ps
```

Expected output:

```bash
namenode
datanode
spark-master
spark-worker
```

All containers should show:

```bash
STATUS: Up
```

### STEP 3 - Verify HDFS is Running

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

### STEP 4 - Create a Folder in HDFS

Create a folder for this project: 

```bash
docker exec -it namenode hdfs dfs -mkdir -p /user/ecommerce
```

Verify:

```bash
docker exec -it namenode hdfs dfs -ls /user
```

Expected Output:

```bash
ecommerce
```

### STEP 5 - Download the Dataset

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

### STEP 6 - Create a Smaller Sample Dataset

The original dataset is very large.

Create a sample containing 100,000 rows:

Mac/Linux: 

```bash
head -100001 data/2019-Oct.csv > data/sample_data.csv
```

Windows PowerShell:

```bash
Get-Content data\2019-Oct.csv -TotalCount 100001 | Set-Content data\sample_data.csv
```

Verify: 

```bash
ls data
```

Expected:

```bash
2019-Oct.csv
sample_data.csv
```

### STEP 7 - Upload Dataset into HDFS

Copy the dataset into the NameNode container:

```bash
docker cp data/sample_data.csv namenode:/tmp/sample_data.csv
```

Upload to HDFS:

```bash
docker exec -it namenode hdfs dfs -put -f /tmp/sample_data.csv /user/ecommerce/
```

Verify Upload:

```bash
docker exec -it namenode hdfs dfs -ls /user/ecommerce
```
Expected:

```bash
sample_data.csv
```

### STEP 8 - Verify Dataset exists in HDFS

Run:

```bash
docker exec -it namenode hdfs dfs -du -h /user/ecommerce/sample_data.csv
```
Expected: 

```bash
XX MB
```

This confirms the dataset is stored successfully inside HDFS

### STEP 9 - Run Spark Batch Analysis

Open a new terminal:

```bash
python jobs/batch_analysis.py
```

The Spark job will:
1. Read dataset from HDFS
2. Analyze customer behavior
3. Calculate revenue metrics
4. Generate output files

Expected output:

```bash
Loading dataset...

Calculating event statistics...

Calculating top categories...

Calculating top brands...

Calculating conversion rate...

Job completed successfully.
```

### STEP 10 - Verify Analysis Output

Check the output folder:

```bash
ls output/
```

Expected:

```bash
event_type_count.csv
top_categories.csv
top_brands.csv
conversion_rate.csv
summary_metrics.csv
```

These files contain the processed results from Spark.

### STEP 11 - Start the Dashboard

Open a new terminal.

Run:

```bash
streamlit run dashboard/app.py
```

Wait until Streamlit starts.

Expected output: 

```bash
Local URL: http://localhost:8501
```

### STEP 12 - Open Dashboard

Open your browser.

Go to:

```bash
http://localhost:8501
```

### STEP 13 - Stop the Project

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
