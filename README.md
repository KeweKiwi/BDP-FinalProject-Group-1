# E-Commerce Customer Behavior Analytics Using Big Data Batch Processing

## 1. Architecture Diagram

## 2. Project Description

## 3. Problem Statement

## 4. Dataset Description

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

Move into the project folder:

```bash
cd bdp-final-project
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

