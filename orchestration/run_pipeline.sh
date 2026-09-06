#!/bin/bash

# Fail-fast behavior: stops the script immediately if any command fails
set -e

# ---------------------------------------------------------
# 0. LOGGING SETUP 
# ---------------------------------------------------------
LOG_DIR="/home/student/pipeline_logs"
mkdir -p "$LOG_DIR"
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
LOG_FILE="${LOG_DIR}/run_${TIMESTAMP}.log"

echo "=====================================================" | tee -a "$LOG_FILE"
echo "Olist Pipeline started at $(date)" | tee -a "$LOG_FILE"
echo "=====================================================" | tee -a "$LOG_FILE"

# ---------------------------------------------------------
# 1. ENVIRONMENT SETUP FOR CRON
# ---------------------------------------------------------
unset PYSPARK_DRIVER_PYTHON
unset PYSPARK_DRIVER_PYTHON_OPTS
export PYSPARK_PYTHON=python3
export HADOOP_CONF_DIR=/home/hadoop/hadoop/etc/hadoop
export YARN_CONF_DIR=/home/hadoop/hadoop/etc/hadoop

# Export PATH so cron can find Sqoop, Hadoop, and Spark executables
export PATH="/usr/local/sqoop/sqoop-1.4.7/bin:/usr/local/bin:/home/hadoop/hadoop/bin:$PATH"

export SQOOP_HOME=/usr/local/sqoop/sqoop-1.4.7
export HADOOP_CLASSPATH="$SQOOP_HOME/lib/*:$HADOOP_CLASSPATH"

export HIVE_HOME=/usr/local/hive/hive-3.1.2
export PATH="$HIVE_HOME/bin:$PATH"

# ---------------------------------------------------------
# 2. DATA INGESTION (SQOOP)
# ---------------------------------------------------------
echo "Step 1: Running Sqoop Ingestion for 4 source tables..." | tee -a "$LOG_FILE"

sqoop import \
  --connect jdbc:mysql://localhost:3306/olist \
  --username student \
  --password student \
  --table customers \
  --target-dir /user/hadoop/commerce_storage/customers \
  --delete-target-dir \
  --m 1 >> "$LOG_FILE" 2>&1
echo "Customer Table Imported Successfully" | tee -a "$LOG_FILE"

sqoop import \
  --connect jdbc:mysql://localhost:3306/olist \
  --username student \
  --password student \
  --table sellers \
  --target-dir /user/hadoop/commerce_storage/sellers \
  --delete-target-dir \
  --m 1 >> "$LOG_FILE" 2>&1
echo "Sellers Table Imported Successfully" | tee -a "$LOG_FILE"

sqoop import \
  --connect jdbc:mysql://localhost:3306/olist \
  --username student \
  --password student \
  --table orders \
  --target-dir /user/hadoop/commerce_storage/orders \
  --delete-target-dir \
  --m 1 >> "$LOG_FILE" 2>&1
echo "Orders Table Imported Successfully" | tee -a "$LOG_FILE"

sqoop import \
  --connect jdbc:mysql://localhost:3306/olist \
  --username student \
  --password student \
  --table order_items \
  --target-dir /user/hadoop/commerce_storage/order_items \
  --delete-target-dir \
  --m 1 >> "$LOG_FILE" 2>&1
echo "Order_Items Table Imported Successfully" | tee -a "$LOG_FILE"

echo "All Tables Imported Successfully" | tee -a "$LOG_FILE"

# ---------------------------------------------------------
# 3. DATA PROCESSING (SPARK)
# ---------------------------------------------------------
echo "Step 2: Running Silver transformation..." | tee -a "$LOG_FILE"
spark-submit /home/student/Scripts/silver_transformations.py >> "$LOG_FILE" 2>&1
echo "Silver layer done." | tee -a "$LOG_FILE"

echo "Step 3: Running Gold transformation..." | tee -a "$LOG_FILE"
spark-submit /home/student/Scripts/gold_transformations.py >> "$LOG_FILE" 2>&1
echo "Gold layer done." | tee -a "$LOG_FILE"

# ---------------------------------------------------------
# 4. HIVE EXTERNAL TABLE CREATION
# ---------------------------------------------------------
echo "Step 4: Running Hive External Tables Creation..." | tee -a "$LOG_FILE"
hive -f /home/student/Scripts/hive_setup.hql >> "$LOG_FILE" 2>&1
echo "External table Creation Done." | tee -a "$LOG_FILE"

echo "=====================================================" | tee -a "$LOG_FILE"
echo "===== Pipeline finished successfully at $(date) =====" | tee -a "$LOG_FILE"
echo "=====================================================" | tee -a "$LOG_FILE"
