#!/bin/bash
export HADOOP_CONF_DIR="/home/hadoop/hadoop/etc/hadoop"
export YARN_CONF_DIR="$HADOOP_CONF_DIR"
export PATH="/usr/local/sqoop/sqoop-1.4.7/bin:/usr/local/bin:/home/hadoop/hadoop/bin:$PATH"
set -e   # stop immediately on first failure, don't silently continue with bad data

LOG_DIR="/home/student/pipeline_logs"
LOG_FILE="$LOG_DIR/run_$(date +%Y%m%d_%H%M%S).log"
mkdir -p "$LOG_DIR"

echo "===== Pipeline started: $(date) =====" | tee -a "$LOG_FILE"

# --- Step 1: Sqoop imports (4 tables) ---
for table in customers sellers orders order_items; do
    echo "Importing $table..." | tee -a "$LOG_FILE"
    sqoop import \
        --connect jdbc:mysql://localhost:3306/olist \
        --username student --password student \
        --table "$table" \
        --target-dir "/user/hadoop/commerce_storage/$table" \
        --delete-target-dir \
        --as-parquetfile \
        --m 1 >> "$LOG_FILE" 2>&1
    echo "$table import done." | tee -a "$LOG_FILE"
done

# --- Step 2: Silver layer ---
echo "Running Silver transformation..." | tee -a "$LOG_FILE"
spark-submit /home/student/Scripts/silver_transformations.py >> "$LOG_FILE" 2>&1
echo "Silver layer done." | tee -a "$LOG_FILE"

# --- Step 3: Gold layer ---
echo "Running Gold transformation..." | tee -a "$LOG_FILE"
spark-submit /home/student/Scripts/gold_transformations.py >> "$LOG_FILE" 2>&1
echo "Gold layer done." | tee -a "$LOG_FILE"


echo "===== Pipeline finished successfully: $(date) =====" | tee -a "$LOG_FILE"
