#!/bin/bash


echo "This script runs Hadoop MapReduce to index documents using mapper1/reducer1"
echo "Activating venv"

source .venv/bin/activate

# Default input if none is provided
INPUT_FILE=${1:-/index/data/part-00000-*}
OUTPUT_DIR="/tmp/index1"

echo "Using input: $INPUT_FILE"
echo "Output will be in: $OUTPUT_DIR"

# Clean up previous output if it exists
hdfs dfs -rm -r -f "$OUTPUT_DIR"

chmod +x /app/mapreduce/mapper1.py
chmod +x /app/mapreduce/reducer1.py

hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
  -files /app/mapreduce/mapper1.py,/app/mapreduce/reducer1.py \
  -archives /app/.venv.tar.gz#.venv \
  -D mapreduce.reduce.memory.mb=2048 \
  -D mapreduce.reduce.java.opts=-Xmx1800m \
  -mapper ".venv/bin/python mapper1.py" \
  -reducer ".venv/bin/python reducer1.py" \
  -input "$INPUT_FILE" \
  -output "$OUTPUT_DIR" \
  -numReduceTasks 1 2>&1 | tee mr_job.log

echo "Indexing complete. Data should now be in Cassandra."
