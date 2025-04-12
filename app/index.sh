#!/bin/bash

echo "Starting full Hadoop MapReduce indexing pipeline for BM25 search"
echo "Activating Python virtual environment"
source .venv/bin/activate

INPUT_ARG="$1"
DEFAULT_HDFS_PATH="/index/data"
HDFS_INPUT_PATH=""
UPLOAD_DIR="/index/data_uploaded"

echo "Handling input: ${INPUT_ARG:-<default>}"

if [[ -z "$INPUT_ARG" ]]; then
    echo "No input provided. Using default HDFS path: $DEFAULT_HDFS_PATH"
    HDFS_INPUT_PATH="$DEFAULT_HDFS_PATH"
elif hdfs dfs -test -e "$INPUT_ARG"; then
    echo "Input exists in HDFS: $INPUT_ARG"
    HDFS_INPUT_PATH="$INPUT_ARG"
elif [[ -f "$INPUT_ARG" ]]; then
    EXT="${INPUT_ARG##*.}"
    if [[ "$EXT" != "csv" ]]; then
        echo "Error: Only .csv files are accepted as input"
        exit 1
    fi
    FILENAME=$(basename "$INPUT_ARG")
    TARGET_PATH="$UPLOAD_DIR/$FILENAME"
    echo "Uploading local .csv file to HDFS: $TARGET_PATH"
    hdfs dfs -mkdir -p "$UPLOAD_DIR"
    hdfs dfs -put -f "$INPUT_ARG" "$TARGET_PATH"
    HDFS_INPUT_PATH="$UPLOAD_DIR"
    echo "File uploaded to: $TARGET_PATH"
elif [[ -d "$INPUT_ARG" ]]; then
    echo "Uploading all .csv files from local directory: $INPUT_ARG"
    FOLDER_NAME=$(basename "$INPUT_ARG")
    TARGET_DIR="$UPLOAD_DIR/$FOLDER_NAME"
    hdfs dfs -mkdir -p "$TARGET_DIR"
    find "$INPUT_ARG" -name '*.csv' -exec hdfs dfs -put -f {} "$TARGET_DIR/" \;
    HDFS_INPUT_PATH="$TARGET_DIR"
    echo "Folder uploaded to: $TARGET_DIR"
else
    echo "Error: Invalid path '$INPUT_ARG'"
    exit 1
fi

# Validate .csv files in HDFS and print them
echo "Files to be processed in HDFS:"
hdfs dfs -ls "$HDFS_INPUT_PATH" | grep ".csv" | while read -r line; do
    FILE=$(echo "$line" | awk '{print $8}')
    echo " - $FILE"
done

# Process all .csv files in the HDFS input directory
INPUT_FILE="$HDFS_INPUT_PATH/*.csv"

# Output paths
TF_INDEX_OUT="/tmp/index1"
BM25_STATS_OUT="/tmp/index2"
VOCAB_OUT="/tmp/vocab"

echo "Running MapReduce Pipeline 1: Term Frequency Indexing"
echo "Extracts term frequencies per document"
echo "Stores in 'term_index' and 'doc_lengths' tables"

hdfs dfs -rm -r -f "$TF_INDEX_OUT"
chmod +x /app/mapreduce/mapper1.py /app/mapreduce/reducer1.py

hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
  -files /app/mapreduce/mapper1.py,/app/mapreduce/reducer1.py \
  -archives /app/.venv.tar.gz#.venv \
  -D mapreduce.reduce.memory.mb=2048 \
  -D mapreduce.reduce.java.opts=-Xmx1800m \
  -mapper ".venv/bin/python mapper1.py" \
  -reducer ".venv/bin/python reducer1.py" \
  -input "$INPUT_FILE" \
  -output "$TF_INDEX_OUT" \
  -numReduceTasks 1 2>&1 | tee mr1.log

echo "Running MapReduce Pipeline 2: BM25 Statistics Generation"
echo "Computes DF and IDF for each term"
echo "Stores TF in 'postings', DF/IDF in 'bm25_stats'"

hdfs dfs -rm -r -f "$BM25_STATS_OUT"
chmod +x /app/mapreduce/mapper2.py /app/mapreduce/reducer2.py

hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
  -files /app/mapreduce/mapper2.py,/app/mapreduce/reducer2.py \
  -archives /app/.venv.tar.gz#.venv \
  -D mapreduce.reduce.memory.mb=2048 \
  -D mapreduce.reduce.java.opts=-Xmx1800m \
  -mapper ".venv/bin/python mapper2.py" \
  -reducer ".venv/bin/python reducer2.py" \
  -input "$TF_INDEX_OUT" \
  -output "$BM25_STATS_OUT" \
  -numReduceTasks 1 2>&1 | tee mr2.log


echo "Running MapReduce Pipeline 3: Vocabulary Extraction"
echo "Extracts unique terms from documents"
echo "Stores terms in 'vocabulary' table"

hdfs dfs -rm -r -f "$VOCAB_OUT"
chmod +x /app/mapreduce/mapper3.py /app/mapreduce/reducer3.py

hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
  -files /app/mapreduce/mapper3.py,/app/mapreduce/reducer3.py \
  -archives /app/.venv.tar.gz#.venv \
  -D mapreduce.reduce.memory.mb=1024 \
  -D mapreduce.reduce.java.opts=-Xmx900m \
  -mapper ".venv/bin/python mapper3.py" \
  -reducer ".venv/bin/python reducer3.py" \
  -input "$INPUT_FILE" \
  -output "$VOCAB_OUT" \
  -numReduceTasks 1 2>&1 | tee mr3.log


echo "Indexing complete. All data stored in Cassandra:"
echo "- term_index (term frequency)"
echo "- doc_lengths (document length)"
echo "- postings (term → doc_id + tf)"
echo "- bm25_stats (df, idf)"
echo "- vocabulary (unique terms)"
