#!/bin/bash

echo "This script runs a BM25 search query using Spark RDD and Cassandra"

# source .venv/bin/activate

# # Python of the driver
# export PYSPARK_DRIVER_PYTHON=$(which python)

# # Python of the executor
# export PYSPARK_PYTHON=./.venv/bin/python

# if ! jps | grep -q "NodeManager"; then
#   echo "NodeManager is not running. Attempting to start it..."
#   yarn --daemon start nodemanager
#   sleep 5
# else
#   echo "NodeManager is already running."
# fi

# # Check if query is provided
# if [[ $# -lt 1 ]]; then
#   echo "Usage: bash search.sh \"your query here\""
#   exit 1
# fi

# QUERY="$1"

# unset PYSPARK_DRIVER_PYTHON

# # Submit query to Spark, passing it via stdin
# spark-submit --master yarn \
#   --deploy-mode client \
#   --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./.venv/bin/python \
#   --conf spark.jars.exclude=kubernetes-*.jar \
#   --conf spark.executor.extraClassPath="/apps/spark/jars/*" \
#   --conf spark.driver.extraClassPath="/apps/spark/jars/*" \
#   --archives /app/.venv.tar.gz#.venv \
#   --packages com.datastax.spark:spark-cassandra-connector_2.12:3.5.0,com.github.jnr:jnr-posix:3.1.15 \
#   --repositories https://jitpack.io \
#   query.py "$@"


source .venv/bin/activate

# Python of the driver (/app/.venv/bin/python)
export PYSPARK_DRIVER_PYTHON=$(which python) 

# Python of the excutor (./.venv/bin/python)
export PYSPARK_PYTHON=./.venv/bin/python

if ! jps | grep -q "NodeManager"; then
  echo "NodeManager is not running. Attempting to start it..."
  yarn --daemon start nodemanager
  sleep 5
else
  echo "NodeManager is already running."
fi

spark-submit --master yarn --archives /app/.venv.tar.gz#.venv query.py $1