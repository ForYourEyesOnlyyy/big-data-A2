# from pathvalidate import sanitize_filename

# from pyspark.sql import SparkSession, functions


# spark = SparkSession.builder \
#     .appName('data preparation') \
#     .master("local[2]") \
#     .config("spark.sql.parquet.enableVectorizedReader", "true") \
#     .config("spark.driver.memory", "8g") \
#     .config("spark.executor.memory", "8g") \
#     .config("spark.hadoop.fs.defaultFS", "hdfs://cluster-master:9000") \
#     .getOrCreate()


# df = spark.read.parquet("/a.parquet")
# n = 100
# df = df.select(['id', 'title', 'text']).sample(fraction=100 * n / df.count(), seed=0).limit(n)


# def create_doc(row):
#     filename = "data/" + sanitize_filename(str(row['id']) + "_" + row['title']).replace(" ", "_") + ".txt"
#     with open(filename, "w") as f:
#         f.write(row['text'])

# def create_docs(partition):
#     for row in partition:
#         create_doc(row)

# df.foreachPartition(create_docs)


# df.select(
#     functions.col("id").cast("string").alias("doc_id"),
#     functions.col("title").alias("doc_title"),
#     functions.col("text").alias("doc_text")
# ).write.option("sep", "\t").csv("/index/data", mode="overwrite")

from pathvalidate import sanitize_filename
from tqdm import tqdm
from pyspark.sql import SparkSession


spark = SparkSession.builder \
    .appName('data preparation') \
    .master("local") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .config("spark.sql.parquet.enableVectorizedReader", "true") \
    .getOrCreate()


df = spark.read.parquet("/a.parquet")
n = 100
df = df.select(['id', 'title', 'text']).sample(fraction=100 * n / df.count(), seed=0).limit(n)


def create_doc(row):
    filename = "data/" + sanitize_filename(str(row['id']) + "_" + row['title']).replace(" ", "_") + ".txt"
    with open(filename, "w") as f:
        f.write(row['text'])

def create_docs(partition):
    for row in partition:
        create_doc(row)

df.foreachPartition(create_docs)

df.write.csv("/index/data", sep = "\t")