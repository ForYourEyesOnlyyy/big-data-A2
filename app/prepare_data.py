from pathvalidate import sanitize_filename
from tqdm import tqdm
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder \
    .appName('data preparation') \
    .master("local[2]") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .config("spark.sql.parquet.enableVectorizedReader", "true") \
    .getOrCreate()


df = spark.read.parquet("/a.parquet")
n = 101
df = df.select(['id', 'title', 'text']).sample(fraction=n / df.count(), seed=0)


def create_doc(row):
    filename = "data/" + sanitize_filename(str(row['id']) + "_" + row['title']).replace(" ", "_") + ".txt"
    with open(filename, "w") as f:
        f.write(row['text'])

def create_docs(partition):
    for row in partition:
        create_doc(row)

df.foreachPartition(create_docs)

# df \
#     .select(
#         F.col("id").cast("string").alias("doc_id"),
#         F.col("title").alias("doc_title"),
#         F.col("text").alias("doc_text")
#     ) \
#     .repartition(4) \
#     .write \
#     .option("sep", "\t") \
#     .csv("/index/data")

df.write.csv("/index/data", sep="\t")