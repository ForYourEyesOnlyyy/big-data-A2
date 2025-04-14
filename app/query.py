#!/usr/bin/env python3

import sys
import re
import traceback
from math import log
from pyspark.sql import SparkSession
from pyspark.sql.functions import col


CASSANDRA_KEYSPACE = "search_engine"
k = 1.2
b = 0.75


def tokenize(text):
    return re.findall(r'\w+', text.lower())


def compute_bm25(tf, doc_len, idf, avg_len):
    numerator = tf * (k + 1)
    denominator = tf + k * (1 - b + b * (doc_len / avg_len))
    return idf * (numerator / denominator)


def load_tables(spark, query_terms):
    sys.stderr.write("[query] Loading index and stats from Cassandra...\n")

    postings_df = spark.read \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="postings", keyspace=CASSANDRA_KEYSPACE) \
        .load() \
        .filter(col("term").isin(query_terms))

    bm25_df = spark.read \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="bm25_stats", keyspace=CASSANDRA_KEYSPACE) \
        .load() \
        .filter(col("term").isin(query_terms)) \
        .select("term", "idf")

    doc_lengths_df = spark.read \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="doc_lengths", keyspace=CASSANDRA_KEYSPACE) \
        .load()

    documents_df = spark.read \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="documents", keyspace=CASSANDRA_KEYSPACE) \
        .load()

    vocab_df = spark.read \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="vocabulary", keyspace=CASSANDRA_KEYSPACE) \
        .load()

    vocab_terms = vocab_df.select("term").rdd.map(lambda row: row["term"]).collect()
    sys.stderr.write(f"[query] Loaded vocabulary: {len(vocab_terms)} terms.\n")

    return postings_df, bm25_df, doc_lengths_df, documents_df, vocab_terms


def run_query(query_text):
    query_terms_raw = tokenize(query_text)
    if not query_terms_raw:
        sys.stderr.write("[query] No valid query terms.\n")
        return

    spark = SparkSession.builder \
        .appName("BM25 Search Query") \
        .config("spark.cassandra.connection.host", "cassandra-server") \
        .getOrCreate()

    try:
        postings_df, bm25_df, doc_lengths_df, documents_df, vocabulary = load_tables(spark, query_terms_raw)

        query_terms = [term for term in query_terms_raw if term in vocabulary]
        if not query_terms:
            print("No query terms matched the indexed vocabulary.")
            return

        # Join postings with document lengths and idf
        joined_df = postings_df \
            .join(doc_lengths_df, on="doc_id") \
            .join(bm25_df, on="term") \
            .join(documents_df, on="doc_id", how="left")

        # Compute average doc length
        avg_doc_len = doc_lengths_df.selectExpr("avg(length) as avg_len").collect()[0]["avg_len"]

        # Compute BM25 per (term, doc)
        scored_rdd = joined_df.rdd.map(lambda row: (
            row["doc_id"],
            row["title"],
            compute_bm25(row["tf"], row["length"], row["idf"], avg_doc_len)
        ))

        # Aggregate BM25 scores per doc_id and title
        doc_scores = scored_rdd.map(lambda x: ((x[0], x[1]), x[2])) \
                               .reduceByKey(lambda a, b: a + b) \
                               .takeOrdered(10, key=lambda x: -x[1])

        print(f"\nTop 10 documents for query: '{query_text}'")
        print("doc_id\ttitle\tscore")
        for (doc_id, title), score in doc_scores:
            clean_title = title if title else "(no title)"
            print(f"{doc_id}\t{clean_title[:60]}\t{score:.4f}")

    except Exception as e:
        sys.stderr.write(f"[query] Error during query execution: {e}\n")
        traceback.print_exc(file=sys.stderr)
    finally:
        spark.stop()


if __name__ == "__main__":
    try:
        if len(sys.argv) < 2:
            print("Usage: spark-submit query.py \"your query here\"")
            sys.exit(1)

        query_line = " ".join(sys.argv[1:]).strip()
        run_query(query_line)

    except Exception as e:
        sys.stderr.write(f"[query] Fatal error: {e}\n")
        traceback.print_exc(file=sys.stderr)
        sys.exit(1)