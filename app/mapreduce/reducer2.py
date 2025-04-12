#!/usr/bin/env python3

import sys
import time
import math
import traceback
from cassandra.cluster import Cluster


def connect_to_cassandra(hosts, keyspace, max_retries=5, delay=5):
    for attempt in range(1, max_retries + 1):
        try:
            sys.stderr.write(f"[reducer2] Connecting to Cassandra (attempt {attempt})...\n")
            cluster = Cluster(hosts, port=9042)
            session = cluster.connect()
            session.execute(f"""
                CREATE KEYSPACE IF NOT EXISTS {keyspace}
                WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}
            """)
            session.set_keyspace(keyspace)
            sys.stderr.write("[reducer2] Cassandra connection established.\n")
            return cluster, session
        except Exception as e:
            sys.stderr.write(f"[reducer2] Connection failed: {e}\n")
            if attempt < max_retries:
                time.sleep(delay)
            else:
                raise RuntimeError("Failed to connect to Cassandra after retries.")


def create_tables(session):
    try:
        session.execute("""
            CREATE TABLE IF NOT EXISTS postings (
                term text,
                doc_id text,
                tf int,
                PRIMARY KEY (term, doc_id)
            )
        """)
        session.execute("""
            CREATE TABLE IF NOT EXISTS bm25_stats (
                term text PRIMARY KEY,
                df int,
                idf float
            )
        """)
        sys.stderr.write("[reducer2] Tables postings and bm25_stats checked/created.\n")
    except Exception as e:
        raise RuntimeError(f"[reducer2] Error creating tables: {e}")


def get_total_doc_count(session):
    try:
        result = session.execute("SELECT COUNT(*) FROM doc_lengths")
        return result.one()[0]
    except Exception as e:
        raise RuntimeError(f"[reducer2] Failed to fetch total doc count: {e}")


def insert_posting(session, term, doc_id, tf):
    try:
        session.execute(
            "INSERT INTO postings (term, doc_id, tf) VALUES (%s, %s, %s)",
            (term, doc_id, tf)
        )
    except Exception as e:
        sys.stderr.write(f"[reducer2] Failed to insert posting ({term}, {doc_id}, {tf}): {e}\n")


def insert_bm25_stat(session, term, df, idf):
    try:
        session.execute(
            "INSERT INTO bm25_stats (term, df, idf) VALUES (%s, %s, %s)",
            (term, df, idf)
        )
    except Exception as e:
        sys.stderr.write(f"[reducer2] Failed to insert bm25_stat ({term}, {df}, {idf}): {e}\n")


def flush_term(session, term, doc_tf_map, total_docs):
    try:
        df = len(doc_tf_map)
        idf = math.log((total_docs - df + 0.5) / (df + 0.5) + 1)

        insert_bm25_stat(session, term, df, idf)

        for doc_id, tf in doc_tf_map.items():
            insert_posting(session, term, doc_id, tf)

        sys.stderr.write(f"[reducer2] Flushed term '{term}' with df={df}.\n")
    except Exception as e:
        sys.stderr.write(f"[reducer2] Error in flush_term for '{term}': {e}\n")
        traceback.print_exc(file=sys.stderr)

def run_reducer(session, total_docs):
    current_term = None
    doc_tf_map = {}

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue

        parts = line.split("\t")
        if len(parts) != 3:
            sys.stderr.write(f"[reducer2] Malformed line: {line}\n")
            continue

        term, doc_id, tf_str = parts
        try:
            tf = int(tf_str)
        except ValueError:
            sys.stderr.write(f"[reducer2] Invalid TF value: {tf_str} in line: {line}\n")
            continue

        if current_term is None:
            current_term = term

        if term != current_term:
            flush_term(session, current_term, doc_tf_map, total_docs)
            doc_tf_map = {}
            current_term = term

        doc_tf_map[doc_id] = tf

    if current_term:
        flush_term(session, current_term, doc_tf_map, total_docs)


if __name__ == "__main__":
    try:
        cluster, session = connect_to_cassandra(['cassandra-server'], 'search_engine')
        create_tables(session)

        total_docs = get_total_doc_count(session)
        sys.stderr.write(f"[reducer2] Total documents: {total_docs}\n")

        run_reducer(session, total_docs)

        cluster.shutdown()
        sys.stderr.write("[reducer2] Reducer 2 (BM25 Data Prep) completed successfully.\n")

    except Exception as e:
        sys.stderr.write(f"[reducer2] Fatal reducer error: {e}\n")
        traceback.print_exc(file=sys.stderr)
        sys.exit(1)
