#!/usr/bin/env python3

import sys
import time
import traceback
from cassandra.cluster import Cluster

def connect_to_cassandra(hosts, keyspace, max_retries=5, delay=5):
    for attempt in range(1, max_retries + 1):
        try:
            sys.stderr.write(f"Connecting to Cassandra (attempt {attempt})...\n")
            cluster = Cluster(hosts, port=9042)
            session = cluster.connect()
            session.execute(f"""
                CREATE KEYSPACE IF NOT EXISTS {keyspace}
                WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}
            """)
            session.set_keyspace(keyspace)
            sys.stderr.write("Cassandra connection established.\n")
            return cluster, session
        except Exception as e:
            sys.stderr.write(f"Connection failed: {e}\n")
            if attempt < max_retries:
                time.sleep(delay)
            else:
                raise RuntimeError("Failed to connect to Cassandra after retries.")


def create_tables(session):
    try:
        session.execute("""
            CREATE TABLE IF NOT EXISTS term_index (
                term text,
                doc_id text,
                count int,
                PRIMARY KEY (term, doc_id)
            )
        """)
        session.execute("""
            CREATE TABLE IF NOT EXISTS doc_lengths (
                doc_id text PRIMARY KEY,
                length int
            )
        """)
        session.execute("""
            CREATE TABLE IF NOT EXISTS documents (
                doc_id text PRIMARY KEY,
                title text
            )
        """)
        sys.stderr.write("Tables checked/created.\n")
    except Exception as e:
        raise RuntimeError(f"Error creating tables: {e}")

def insert_doc_length(session, doc_id, length):
    try:
        session.execute(
            "INSERT INTO doc_lengths (doc_id, length) VALUES (%s, %s)",
            (doc_id, int(length))
        )
        print(f"!doclen\t{doc_id}\t{length}")
    except Exception as e:
        sys.stderr.write(f"Failed to insert doc_length ({doc_id}): {e}\n")

def insert_term_index(session, term, doc_id, count):
    try:
        session.execute(
            "INSERT INTO term_index (term, doc_id, count) VALUES (%s, %s, %s)",
            (term, doc_id, count)
        )
        print(f"{term}\t{doc_id}\t{count}")
    except Exception as e:
        sys.stderr.write(f"Failed to insert term_index ({term}, {doc_id}): {e}\n")

def insert_document_title(session, doc_id, title):
    try:
        session.execute(
            "INSERT INTO documents (doc_id, title) VALUES (%s, %s)",
            (doc_id, title)
        )
    except Exception as e:
        sys.stderr.write(f"Failed to insert document title ({doc_id}): {e}\n")

def run_reducer(session):
    prev_key = None
    count = 0

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue

        parts = line.split("\t")

        if not parts or len(parts) < 2:
            sys.stderr.write(f"⚠️ Malformed input skipped: {line}\n")
            continue

        if parts[0] == "!doclen":
            if len(parts) != 3:
                sys.stderr.write(f"⚠️ Invalid !doclen format: {line}\n")
                continue
            _, doc_id, length = parts
            insert_doc_length(session, doc_id, length)
            continue

        if parts[0] == "!title":
            if len(parts) != 3:
                sys.stderr.write(f"⚠️ Invalid !title format: {line}\n")
                continue
            _, doc_id, title = parts
            insert_document_title(session, doc_id, title)
            continue

        try:
            term, doc_id = parts
            key = (term, doc_id)

            if prev_key and key != prev_key:
                prev_term, prev_doc_id = prev_key
                insert_term_index(session, prev_term, prev_doc_id, count)
                count = 1
            else:
                count += 1

            prev_key = key

        except Exception as e:
            sys.stderr.write(f"Exception while processing line: {line}\n{e}\n")
            traceback.print_exc(file=sys.stderr)
            continue

    # Final flush
    if prev_key:
        t, d = prev_key
        insert_term_index(session, t, d, count)

if __name__ == "__main__":
    try:
        cluster, session = connect_to_cassandra(['cassandra-server'], 'search_engine')
        create_tables(session)
        run_reducer(session)
        cluster.shutdown()
        sys.stderr.write("Reducer 1 finished successfully.\n")
    except Exception as e:
        sys.stderr.write(f"Reducer 1 failed: {e}\n")
        traceback.print_exc(file=sys.stderr)
        sys.exit(1)