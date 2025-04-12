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
            CREATE TABLE IF NOT EXISTS vocabulary (
                term text PRIMARY KEY
            )
        """)
        sys.stderr.write("vocabulary table checked/created.\n")
    except Exception as e:
        raise RuntimeError(f"Error creating vocabulary table: {e}")


def insert_term(session, term):
    try:
        session.execute(
            "INSERT INTO vocabulary (term) VALUES (%s)",
            (term,)
        )
    except Exception as e:
        sys.stderr.write(f"Failed to insert term '{term}': {e}\n")

def run_reducer(session):
    current_term = None

    for line in sys.stdin:
        term = line.strip()
        if not term:
            continue

        if term != current_term:
            insert_term(session, term)
            current_term = term

if __name__ == "__main__":
    try:
        cluster, session = connect_to_cassandra(['cassandra-server'], 'search_engine')
        create_tables(session)
        run_reducer(session)
        cluster.shutdown()
        sys.stderr.write("Reducer 3 (Vocabulary) finished successfully.\n")
    except Exception as e:
        sys.stderr.write(f"Reducer 3 failed: {e}\n")
        traceback.print_exc(file=sys.stderr)
        sys.exit(1)
