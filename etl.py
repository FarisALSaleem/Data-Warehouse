import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Stages each table using the queries in `copy_table_queries` list.
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Insert transform data into each table using the queries in
    `insert_table_queries` list.
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """Connects to Redshift database, extracts data from sparkify S3 bucket,
    stages it redshift and transforms it into dimensionals

    - Creates connects a database and initiate a session.
    - Extracts user, song and metadata data from sparkify S3 bucket
    - Transforms the staged data into a set of dimensional tables
    - The connection is terminated in the end.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}"
                            .format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
