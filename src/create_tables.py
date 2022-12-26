import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries
import logging 
logging.basicConfig(level = logging.INFO,format='%(asctime)s-%(levelname)s-%(message)s')


def drop_tables(cur, conn):
    '''
    Drops staging, fact and dimension tables
    '''
    for query in drop_table_queries:
        logging.info(f"running query {query}")
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    '''
    Creates staging, fact and dimension tables
    '''
    for query in create_table_queries:
        logging.info(f"running query {query}")
        cur.execute(query)
        conn.commit()


def main():
    '''
    Entry point to the program
    '''
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()