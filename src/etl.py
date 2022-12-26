import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries
import logging 
logging.basicConfig(level = logging.INFO,format='%(asctime)s-%(levelname)s-%(message)s')


def load_staging_tables(cur, conn):
    '''
    Loads data to staging tables
    '''
    for query in copy_table_queries:
        logging.info(f"running query {query}")
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    '''
    Inserts data into fact and dimension tables
    '''
    for query in insert_table_queries:
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
    
    #load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()