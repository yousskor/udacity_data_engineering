import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries
from great_expectations.dataset import SqlAlchemyDataset
from great_expectations import DataContext
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataQualityChecker:
    def __init__(self, conn):
        self.conn = conn
        self.context = DataContext()
        
    def create_validator(self, table_name):
        """Creates a Great Expectations validator for a specific table"""
        return SqlAlchemyDataset(
            table_name=table_name,
            engine=self.conn,
            schema="public"
        )
    
    def run_table_checks(self, table_name, expectations):
        """Runs data quality checks for a table"""
        validator = self.create_validator(table_name)
        results = {}
        
        for expectation in expectations:
            try:
                # Execute each expectation and store the result
                method = getattr(validator, expectation['method'])
                result = method(**expectation['kwargs'])
                results[expectation['name']] = result.success
                
                if not result.success:
                    logger.warning(f"Failed on {table_name}.{expectation['name']}: {result.result}")
            except Exception as e:
                logger.error(f"Error running {expectation['name']} on {table_name}: {str(e)}")
                results[expectation['name']] = False
        
        return results
    
    def check_staging_events(self):
        expectations = [
            {
                'name': 'row_count',
                'method': 'expect_table_row_count_to_be_between',
                'kwargs': {'min_value': 1}
            },
            {
                'name': 'required_fields',
                'method': 'expect_column_values_to_not_be_null',
                'kwargs': {'column': 'userid'}
            }
        ]
        return self.run_table_checks('staging_events', expectations)
    
    def check_staging_songs(self):
        expectations = [
            {
                'name': 'row_count',
                'method': 'expect_table_row_count_to_be_between',
                'kwargs': {'min_value': 1}
            },
            {
                'name': 'required_song_id',
                'method': 'expect_column_values_to_not_be_null',
                'kwargs': {'column': 'song_id'}
            }
        ]
        return self.run_table_checks('staging_songs', expectations)
    
    def check_final_tables(self):
        tables = ['songplays', 'users', 'songs', 'artists', 'time']
        results = {}
        
        for table in tables:
            expectations = [
                {
                    'name': 'row_count',
                    'method': 'expect_table_row_count_to_be_between',
                    'kwargs': {'min_value': 1}
                }
            ]
            results[table] = self.run_table_checks(table, expectations)
        
        return results

def load_staging_tables(cur, conn):
    logger.info("Loading staging tables...")
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()
    logger.info("Staging tables loaded successfully.")

def insert_tables(cur, conn):
    logger.info("Inserting data into final tables...")
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()
    logger.info("Data inserted into final tables successfully.")

def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    try:
        # Connect to Redshift
        conn = psycopg2.connect(
            host=config['CLUSTER']['HOST'],
            dbname=config['CLUSTER']['DB_NAME'],
            user=config['CLUSTER']['DB_USER'],
            password=config['CLUSTER']['DB_PASSWORD'],
            port=config['CLUSTER']['DB_PORT']
        )
        cur = conn.cursor()
        
        # Initialize data quality checker
        dq_checker = DataQualityChecker(conn)
        
        # Step 1: Load staging tables
        load_staging_tables(cur, conn)
        
        # Run data quality checks on staging tables
        logger.info("Running data quality checks on staging tables...")
        staging_results = {
            'staging_events': dq_checker.check_staging_events(),
            'staging_songs': dq_checker.check_staging_songs()
        }
        logger.info(f"Staging tables results: {staging_results}")
        
        # Step 2: Insert into final tables
        insert_tables(cur, conn)
        
        # Run data quality checks on final tables
        logger.info("Running data quality checks on final tables...")
        final_results = dq_checker.check_final_tables()
        logger.info(f"Final tables results: {final_results}")
        
        # Generate documentation
        dq_checker.context.build_data_docs()
        logger.info("Great Expectations documentation generated.")
        
    except Exception as e:
        logger.error(f"Error during ETL pipeline execution: {str(e)}")
        raise
    finally:
        if conn:
            conn.close()
            logger.info("Database connection closed.")

if __name__ == "__main__":
    main()