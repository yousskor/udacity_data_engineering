import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries
from great_expectations.dataset import SqlAlchemyDataset
from great_expectations import DataContext
import logging

# Configuration du logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataQualityChecker:
    def __init__(self, conn):
        self.conn = conn
        self.context = DataContext()
        
    def create_validator(self, table_name):
        """Crée un validateur Great Expectations pour une table spécifique"""
        return SqlAlchemyDataset(
            table_name=table_name,
            engine=self.conn,
            schema="public"
        )
    
    def run_table_checks(self, table_name, expectations):
        """Exécute les vérifications de qualité pour une table"""
        validator = self.create_validator(table_name)
        results = {}
        
        for expectation in expectations:
            try:
                # Exécute chaque expectation et stocke le résultat
                method = getattr(validator, expectation['method'])
                result = method(**expectation['kwargs'])
                results[expectation['name']] = result.success
                
                if not result.success:
                    logger.warning(f"Échec sur {table_name}.{expectation['name']}: {result.result}")
            except Exception as e:
                logger.error(f"Erreur lors de l'exécution de {expectation['name']} sur {table_name}: {str(e)}")
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
    logger.info("Chargement des tables de staging...")
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()
    logger.info("Chargement des tables de staging terminé.")

def insert_tables(cur, conn):
    logger.info("Insertion dans les tables finales...")
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()
    logger.info("Insertion dans les tables finales terminée.")

def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    try:
        # Connexion à Redshift
        conn = psycopg2.connect(
            host=config['CLUSTER']['HOST'],
            dbname=config['CLUSTER']['DB_NAME'],
            user=config['CLUSTER']['DB_USER'],
            password=config['CLUSTER']['DB_PASSWORD'],
            port=config['CLUSTER']['DB_PORT']
        )
        cur = conn.cursor()
        
        # Initialisation du vérificateur de qualité
        dq_checker = DataQualityChecker(conn)
        
        # Étape 1: Chargement des tables de staging
        load_staging_tables(cur, conn)
        
        # Vérification qualité après chargement staging
        logger.info("Vérification qualité des tables de staging...")
        staging_results = {
            'staging_events': dq_checker.check_staging_events(),
            'staging_songs': dq_checker.check_staging_songs()
        }
        logger.info(f"Résultats staging: {staging_results}")
        
        # Étape 2: Insertion dans les tables finales
        insert_tables(cur, conn)
        
        # Vérification qualité des tables finales
        logger.info("Vérification qualité des tables finales...")
        final_results = dq_checker.check_final_tables()
        logger.info(f"Résultats tables finales: {final_results}")
        
        # Génération de la documentation
        dq_checker.context.build_data_docs()
        logger.info("Documentation Great Expectations générée.")
        
    except Exception as e:
        logger.error(f"Erreur lors de l'exécution du pipeline ETL: {str(e)}")
        raise
    finally:
        if conn:
            conn.close()
            logger.info("Connexion à la base de données fermée.")

if __name__ == "__main__":
    main()