from src.root import get_root
import yaml
from psycopg2 import Error as Error

from dbconnection import Database
from logs.logger import CustomLogger

logger = CustomLogger(__name__, log_file_name='aggregation.log').get_logger()

db = Database()


class Aggregator:
    def __init__(self, name: str):
        self.user = name
        self.db = Database()
        db.connect()
        db.__exit__()
        self.logger = CustomLogger(__name__, log_file_name=f'aggregation({name}).log').get_logger()
        self.query_path = get_root()  + '/src/data/queries/'

    def integrated_aggregation(self):
        try:
            sql_query = self.load_sql_query('/src/data/queries/integrated.sql')
            logger.debug(msg=f'Successfully loaded table configs and sql template.')

            db.__enter__()
            db.execute(
                query=sql_query,
                do_return=False
            )
            db.commit()

            logger.debug(msg=f'Successfully applied the query:\n{sql_query}\n on database.')

            target_table = 'integrated_data'

            db.lazy_copy_expert(
                table_name=target_table,
                file='/data/processed/integrated.csv',
                mode='w',
                into_local=True
            )

            db.commit()

            db.__exit__()

        except Error as e:
            logger.error(f"Couldn't apply the query:\n{sql_query}\n Exception:\n{e}\n occurred.")
        except Exception as exc:
            logger.error(f"Couldn't apply the query:\n{sql_query}\n Exception:\n{exc}\n occurred.")

    def load_sql_query(self, filename):
        with open(filename, 'r') as f:
            return f.read()

    def load_tables_configs(self, filename):
        with open(filename, 'r') as f:
            return yaml.safe_load(f)
