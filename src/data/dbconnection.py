from src.root import get_root
import yaml
import psycopg2
from logs.logger import CustomLogger

logger = CustomLogger(__name__).get_logger()


class Database:
    def __init__(self):
        db_config_path = get_root() + '/configs/database.yaml'
        with open(db_config_path, "r", encoding="utf-8") as f:
            self.connection_parameters = yaml.load(f, Loader=yaml.SafeLoader)

        self.connection = None

    def __enter__(self):
        try:
            if self.connection is None:
                self.connection = psycopg2.connect(**self.connection_parameters)
            logger.debug('User connected to the DB')
            return self
        except Exception as e:
            logger.error(f"Couldn't connect to the DB. Exception: \n{e}\n occurred.")
            raise

    def __exit__(self, exc_type, exc_value, traceback):
        try:
            if self.connection:
                if exc_type is None:
                    self.connection.commit()
                else:
                    self.connection.rollback()
                self.connection.close()
                self.connection = None
            logger.debug(f'Connection to the DB was closed.')
        except Exception as e:
            logger.error(f"Couldn't exit the DB. Exception\n{e}\n occurred.")

        return False  # to raise error again

    def execute(self, query: str, params=None, do_return=False):
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query, params)
                logger.debug(f'Successfully execute the query: \n{query}\n on the DB.')
                if do_return:
                    return cursor.fetchall()

        except Exception as e:
            logger.error(f"Couldn't execute query: \n{query}\n on the DB. Exception \n{e}\n occurred.")
            raise

    def commit(self):
        try:
            if self.connection:
                self.connection.commit()
                logger.debug('Commitment successfully applied.')
        except Exception as e:
            logger.error(f'Commitment failed.')

    def rollback(self):
        try:
            if self.connection:
                self.connection.rollback()
                logger.debug('Rollback successfully applied.')
        except Exception as e:
            logger.error(f"Rollback couldn't be applied. Exception\n{e}\n occurred.")

    def create_table(self, table_name: str, col_names_and_types: dict[str: str]):
        features = [f'{col_name} {col_type}' for col_name, col_type in col_names_and_types.items()]
        columns_defs = ', '.join(features)
        self.execute(
            query=f'create table if not exists {table_name} ({columns_defs})',
            do_return=False
        )

    def copy_expert(self, table_name: str, file: str, into_db=False):
        try:
            if into_db:
                query = f"copy {table_name} from stdin with delimiter ',' csv header NULL as 'NULL'"
                mode = 'r'
            else:
                query = f"copy {table_name} to stdout with delimiter ',' csv header NULL as 'NULL'"
                mode = 'w'

            with self.connection.cursor() as cursor:
                with open(file, mode, encoding='utf-8') as f:
                    cursor.copy_expert(query, f)
                logger.debug(f'Successfully copied file {file}')

        except Exception as e:
            logger.error(f"Couldn't copy the file with query:\n{query}\n because the Exception\n{e}\n occurred.")
