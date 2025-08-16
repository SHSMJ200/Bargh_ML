import yaml
import psycopg2
from logs.logger import CustomLogger

logger = CustomLogger(__name__, log_file_name='database.log').get_logger()

class Database:
    def __init__(self):
        self.connection_parameters = {
            key: value for key, value in yaml.load(open('U:/ML_project/bargh/configs/database.yaml'), Loader=yaml.SafeLoader).items()
            }
        self.connection = None
    
    def connect(self):
        if self.connection is None:
            try:
                self.connection = psycopg2.connect(**self.connection_parameters)
                logger.debug(f'a user connected to DB.')
            except Exception as e:
                logger.error(f'Couldnt connect to the DB. Exception: \n{e}\n occurred.')
    
    def close(self):
        if self.connection:
            try:
                self.connection.close()
                self.connection = None
                logger.debug(f'Connection to the DB cloesd.')
            except Exception as e:
                logger.error(f'Couldnt disconnect, Exception \n{e}\n occurred.')
    
    def execute(self, query:str, params=None, do_return=False):
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query, params)
                logger.debug(f'Successfully execute the query: \n{query}\n on the DB.')
                if do_return:
                    return cursor.fetchall()
            
        except Exception as e:
            logger.error(f'Couldnt execute query: \n{query}\n on the DB. Exception \n{e}\n occurred.')
    
    def copy_expert(self, query: str, file, mode: str):
        try:
            with self.connection.cursor() as cursor:
                with open(file, mode, encoding='utf-8') as f:
                    cursor.copy_expert(query, f)
                logger.debug(f'Successfully copied file {file}')
        except Exception as e:
            logger.error(f"Couldn't copy the file with query:\n{query}\n because the Exception\n{e}\n occurred.")
    
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
            logger.error(f'Rollback couldnt apply. Exception\n{e}\n occurred.')
    
    def __enter__(self):
        try:
            self.connect()
            logger.debug('User reconnected to the DB')
            return self
        except Exception as e:
            logger.error('Can not Enter the DB. Exception\n{e}\n occurred.')
    
    def __exit__(self):
        try:
            self.close()
            logger.debug('User exited from DB.')
        except Exception as e:
            logger.error('Can not exit the DB. Exception\n{e}\n occurred.')
    
    def get_cursor(self):
        return self.connection.cursor()
    
    def create_table(self, table_name: str, columns: dict[str: str]):
        features = [f'{col_name} {col_type}' for col_name, col_type in columns.items()]
        converted = ', '.join(features)
        self.execute(
            query=f'create table if not exists {table_name} ({converted})',
            do_return=False
        )

    def lazy_copy_expert(self, table_name: str, file: str, mode:str, into_db=False, into_local=False):
        if into_db:
            self.copy_expert(
                query=f"copy {table_name} from stdin with delimiter ',' csv header NULL as 'NULL'",
                file=file,
                mode=mode
            )
        if into_local:
            self.copy_expert(
                query=f"copy {table_name} to stdout with delimiter ',' csv header NULL as 'NULL'",
                file=file,
                mode=mode
            )