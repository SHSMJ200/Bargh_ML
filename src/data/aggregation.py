from dbconnection import Database
from logs.logger import CustomLogger
from src.root import get_root

logger = CustomLogger(__name__).get_logger()


def integrated_aggregation():
    try:
        sql_path = get_root() + '/src/data/queries/integrated.sql'
        with open(sql_path, 'r') as f:
            sql_query = f.read()
        logger.info(f'Successfully loaded sql template.')

        with Database() as db:

            db.execute(query=sql_query, do_return=False)
            db.commit()

            logger.info(f'Successfully applied the query:\n{sql_query}\n on database.')

            integrated_path = get_root() + '/data/processed/integrated.csv'
            db.copy_expert(table_name='integrated_data', file=integrated_path, into_db=False)

            db.commit()

    except Exception as exc:
        logger.error(f"Couldn't apply the query:\n{sql_query}\n Exception:\n{exc}\n occurred.")
