from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    
    """
    DataQualityOperator is designed to perform a simple data quality check on a list of provided tables
    
    A list of table name is iterated over and for each, a check is performed to see if the table has rows
    
    -Parameters
    redshift_conn_id: Apache Airflow Connection with Amazon Redshift cluster details
    tables: Python list containing the names of tables to be checked
    """
    
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 tables="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables

    def execute(self, context):
        # As has_rows operator in example video
        # Iterates through list of table names
        # Checks if they have rows and if not, returns an error
        # or success if logged
        self.log.info('DataQualityOperator not implemented yet')
        redshifthook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        for t in self.tables:
            self.log.info(f"Check if {t} has any records")
            row_count = redshifthook.get_records(f"SELECT COUNT(*) FROM {t}")
            if len(row_count) < 1 or len(row_count[0]) < 1:
                raise ValueError(f"Data quality check failed. {t} did not return results")
            num_records = row_count[0][0]
            if num_records < 1:
                raise ValueError(f"Data quality check failed. {t} has no data")
            self.log.info(f"Data quality check on {t} passed. {row_count[0][0]} found")
