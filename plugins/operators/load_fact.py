from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    """
    LoadFactOperator is designed to load data from staging tables to a target fact table
        
    -Parameters
    redshift_conn_id: Apache Airflow Connection with Amazon Redshift cluster details
    table: Name of the target dimension table
    load_sql: SQL code to transform and load the data as desired
    """
    
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 load_sql="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.load_sql = load_sql

    def execute(self, context):
        # Using Redshift hook, loads data from staging table to fact table
        self.log.info('LoadFactOperator not implemented yet')
        redshifthook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info("Copying data from staging tables to fact table")
        fact_sql = f"""
            INSERT INTO {self.table} 
            {self.load_sql}
        """
        
        redshifthook.run(fact_sql)
        
