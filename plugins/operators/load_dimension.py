from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    
    """
    LoadDimensionOperator is designed to load data from staging tables to a target dimension table
    
    A truncate-insert pattern is adopted
    
    -Parameters
    redshift_conn_id: Apache Airflow Connection with Amazon Redshift cluster details
    table: Name of the target dimension table
    load_sql: SQL code to transform and load the data as desired
    """
    
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 load_sql="",
                 delete_load=False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.load_sql = load_sql
        self.delete_load = delete_load

    def execute(self, context):
        # Using Redshift hook, clears table, then loads data from staging table to dimension table
        self.log.info('LoadDimensionOperator not implemented yet')
        redshifthook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if self.delete_load == True:
             self.log.info("Truncate target dimension table to be performed")
             truncate_table = f"TRUNCATE TABLE {self.table}"
             redshifthook.run(truncate_table)
        
        self.log.info("Insert data from staging tables to dimension table")
        fact_sql = f"""
            INSERT INTO {self.table} 
            {self.load_sql};
        """
        
        redshifthook.run(fact_sql)
