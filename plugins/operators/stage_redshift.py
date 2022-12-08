from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

# See lesson 4 ex 1 for structure

class StageToRedshiftOperator(BaseOperator):
    
    """
    StageToRedshiftOperator is designed to load data from json files to Amazon Redshift
    
    -Parameters
    redshift_conn_id: Apache Airflow Connection with Amazon Redshift cluster details
    aws_credentials_id: Apache Airflow Connection with AWS user details
    table: Name of the target staging table
    s3_bucket: Name of the source bucket on Amazon S3
    s3_key: Name of the source directory withing the Amazon S3 bucket
    json_path: JSON parameter file if required
    """
    
    ui_color = '#358140'
    template_fields = ("s3_key",)
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        json '{}'
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 json_path="auto",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json_path = json_path

    def execute(self, context):
        # Using AWS amd Redshift hooks, connects to and S3 bucket
        # Then loads data to staging tables
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshifthook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info('StageToRedshiftOperator not implemented yet')
        self.log.info("Clearing data from destination Redshift table")
        redshifthook.run("DELETE FROM {}".format(self.table))
        
        self.log.info("Copying data from S3 to Redshift")
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.json_path
        )
        redshifthook.run(formatted_sql)




