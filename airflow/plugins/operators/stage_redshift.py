from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    copy_sql = """COPY {}
                 FROM '{}'
                 ACCESS_KEY_ID '{}'
                 SECRET_ACCESS_KEY '{}'
                 REGION 'us-west-2'
                 JSON '{}'""" 

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 log_json_path="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)

        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials = aws_credentials
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.log_json_path = log_json_path
        

    def execute(self, context):
        aws_hook=AwsHook(self.aws_credentials)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("Clearing data from Staging tables")
        redshift.run("DELETE FROM {}".format(self.table))
        
        self.log.info("Copying data from S3 to Redshift")
        s3_path = "s3://{}/{}".format(self.s3_bucket,self.s3_key)
        if self.s3_key == "log_data":
            json_path = "s3://{}/{}".format(self.s3_bucket,self.log_json_path)
        else:
            json_path = 'auto'
        formatted_sql = StageToRedshiftOperator.copy_sql.format(self.table,
                                                                s3_path,
                                                                credentials.access_key,
                                                                credentials.secret_key,
                                                                json_path)
        redshift.run(formatted_sql)
        self.log.info(f"Staging table {self.table} created successfully")





