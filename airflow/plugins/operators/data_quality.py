from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",   
                 tables=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)

        self.redshift_conn_id = redshift_conn_id
        self.tables = tables

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info('Starting Data Quality Check')
        
        for table in self.tables:
            records = redshift_hook.get_records(f"Select count(*) from {table};")
            self.log.info(f"Table {table} has {records[0][0]} records!!")    
        
        