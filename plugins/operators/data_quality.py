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

        redshift_hook = PostgresHook(self.redshift_conn_id)

        dq_checks={"check_sql":"SELECT COUNT(*) FROM " , "expected_result": 1}
         
        for table in self.tables:
            self.log.info(f"DATA QUALITY VERIFICATION FOR TABLE {table}")

            sql_query_dq = dq_checks["check_sql"] + f" {table} "
            
            records = redshift_hook.get_records(sql_query_dq)

            if len(records) < dq_checks["expected_result"]:
                raise ValueError(f"DQ FAILURE table {table} has no data")
            num_records = records[0][0]
            
            if num_records < dq_checks["expected_result"]:
                raise ValueError(f"DQ FAILURE table {table} has no data")
            self.log.info(f"DQ SUCCESS for table {table} with {records[0][0]} records")
            
            #if len(records[0]) < 1 or len(records) < 1:
                #raise ValueError(f"DQ FAILURE table {table} has no data")
            #num_records = records[0][0]

            #if num_records < 1:
               # raise ValueError(f"DQ FAILURE table {table} has no data")
            #self.log.info(f"DQ SUCCESS for table {table} with {records[0][0]} records")
            
            
            
