from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql_query="",
                 skip_truncate=False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_query = sql_query
        self.skip_truncate = skip_truncate

    #Executing function to execute the SQL queries defined here.
    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        sql_code = ""

        if self.skip_truncate:
            sql_code = f"""
                    BEGIN;
                    
                    INSERT INTO {self.table}
                    
                    {self.sql_query};"""
        else:
            sql_code = f"""
                    BEGIN;
                    TRUNCATE TABLE {self.table}; 
                    
                    INSERT INTO {self.table}
                    
                    {self.sql_query};"""


        redshift.run(sql_code)
