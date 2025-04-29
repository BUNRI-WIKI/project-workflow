from airflow.models import BaseOperator

class HateSpeechOperator(BaseOperator):
    def __init__(self, conn_id, **kwargs):
        super().__init__(**kwargs)
        self._conn_id = conn_id
    
    def execute(self, context):
        import pandas as pd

        from plugins.hooks.aws_rds_hook import AWSRDSHook
    
        rds_hook = AWSRDSHook(self._conn_id)
        rds_conn, rds_cur = rds_hook.get_conn()

        df = pd.read_sql('''SELECT * FROM account''', rds_conn)
        print(df)

        rds_hook.close()
