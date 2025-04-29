
from airflow.hooks.base import BaseHook

class AWSRDSHook(BaseHook):
    def __init__(self, conn_id):
        super().__init__()
        self._conn_id = conn_id
        self._conn = None
        self._cur = None

    def get_conn(self):
        from airflow.models import Variable

        import pymysql

        if self._conn is None:
            try:
                self._conn = pymysql.connect(
                    host=Variable.get('aws_rds_host'),
                    user=Variable.get('aws_rds_user'),
                    password=Variable.get('aws_rds_password'),
                    port=3306,
                    database=Variable.get('aws_rds_database'),
                )
                self._cur = self._conn.cursor()
            except Exception as e:
                print(f"Failed to connect to RDS: {e}")
                self._conn, self._cur = None, None

        return self._conn, self._cur

    def close(self):
        if self._cur:   self._cur.close()
        if self._conn:  self._conn.close()
