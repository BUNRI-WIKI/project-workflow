from airflow.models import BaseOperator

class StoreModelInS3Operator(BaseOperator):
    '''
    모델을 s3에 저장하는 Operator
    '''
    def __init__(self, conn_id="aws_s3", **kwargs):
        super().__init__(**kwargs)
        self._conn_id = conn_id
    
    def execute(self, context):

        for image in s3_hook.list_keys(bucket_name=bucket):
            print(image)
