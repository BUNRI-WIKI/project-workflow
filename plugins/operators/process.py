import os

from airflow.models.dag import BaseOperator
from airflow.providers.google.suite.hooks.sheets import GSheetsHook
from airflow.utils.decorators import apply_defaults

from typing import Optional, Union, Sequence

class GoogleSheetToCSVOperator(BaseOperator):
    """
    분리 배출 방법이 적힌 구글 시트를 csv 파일로 저장
    """
    @apply_defaults
    def __init__(
        self,
        gsheet_id: str,
        range: str,
        file_name: str,
        save_path: str,
        gcp_conn_id: str,
        delegate_to: Optional[str] = None,
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        api_version: str = 'v4',
        **kwargs
    ) -> None:
        super().__init__(**kwargs)
        self.api_version = api_version
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.impersonation_chain = impersonation_chain
        self.gsheet_id = gsheet_id
        self.range = range
        self.file_name = file_name
        self.save_path = save_path
    
    def execute(self, context):
        import pandas as pd

        hook = GSheetsHook(
            gcp_conn_id=self.gcp_conn_id,
        )
        self.log.info(f"Ready GSheetsHook")

        spreadsheet = hook.get_values(spreadsheet_id=self.gsheet_id, range_=self.range)
        self.log.info(f"Load {hook.get_spreadsheet(spreadsheet_id=self.gsheet_id)}")

        os.makedirs(self.save_path, exist_ok=True)
        
        df = pd.DataFrame(spreadsheet[2:], columns=spreadsheet[1])

        df.to_csv(os.path.join(self.save_path, self.file_name), index=False)
        self.log.info(f"Save {os.path.join(self.save_path, self.file_name)}")
        