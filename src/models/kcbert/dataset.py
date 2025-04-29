from torch.utils.data import Dataset

class CustomHateSpeechDataset(Dataset):
    def __init__(
            self, 
            tokenizer, 
            max_length=128
        ) -> None:
        import pandas as pd

        from airflow.providers.postgres.hooks.postgres import PostgresHook

        hook = PostgresHook(postgres_conn_id="postgres_default")
        conn = hook.get_conn()

        self.dataset = pd.read_sql("""
            SELECT content, type
            FROM comment 
            WHERE type IS NOT NULL
        """, con=conn)
        self.tokenizer = tokenizer
        self.max_length = max_length

    def getText(self, index):
        return self.dataset.iloc[index, 0]

    def __len__(self) -> int:
        return len(self.dataset)
    
    def __getitem__(self, index) -> dict :
        text = self.dataset.iloc[index, 0]
        label = self.dataset.iloc[index, 1]

        encoding = self.tokenizer(text, padding='max_length', truncation=True, max_length=self.max_length, return_tensors='pt')
        input_ids = encoding["input_ids"].squeeze()
        attention_mask = encoding["attention_mask"].squeeze()

        return {"input_ids": input_ids, "attention_mask": attention_mask, "label": label}