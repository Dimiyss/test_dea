import pandas as pd

class Extractor():
    def __init__(self, source_path, source_type='csv'):
        self.source_path = source_path
        self.source_type = source_type

    def extract(self):
        # Placeholder for extraction logic
        if self.source_type == 'csv':
            return pd.read_csv(self.source_path)
        else:
            raise ValueError(f"Unsupported source type: {self.source_type}")
