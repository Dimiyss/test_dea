import os
from sqlalchemy import create_engine
from typing import Optional
import pandas as pd

class Loader:
    def __init__(
        self,
        db_url: Optional[str] = "sqlite:///src/etl_pipeline/output/agg_result.db",
    ):
        if db_url.startswith("sqlite:///"):
            db_path = db_url.replace("sqlite:///", "")
            os.makedirs(os.path.dirname(db_path), exist_ok=True)
        self.engine = create_engine(db_url)

    def save_df_to_db(
        self,
        df: pd.DataFrame,
        table_name: Optional[str] = "agg_trades_weekly",
        if_exists: Optional[str] = "replace"
    ) -> bool:
        """Save DataFrame to SQLite database.

        Args:
            df (pd.DataFrame): DataFrame to save.
            table_name (str, optional): Name of the table in the database. Defaults to 'agg_trades_weekly'.
            if_exists (str, optional): Behavior when the table already exists. Defaults to 'replace'.

        Returns:
            bool: True if the operation was successful, False otherwise.
        """
        try:
            df.to_sql(table_name, self.engine, if_exists=if_exists, index=False)
            return True
        except Exception as e:
            print(f"Error saving DataFrame to database: {e}")
            return False
