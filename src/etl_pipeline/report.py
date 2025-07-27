import pandas as pd

class Reporter:
    def __init__(self, aggregated_df: pd.DataFrame):
        self.aggregated_df = aggregated_df

    def _create_top_clients(
        self,
        top_n: int = 3,
        client_category: str = "bronze"
    ) -> pd.DataFrame:
        return self.aggregated_df.loc[
            self.aggregated_df["client_type"] == client_category
        ].nlargest(top_n, columns=["total_volume"])


    def generate_report(self) -> bool:
        """Generate top 3 bronze clients report.

        Returns:
            bool: Status of the report generation.
        """
        n_largest_clients  = self._create_top_clients()
        if n_largest_clients.empty:
            print("No data available for the report.")
            return False
        n_largest_clients.to_csv("src/etl_pipeline/output/top_clients.csv", index=False)
        return True
