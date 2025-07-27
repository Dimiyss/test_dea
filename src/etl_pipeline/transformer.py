from numpy import double
import pandas as pd
from typing import List


class Transformer:
    """Transformation class as T in ETL pipeline."""

    def __init__(
        self,
        input_df: pd.DataFrame,
        col_list: List[str] = ["week_start_date", "client_type", "user_id", "symbol"],
    ):
        self.input_df: pd.DataFrame = input_df
        self.col_list: List[str] = col_list

    def _clean_and_cast_df(self) -> None:
        """Cleans and cast strings to datetime in the input data frame.

        Raises:
            Exception: Incorrect input data, column 'timestamp' is not in datetime format.
        """
        try:
            self.input_df["timestamp"] = pd.to_datetime(
                self.input_df["timestamp"], errors="coerce"
            )
        except Exception as e:
            raise Exception(
                f"Incorrect input data, column 'timestamp' is not in datetime format: {e}"
            )

        self.input_df.dropna(how="all", inplace=True)
        self.input_df.fillna({"quantity": 0, "price": 0}, inplace=True)

    def _aggregate(self) -> pd.DataFrame:
        """Aggregates the input data frame by specified columns and calculates total volume and count.

        Returns:
            pd.DataFrame: Aggregated DataFrame with total volume and count for each group.
        """
        groupby_df = self.input_df.groupby(self.col_list)
        new_df = pd.DataFrame(
            {
                "total_volume": groupby_df["volume"].sum(),
                "total_count": groupby_df["quantity"].sum(),
            }
        )
        return new_df.loc[
            (new_df["total_volume"] > 0.0) & (new_df["total_count"] > 0.0)
        ]

    def main(self) -> pd.DataFrame | None:
        """Main transformation execution method.

        Raises:
            ValueError: Data must be a pandas DataFrame if input is not a pandas data frame.

        Returns:
            pd.DataFrame: Aggregated DataFrame with required metrics.
        """
        if isinstance(self.input_df, pd.DataFrame):
            self._clean_and_cast_df()
            self.input_df["client_type"] = self.input_df["client_type"].astype(
                "category"
            )
            self.input_df["week_start_date"] = (
                self.input_df["timestamp"].dt.to_period("W").dt.start_time
            )
            self.input_df["volume"] = (
                self.input_df["quantity"] * self.input_df["price"]
            ).astype(double)
            aggregated_df = self._aggregate()
            return aggregated_df.reset_index()
        else:
            raise ValueError("Data must be a pandas DataFrame")
