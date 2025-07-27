from numpy import require
from etl_pipeline.extractor import Extractor
from etl_pipeline.transformer import Transformer
from etl_pipeline.loader import Loader
import click

@click.command()
@click.argument("source_path")
@click.argument("source_type", required=False, default="csv")
@click.argument("output_path", required=False, default="src/etl_pipeline/output/agg_result.db")
@click.argument("output_table", required=False, default="agg_trades_weekly")
def main(
    source_path: str,
    source_type: str = "csv",
    output_path: str = "src/etl_pipeline/output/agg_result.db",
    output_table: str = "agg_trades_weekly"
) -> None:
    """ETL Pipeline Main Function

    Args:
        source_path (str): Source file path for processing.
        source_type (str, optional): Source file datatype. Defaults to 'csv'.
    """
    # print(source_path)
    # print(source_type, required=False)
    # print(output_path, required=False)
    # print(output_table, required=False)
    extractor = Extractor(source_path=source_path, source_type=source_type)
    raw_df = extractor.extract()
    transformer = Transformer(input_df=raw_df)
    transformed_df = transformer.main()

    loader = Loader(
        db_url=f"sqlite:///{output_path}"
    )
    save_status = loader.save_df_to_db(
        df=transformed_df,
        table_name = output_table
    )

    if not save_status:
        print("Failed to save the DataFrame to the database.")
        raise Exception("DataFrame saving failed.")
    print("ETL Pipeline completed successfully.")

if __name__ == "__main__":
    main()
