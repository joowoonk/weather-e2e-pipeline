from snowflake.snowpark.dataframe import DataFrame
from snowflake.snowpark.session import Session
from project.transformers import *
from project.utils import get_env_var_config
from snowflake.snowpark.functions import col, to_date, hour, to_timestamp


def create_fact_tables(session: Session, schema, source_table) -> int:
    """
    This job applies the transformations in `project.transformers` to the weather dataset
    and creates more tables under WEATHER.PUBLIC.
    The job's return value is the total number of rows created in the new tables.
    """

    SOURCE_DB = "WEATHER_DB"

    df: DataFrame = session.table([SOURCE_DB, schema, source_table])

    # Delegate transformations to the modular transformer function
    df2 = transform_by_hourly_and_date(df)
    df2 = tranform_from_f_to_c(df2)

    df2.write.save_as_table([SOURCE_DB, schema, "WEATHER_TIME"], table_type="", mode="overwrite")

    

    row_counts = df2.count()

    return row_counts


if __name__ == "__main__":
    print("Creating session")
    session = Session.builder.configs(get_env_var_config()).create()

    print("Running job...")
    rows = create_fact_tables(session, "PUBLIC", "WEATHER")

    print(f'Job complete. Number of rows created: {rows}')
    # session.table("WEATHER_DB.PUBLIC.WEATHER_TIME").show(10)
