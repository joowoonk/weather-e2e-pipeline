from snowflake.snowpark.dataframe import DataFrame
from snowflake.snowpark.session import Session
from project.transformers import *
from project.utils import get_env_var_config
from snowflake.snowpark.functions import col, to_date, hour, to_timestamp


def transform_tables(session: Session, schema, source_table) -> int:
    """
    This job applies the transformations in `project.transformers` to the weather dataset
    and creates more tables under WEATHER.PUBLIC.
    The job's return value is the total number of rows created in the new tables.
    """

    SOURCE_DB = "WEATHER_DB"

    df: DataFrame = session.table([SOURCE_DB, schema, source_table])

    # Delegate transformations to the modular transformer function
    hourly_date_df = transform_by_hourly_and_date(df)
    hourly_date_df.write.save_as_table([SOURCE_DB, schema, "WEATHER_TIME"], table_type="", mode="overwrite")

    f_conversion_to_c = tranform_from_f_to_c(hourly_date_df)
    f_conversion_to_c.write.save_as_table([SOURCE_DB, schema, "WEATHER_TEMP"], table_type="", mode="overwrite")

    hourly_date_counts = hourly_date_df.count()
    f_conversion_to_c = f_conversion_to_c.count()

    return hourly_date_counts + f_conversion_to_c




if __name__ == "__main__":
    print("Creating session")
    session = Session.builder.configs(get_env_var_config()).create()

    print("Running job...")
    rows = transform_tables(session, "PUBLIC", "WEATHER")

    print(f'Job complete. Number of rows created: {rows}')

