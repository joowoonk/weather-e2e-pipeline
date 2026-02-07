from snowflake.snowpark.dataframe import DataFrame
from snowflake.snowpark.session import Session
from project.transformers import *
from project.utils import get_env_var_config


def transform_tables(session: Session, schema, source_table) -> int:
    """
    This job applies the transformations in `project.transformers` to the weather dataset
    and creates more tables under WEATHER.PUBLIC.
    The job's return value is the total number of rows created in the new tables.
    """

    SOURCE_DB = "WEATHER_DB"

    df: DataFrame = session.table([SOURCE_DB, schema, source_table])

    # Delegate transformations to the modular transformer function
    enriched_df = transform_by_hourly_and_date(df)
    enriched_df.write.save_as_table([SOURCE_DB, schema, "WEATHER_TIME"], table_type="", mode="overwrite")


    enriched_df = tranform_from_f_to_c(df)
    enriched_df.write.save_as_table([SOURCE_DB, schema, "WEATHER_TEMP_COVERSION"], table_type="", mode="overwrite")

    enriched_df = add_is_snow(df)
    enriched_df = add_is_rain(df)

    enriched_df.write.save_as_table([SOURCE_DB, schema, "WEATHER_IS_SNOW_RAIN"], table_type="", mode="overwrite")
    
    enriched_df = daily_aggregations(df)
    enriched_df.write.save_as_table([SOURCE_DB, schema, "WEATHER_DAILY_AGGREGATIONS"], table_type="", mode="overwrite")

    row_counts = enriched_df.count()

    return row_counts




if __name__ == "__main__":
    print("Creating session")
    session = Session.builder.configs(get_env_var_config()).create()

    print("Running job...")
    rows = transform_tables(session, "PUBLIC", "WEATHER")

    print(f'Job complete. Number of rows created: {rows}')

