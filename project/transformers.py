from snowflake.snowpark.dataframe import DataFrame, col
from snowflake.snowpark.functions import to_timestamp, to_date, hour, avg, min, max, sum, when

def transform_by_hourly_and_date(df: DataFrame) -> DataFrame:
	"""
	Apply common weather dataset transformations and return the transformed DataFrame.
	This keeps transformations modular so `sproc.py` can import and reuse them.
	"""
	df = df.with_column("OBSERVATION_TS", to_timestamp(col("OBSERVATION_TIME")))
	df = df.with_column("OBSERVATION_DATE", to_date(col("OBSERVATION_TS"))).with_column(
		"OBSERVATION_HOUR", hour(col("OBSERVATION_TS"))
	)
	return df

def tranform_from_f_to_c(df: DataFrame) -> DataFrame:
    """
    Example of another transformation function that could be added to this module.
    """
    df = df.with_column("TEMPERATURE_C", (col("TEMPERATURE_F") - 32) * 5.0/9.0)


    return df

def add_is_snow(df: DataFrame) -> DataFrame: 
    """
    Example of another transformation function that could be added to this module.
    """
    df = df.with_column("IS_SNOW", col("CONDITION") == "Snow")

    return df

def add_is_rain(df: DataFrame) -> DataFrame: 
    """
    Example of another transformation function that could be added to this module.
    """
    df = df.with_column("IS_RAIN", col("CONDITION") == "Rain")

    return df

def daily_aggregations(df: DataFrame) -> DataFrame:
    """
    Example of another transformation function that could be added to this module.
    """
    df = df.with_column(
        "IS_RAIN",
        when(col("CONDITION") == "Rain", 1).otherwise(0)
    ).with_column(
        "IS_SNOW",
        when(col("CONDITION") == "Snow", 1).otherwise(0)
    )

    daily_df = df.group_by(
        "CITY_ID",
        "CITY_NAME",
        "OBSERVATION_DATE"
    ).agg(
        avg("TEMPERATURE_F").alias("AVG_TEMPERATURE_F"),
        min("TEMPERATURE_F").alias("MIN_TEMPERATURE_F"),
        max("TEMPERATURE_F").alias("MAX_TEMPERATURE_F"),
        sum("IS_RAIN").alias("RAIN_HOURS"),
        sum("IS_SNOW").alias("SNOW_HOURS")
    )

    return daily_df