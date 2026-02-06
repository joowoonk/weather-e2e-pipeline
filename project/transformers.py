from snowflake.snowpark.dataframe import DataFrame, col
from snowflake.snowpark.functions import to_timestamp, to_date, hour

def transform_weather(df: DataFrame) -> DataFrame:
	"""
	Apply common weather dataset transformations and return the transformed DataFrame.
	This keeps transformations modular so `sproc.py` can import and reuse them.
	"""
	df = df.with_column("OBSERVATION_TS", to_timestamp(col("OBSERVATION_TIME")))
	df = df.with_column("OBSERVATION_DATE", to_date(col("OBSERVATION_TS"))).with_column(
		"OBSERVATION_HOUR", hour(col("OBSERVATION_TS"))
	)
	return df


