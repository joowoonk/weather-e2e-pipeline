from project.utils import get_env_var_config
from snowflake.snowpark.session import Session


def set_up_tables(session: Session):
    DB = 'WEATHER_DB'
    SCHEMA = 'PUBLIC'
    TEST_SCHEMA = 'TEST'

    # Create the DB and Schema
    session.sql(F'CREATE DATABASE IF NOT EXISTS {DB}').collect()
    session.sql(F'CREATE SCHEMA IF NOT EXISTS {DB}.{SCHEMA}').collect()
    session.sql(F'CREATE SCHEMA IF NOT EXISTS {DB}.{TEST_SCHEMA}').collect()

    table_builder = session.create_dataframe(
    data=[
        # Seattle
        [1, 'Seattle', '2026-02-04 00:00:00.000 +0000', 41.0, 63, 4.5, 'Clear'],
        [1, 'Seattle', '2026-02-04 06:00:00.000 +0000', 39.2, 68, 3.9, 'Fog'],
        [1, 'Seattle', '2026-02-04 12:00:00.000 +0000', 45.5, 55, 5.0, 'Clouds'],
        [1, 'Seattle', '2026-02-04 18:00:00.000 +0000', 43.8, 60, 5.2, 'Rain'],
        [1, 'Seattle', '2026-02-05 00:00:00.000 +0000', 40.3, 64, 4.7, 'Clear'],
        [1, 'Seattle', '2026-02-05 06:00:00.000 +0000', 38.9, 66, 4.2, 'Fog'],
        [1, 'Seattle', '2026-02-05 12:00:00.000 +0000', 46.1, 53, 5.5, 'Clouds'],
        [1, 'Seattle', '2026-02-05 18:00:00.000 +0000', 44.2, 59, 5.3, 'Rain'],

        # Dallas
        [2, 'Dallas', '2026-02-04 00:00:00.000 +0000', 52.0, 48, 6.1, 'Clouds'],
        [2, 'Dallas', '2026-02-04 06:00:00.000 +0000', 50.3, 50, 5.9, 'Fog'],
        [2, 'Dallas', '2026-02-04 12:00:00.000 +0000', 58.2, 42, 6.5, 'Clear'],
        [2, 'Dallas', '2026-02-04 18:00:00.000 +0000', 55.7, 45, 6.8, 'Clouds'],
        [2, 'Dallas', '2026-02-05 00:00:00.000 +0000', 53.1, 47, 6.2, 'Clear'],
        [2, 'Dallas', '2026-02-05 06:00:00.000 +0000', 51.8, 49, 5.7, 'Clouds'],
        [2, 'Dallas', '2026-02-05 12:00:00.000 +0000', 60.3, 40, 6.0, 'Clear'],
        [2, 'Dallas', '2026-02-05 18:00:00.000 +0000', 57.8, 43, 6.4, 'Clouds'],

        # New York
        [3, 'New York', '2026-02-04 00:00:00.000 +0000', 36.5, 70, 5.0, 'Snow'],
        [3, 'New York', '2026-02-04 06:00:00.000 +0000', 35.2, 72, 4.8, 'Snow'],
        [3, 'New York', '2026-02-04 12:00:00.000 +0000', 39.1, 65, 5.5, 'Clouds'],
        [3, 'New York', '2026-02-04 18:00:00.000 +0000', 37.6, 68, 5.2, 'Snow'],
        [3, 'New York', '2026-02-05 00:00:00.000 +0000', 34.8, 71, 5.1, 'Fog'],
        [3, 'New York', '2026-02-05 06:00:00.000 +0000', 33.9, 73, 4.9, 'Snow'],
        [3, 'New York', '2026-02-05 12:00:00.000 +0000', 41.0, 64, 5.8, 'Clouds'],
        [3, 'New York', '2026-02-05 18:00:00.000 +0000', 42.5, 60, 5.0, 'Clear']
    ],
    schema=[
        'CITY_ID',
        'CITY_NAME',
        'OBSERVATION_TIME',
        'TEMPERATURE_F',
        'HUMIDITY_PCT',
        'WIND_SPEED_MPH',
        'CONDITION']
    )


    table_builder.write.mode('overwrite').save_as_table([DB, SCHEMA, 'WEATHER'], mode='overwrite')


def main():
    print('Creating session from environment variables')
    session = Session.builder.configs(get_env_var_config()).create()

    print('Creating tables...')
    set_up_tables(session=session)



if __name__ == '__main__':
    main()
