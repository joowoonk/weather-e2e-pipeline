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
            [1, 'Seattle', '2026-02-04 20:00:00.000 +0000', 42.3, 61, 5.1, 'Clear'],
            [2, 'Dallas',  '2026-02-04 20:00:00.000 +0000', 55.2, 48, 7.3, 'Clouds'],
            [3, 'New York','2026-02-04 20:00:00.000 +0000', 38.7, 70, 4.8, 'Snow'],
            [1, 'Seattle', '2026-02-04 21:00:00.000 +0000', 43.1, 59, 5.5, 'Clear'],
            [2, 'Dallas',  '2026-02-04 21:00:00.000 +0000', 54.8, 50, 6.9, 'Clouds'],
            [3, 'New York','2026-02-04 21:00:00.000 +0000', 39.0, 68, 5.0, 'Snow'],
            [1, 'Seattle', '2026-02-04 22:00:00.000 +0000', 41.9, 62, 5.0, 'Rain'],
            [2, 'Dallas',  '2026-02-04 22:00:00.000 +0000', 53.5, 52, 7.1, 'Clouds'],
            [3, 'New York','2026-02-04 22:00:00.000 +0000', 38.5, 71, 4.7, 'Snow']
        ],
        schema=[
            'CITY_ID',       
            'CITY_NAME',
            'OBSERVATION_TIME',
            'TEMPERATURE_F',
            'HUMIDITY_PCT',
            'WIND_SPEED_MPH',
            'CONDITION'
        ]
    )

    table_builder.write.mode('overwrite').save_as_table([DB, SCHEMA, 'WEATHER'], mode='overwrite')


def get_table(session: Session):
    return session.table('WEATHER_DB.PUBLIC.WEATHER').show()



def main():
    print('Creating session from environment variables')
    session = Session.builder.configs(get_env_var_config()).create()

    print('Creating tables...')
    set_up_tables(session=session)
    get_table(session=session)


if __name__ == '__main__':
    main()
