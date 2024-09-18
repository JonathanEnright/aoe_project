import pandas as pd
from datetime import datetime

START_YEAR = 2020
END_YEAR = 2029

def model(dbt, session):
    """Generates a date dimension table"""

    # Define the start and end dates for the dimension table
    start_date = datetime(START_YEAR, 1, 1)
    end_date = datetime(END_YEAR, 12, 31)

    # Generate a date range
    date_range = pd.date_range(start=start_date, end=end_date, freq='D')

    dim_date_df = pd.DataFrame({
        'date': date_range,
        'year': date_range.year,
        'month': date_range.month,
        'day': date_range.day,
        'day_of_week': date_range.dayofweek  # 0=Monday, 6=Sunday
    })

    dim_date_df['is_weekend'] = dim_date_df['day_of_week'].isin([5, 6])

    # Convert all column names to uppercase, to make case insensitve in Snowflake
    dim_date_df = dim_date_df.rename(columns=str.upper)

    return dim_date_df