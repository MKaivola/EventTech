from collections.abc import Iterable

import pandas as pd
from sqlalchemy import Select, Connection

from Data.db_metadata import EventDataBase

def get_periods(db: EventDataBase,
                conn: Connection,
                period_names: Iterable[str]) -> pd.DataFrame:
    """
    Get fiscal period table for given periods

    Arguments
    ---------
    db:
        A database object
    conn:
        A db connection object
    period_names:
        An iterable of periods to get by period name
    """

    # Select relevant periods
    periods_stmt = (Select(db.periods.c['period_name', 'start_date', 'end_date'])
                    .where(db.periods.c.period_name.in_(period_names)))

   
    periods = pd.read_sql(periods_stmt,
                        conn)
        
    return periods

def generate_pd_periods(periods: pd.DataFrame,
                        freq: str) -> pd.DataFrame:
    """
    Generate pandas Period objects spanning each fiscal year for a given frequency

    Arguments
    ---------
    periods:
        A dataframe containing each period and its start and end dates
    freq:
        A string specifying the frequency of periods
    """

    df = periods.assign(Period = periods.apply(lambda row: pd.period_range(start=row['start_date'],
                                                                            end=row['end_date'],
                                                                            freq=freq),
                                                axis=1))

    df = df.explode('Period').drop(columns=['start_date',
                                            'end_date'])
    
    return df

def get_and_concat_periods(db: EventDataBase,
                           conn: Connection,
                           stmt: Select,
                           periods: pd.DataFrame) -> pd.DataFrame:
    """
    Get and concatenate data that is extracted for each period separately

    Arguments
    ---------
    db:
        A database object
    conn:
        A db connection object
    stmt:
        A SQLAlchemy Select object to be executed
    periods:
        A dataframe containing each period and its start and end dates
    """
           
    # Extract data for each fiscal period
    data_for_each_period = [pd.read_sql(stmt,
                                        conn,
                                        params={
                                            'start_date': row['start_date'],
                                            'end_date': row['end_date']
                                        }) for _, row in periods.iterrows()]
    
    all_data = pd.concat(data_for_each_period)

    return all_data