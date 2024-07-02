import pandas as pd
from sqlalchemy import Select
from sqlalchemy import func

from Data.db_metadata import EventDataBase

def monthly_event_counts(db: EventDataBase) -> pd.DataFrame:
    """
    Extract monthly event counts for each fiscal period

    Arguments
    ---------
    db:
        A database object
    """

    # Extract year and month component of each event date
    events_sbq = (Select(func.extract('year', db.events.c.date_event).label('year'),
                        func.extract('month', db.events.c.date_event).label('month'))
                        .subquery())

    # Calculate how many events fall to each month-year
    events_per_month_stmt = (Select(events_sbq.c['year', 'month'], func.count().label('event_count'))
                            .group_by(events_sbq.c['year', 'month'])
                            .order_by(events_sbq.c['year', 'month'])
                            )
    with db.engine.begin() as conn:
        events_per_month = pd.read_sql(events_per_month_stmt,
                                    conn)
        periods = pd.read_sql(Select(db.periods.c['period_name', 'start_date', 'end_date']),
                            conn)

    # Construct all monthly periods for each fiscal year
    periods['Month'] = periods.apply(lambda row: pd.period_range(start=row['start_date'],
                                                                end=row['end_date'],
                                                                freq='M'),
                                    axis=1)

    periods = periods.explode('Month').drop(columns=['start_date',
                                                    'end_date'])

    # Construct period objects for each month year pair
    periods_in_data = pd.to_datetime(events_per_month.drop(columns='event_count').assign(day = 1)).dt.to_period('M')

    events_per_month = (events_per_month.assign(period = periods_in_data)
                        .loc[:, ['period', 'event_count']].set_index('period'))

    # Join all periods with periods containing data
    event_counts = periods.join(events_per_month, on='Month').fillna(0)

    event_counts['Month'] = event_counts['Month'].dt.month

    # Pivot so that each fiscal year has a separate column
    event_counts = (event_counts.pivot(index='Month',
                                    columns='period_name',
                                    values='event_count')
                                    .rename_axis(columns='Period'))
    
    return event_counts