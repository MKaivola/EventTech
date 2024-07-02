from collections.abc import Iterable

import pandas as pd
from sqlalchemy import Select, bindparam
from sqlalchemy import func

from Data.db_metadata import EventDataBase
import utils_analysis

def monthly_event_counts(db: EventDataBase,
                         period_names: Iterable[str]) -> pd.DataFrame:
    """
    Extract monthly event counts for each fiscal period

    Arguments
    ---------
    db:
        A database object
    period_names:
        An iterable containing the period names to extract
    """

    # Extract year and month component of each event date, 
    # selecting events for a given fiscal period
    events_sbq = (Select(func.extract('year', db.events.c.date_event).label('year'),
                        func.extract('month', db.events.c.date_event).label('month'))
                        .where(bindparam('start_date') <= db.events.c.date_event,
                                   db.events.c.date_event <= bindparam('end_date'))
                        .subquery())

    # Calculate how many events fall to each month-year
    events_per_month_stmt = (Select(events_sbq.c['year', 'month'], func.count().label('event_count'))
                            .group_by(events_sbq.c['year', 'month'])
                            .order_by(events_sbq.c['year', 'month'])
                            )
    
    # Select relevant periods
    periods = utils_analysis.get_periods(db,
                                         period_names)

    events_per_month = utils_analysis.get_and_concat_periods(db,
                                                             events_per_month_stmt,
                                                             periods)

    # Construct all monthly periods for each fiscal year
    periods = utils_analysis.generate_pd_periods(periods,
                                                 'M')

    # Construct period objects for each month year pair
    periods_in_data = (pd.to_datetime(events_per_month.drop(columns='event_count')
                                      .assign(day = 1)).dt.to_period('M'))

    events_per_month = (events_per_month.assign(period = periods_in_data)
                        .loc[:, ['period', 'event_count']].set_index('period'))

    # Join all periods with periods containing data
    event_counts = periods.join(events_per_month, on='Period').fillna(0)

    event_counts['Month'] = event_counts['Period'].dt.month

    # Pivot so that each fiscal year has a separate column
    event_counts = (event_counts.pivot(index='Month',
                                    columns='period_name',
                                    values='event_count')
                                    .rename_axis(columns='Fiscal Year'))
    
    return event_counts

def yearly_technician_signups(db: EventDataBase,
                            period_names:
                            Iterable[str]) -> pd.DataFrame:
    """
    Extract yearly signup counts for each technician

    Arguments
    ---------
    db:
        A database object
    period_names:
        An iterable containing the period names to extract
    """

    # Select each technician and event date from signup data for a given period
    signups_for_period_stmt = (Select(db.names.c.name_tech,
                                db.events.c.date_event)
                                .join_from(db.signups, db.names, 
                                           db.signups.c.name_id == db.names.c.id_name)
                                .join(db.events, 
                                      db.events.c.id_event == db.signups.c.event_id)
                                .where(bindparam('start_date') <= db.events.c.date_event,
                                   db.events.c.date_event <= bindparam('end_date')))
    
    periods = utils_analysis.get_periods(db,
                                         period_names)
    
    signups = utils_analysis.get_and_concat_periods(db,
                                                    signups_for_period_stmt,
                                                    periods)
    
    periods = utils_analysis.generate_pd_periods(periods,
                                                 'D')
    
    # Add Period objects corresponding to date_event for a merge
    signups['Period'] = pd.to_datetime(signups['date_event']).dt.to_period('D')

    signups = signups.merge(periods, 
                            on='Period')
    
    # Count the number of signups for each technician and fiscal year
    signup_counts = (pd.pivot_table(signups,
                                   index='name_tech',
                                   columns='period_name',
                                   values='Period',
                                   aggfunc='count',
                                   fill_value=0)
                                   .rename_axis(columns='Fiscal Year'))
    
    return signup_counts
