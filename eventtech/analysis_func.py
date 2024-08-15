from collections.abc import Iterable

import pandas as pd
from sqlalchemy import Select, bindparam, Connection
from sqlalchemy import func

from data.db_metadata import EventDataBase
import eventtech.utils_analysis as utils_analysis


def monthly_event_counts(
    db: EventDataBase, conn: Connection, period_names: Iterable[str]
) -> pd.DataFrame:
    """
    Extract monthly event counts for each fiscal period

    Arguments
    ---------
    db:
        A database object
    conn:
        A db connection object
    period_names:
        An iterable containing the period names to extract
    """

    # Extract year and month component of each event date,
    # selecting events for a given fiscal period
    events_sbq = (
        Select(
            func.extract("year", db.events.c.date_event).label("year"),
            func.extract("month", db.events.c.date_event).label("month"),
        )
        .where(
            bindparam("start_date") <= db.events.c.date_event,
            db.events.c.date_event <= bindparam("end_date"),
        )
        .subquery()
    )

    # Calculate how many events fall to each month-year
    events_per_month_stmt = (
        Select(events_sbq.c["year", "month"], func.count().label("event_count"))
        .group_by(events_sbq.c["year", "month"])
        .order_by(events_sbq.c["year", "month"])
    )

    # Select relevant periods
    periods = utils_analysis.get_periods(db, conn, period_names)

    events_per_month = utils_analysis.get_and_concat_periods(
        db, conn, events_per_month_stmt, periods
    )

    # Construct all monthly periods for each fiscal year
    periods = utils_analysis.generate_pd_periods(periods, "M")

    # Construct period objects for each month year pair
    periods_in_data = pd.to_datetime(
        events_per_month.drop(columns="event_count").assign(day=1)
    ).dt.to_period("M")

    events_per_month = (
        events_per_month.assign(period=periods_in_data)
        .loc[:, ["period", "event_count"]]
        .set_index("period")
    )

    # Join all periods with periods containing data
    event_counts = periods.join(events_per_month, on="Period").fillna(0)

    event_counts["Month"] = event_counts["Period"].dt.month

    # Pivot so that each fiscal year has a separate column
    event_counts = event_counts.pivot(
        index="Month", columns="period_name", values="event_count"
    ).rename_axis(columns="Fiscal Year")

    return event_counts


def _monthly_event_counts_csv(file: str, period_data: pd.DataFrame) -> pd.DataFrame:
    """
    Extract monthly event counts for each fiscal year from a csv file

    Arguments
    ---------
    file
        Filepath to csv file
    period_data
        Dataframe containing period names, start and end dates that are in
        the csv file
    """
    periods = utils_analysis.generate_pd_periods(period_data, "M")

    event_data = pd.read_csv(file)

    event_data = event_data.assign(
        period=pd.to_datetime(event_data["event_date"], dayfirst=True).dt.to_period("M")
    )

    event_counts = pd.pivot_table(
        event_data,
        index="period",
        values="event_date",
        aggfunc="count",
    )

    event_counts = periods.join(event_counts, on="Period").fillna(0)

    event_counts["Month"] = event_counts["Period"].dt.month

    event_counts = event_counts.pivot(
        index="Month", columns="period_name", values="event_date"
    ).rename_axis(columns="Fiscal Year")

    return event_counts


def yearly_technician_signups(
    db: EventDataBase, conn: Connection, period_names: Iterable[str]
) -> pd.DataFrame:
    """
    Extract yearly signup counts for each technician

    Arguments
    ---------
    db:
        A database object
    conn:
        A db connection object
    period_names:
        An iterable containing the period names to extract
    """

    # Select each technician and event date from signup data for a given period
    signups_for_period_stmt = (
        Select(db.names.c.name_tech, db.events.c.date_event)
        .join_from(db.signups, db.names, db.signups.c.name_id == db.names.c.id_name)
        .join(db.events, db.events.c.id_event == db.signups.c.event_id)
        .where(
            bindparam("start_date") <= db.events.c.date_event,
            db.events.c.date_event <= bindparam("end_date"),
        )
    )

    periods = utils_analysis.get_periods(db, conn, period_names)

    signups = utils_analysis.get_and_concat_periods(
        db, conn, signups_for_period_stmt, periods
    )

    periods = utils_analysis.generate_pd_periods(periods, "D")

    # Add Period objects corresponding to date_event for a merge
    signups["Period"] = pd.to_datetime(signups["date_event"]).dt.to_period("D")

    signups = signups.merge(periods, on="Period")

    # Count the number of signups for each technician and fiscal year
    signup_counts = pd.pivot_table(
        signups,
        index="name_tech",
        columns="period_name",
        values="Period",
        aggfunc="count",
        fill_value=0,
    ).rename_axis(columns="Fiscal Year")

    return signup_counts


def popular_event_signups_per_job(
    db: EventDataBase,
    conn: Connection,
    period: Iterable[str],
    jobs: Iterable[str],
    top_n_events: int,
) -> pd.DataFrame:
    """
    Extract top n events by total signups for each given fiscal year

    Arguments
    ---------
    db:
        A database object
    conn:
        A db connection object
    period:
        An iterable containing the period names to extract
    jobs:
        An iterable containing the jobs to consider
    top_n_events:
        An integer specifying how many top events to return
    """

    # Calculate signups for each event and given jobs in a given fiscal year
    event_signup_count_per_job_stmt = (
        Select(
            db.events.c.name_event,
            db.events.c.date_event,
            db.jobs.c.name_job,
            func.count().label("signup_count"),
        )
        .join_from(db.events, db.signups, db.events.c.id_event == db.signups.c.event_id)
        .join(db.jobs, db.jobs.c.id_job == db.signups.c.job_id)
        .where(
            bindparam("start_date") <= db.events.c.date_event,
            db.events.c.date_event <= bindparam("end_date"),
            db.jobs.c.name_job.in_(jobs),
        )
        .group_by(db.events.c.name_event, db.jobs.c.name_job, db.events.c.date_event)
    )

    periods = utils_analysis.get_periods(db, conn, period)

    event_signup_count_per_job = utils_analysis.get_and_concat_periods(
        db, conn, event_signup_count_per_job_stmt, periods
    )

    periods = utils_analysis.generate_pd_periods(periods, "D")

    event_signup_count_per_job["Period"] = pd.to_datetime(
        event_signup_count_per_job["date_event"]
    ).dt.to_period("D")

    # Merge fiscal year information to events
    event_signup_count_per_job = event_signup_count_per_job.merge(periods, on="Period")

    # Use MultiIndex to represent each event
    event_signup_counts = (
        event_signup_count_per_job.pivot(
            index=["period_name", "name_event"],
            columns="name_job",
            values="signup_count",
        )
        .rename_axis(columns="Job")
        .fillna(0)
    )

    # Calculate total signups, group by fiscal year and extract top events
    # by total signup for each fiscal year
    event_sums = (
        event_signup_counts.sum(axis=1)
        .groupby(level="period_name", group_keys=False)
        .nlargest(top_n_events)
    )

    event_signup_counts = event_signup_counts.loc[event_sums.index, :]

    return event_signup_counts
