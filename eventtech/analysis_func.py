from collections.abc import Iterable

import pandas as pd
from sqlalchemy import Select, bindparam, Connection
from sqlalchemy import func
import numpy as np
from sklearn.linear_model import LinearRegression

from data.db_metadata import EventDataBase
import eventtech.utils_analysis as utils_analysis


def monthly_event_counts(
    db: EventDataBase,
    conn: Connection,
    period_names: dict[str, set[str]],
    csv_filepath: str = None,
) -> pd.DataFrame:
    """
    Extract monthly event counts for each fiscal period

    Arguments
    ---------
    db
        A database object
    conn
        A db connection object
    period_names
        A dictionary containing (source name, period name iterable) pairs to extract
    csv_filepath
        Path to csv file
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
    periods = utils_analysis.get_periods(db, conn, period_names["db"])

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

    if "csv" in period_names and csv_filepath is not None:
        csv_period_data = utils_analysis.get_periods(db, conn, period_names["csv"])

        event_counts_csv = _monthly_event_counts_csv(csv_filepath, csv_period_data)

        event_counts = pd.concat((event_counts, event_counts_csv), axis=1)

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
        period=pd.to_datetime(event_data["date_event"], dayfirst=True).dt.to_period("M")
    )

    event_counts = pd.pivot_table(
        event_data,
        index="period",
        values="date_event",
        aggfunc="count",
    )

    event_counts = periods.join(event_counts, on="Period").fillna(0)

    event_counts["Month"] = event_counts["Period"].dt.month

    event_counts = event_counts.pivot(
        index="Month", columns="period_name", values="date_event"
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


class EventSignups:
    """
    Class representing event signup data extracted from database and csv files

    Arguments
    ---------
    db
        Database object
    conn
        Database connection
    period_names
        A dictionary containing (source name, period name iterable) pairs to extract
    jobs
        Iterable containing jobs to consider
    csv_file
        Optional csv file path
    """

    def __init__(
        self,
        db: EventDataBase,
        conn: Connection,
        period_names: dict[str, set[str]],
        jobs: Iterable[str],
        csv_file: str = None,
    ) -> None:
        period_db = utils_analysis.get_periods(db, conn, period_names["db"])

        db_data = self._event_signups_per_job(db, conn, period_db, jobs)

        self.data = db_data
        self.periods = period_db

        if csv_file is not None:
            period_csv = utils_analysis.get_periods(db, conn, period_names["csv"])
            csv_data = self._event_signups_per_job_csv(csv_file, period_csv, jobs)
            self.data = pd.concat((db_data, csv_data), axis=0)
            self.periods = pd.concat((period_db, period_csv), axis=0)

    def _event_signups_per_job(
        self,
        db: EventDataBase,
        conn: Connection,
        period_data: pd.DataFrame,
        jobs: Iterable[str],
    ):
        """
        Extract event signups for each job for each given fiscal year

        Arguments
        ---------
        db:
            A database object
        conn:
            A db connection object
        period_data:
            Dataframe containing period_names, start and end dates
        jobs:
            An iterable containing the jobs to consider
        """

        # Calculate signups for each event and given jobs in a given fiscal year
        event_signup_count_per_job_stmt = (
            Select(
                db.events.c.name_event,
                db.events.c.date_event,
                db.jobs.c.name_job,
                func.count().label("signup_count"),
            )
            .join_from(
                db.events, db.signups, db.events.c.id_event == db.signups.c.event_id
            )
            .join(db.jobs, db.jobs.c.id_job == db.signups.c.job_id)
            .where(
                bindparam("start_date") <= db.events.c.date_event,
                db.events.c.date_event <= bindparam("end_date"),
                db.jobs.c.name_job.in_(jobs),
            )
            .group_by(
                db.events.c.name_event, db.jobs.c.name_job, db.events.c.date_event
            )
        )

        periods = period_data

        event_signup_count_per_job = utils_analysis.get_and_concat_periods(
            db, conn, event_signup_count_per_job_stmt, periods
        )

        periods = utils_analysis.generate_pd_periods(periods, "D")

        event_signup_count_per_job["Period"] = pd.to_datetime(
            event_signup_count_per_job["date_event"]
        ).dt.to_period("D")

        # Merge fiscal year information to events
        event_signup_count_per_job = event_signup_count_per_job.merge(
            periods, on="Period"
        ).drop(columns="date_event")

        return event_signup_count_per_job

    def _event_signups_per_job_csv(
        self,
        file: str,
        period_data: pd.DataFrame,
        jobs: Iterable[str],
    ) -> pd.DataFrame:
        """
        Get event signups per job from a csv file

        Arguments
        ---------
        file
            Path to csv file
        period_data
            Dataframe containing period names, start and end dates that are in
            the csv file
        jobs
            Iterable containing jobs to consider
        """
        periods = utils_analysis.generate_pd_periods(period_data, "D")

        event_signup_counts = pd.read_csv(file)

        event_signup_counts["Period"] = pd.to_datetime(
            event_signup_counts["date_event"], dayfirst=True
        ).dt.to_period("D")

        event_signup_counts = event_signup_counts.merge(periods, on="Period")

        # Select columns that are in the database data
        event_signup_counts = event_signup_counts.loc[
            :,
            (
                "name_event",
                "Purku",
                "Veto",
                "Kasaus",
                "Period",
                "period_name",
            ),
        ]
        # Melt the dataframe to conform with the database counterpart
        event_signup_counts = event_signup_counts.melt(
            id_vars=["name_event", "Period", "period_name"],
            value_vars=jobs,
            var_name="name_job",
            value_name="signup_count",
        )

        return event_signup_counts

    def popular_event_signups_per_job(
        self,
        top_n_events: int,
    ) -> pd.DataFrame:
        """
        Extract top n events by total signups for each given fiscal year

        Arguments
        ---------
        top_n_events:
            An integer specifying how many top events to return
        """
        # Use MultiIndex to represent each event
        event_signup_counts = (
            self.data.pivot(
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

    def event_signup_medians_per_month(self) -> pd.DataFrame:
        """
        Compute event signup medians over jobs and months for each fiscal year
        """
        median_per_month = self.data.assign(
            month=self.data["Period"].dt.month
        ).pivot_table(
            index="month",
            columns="period_name",
            values="signup_count",
            aggfunc="median",
            fill_value=0.0,
        )

        # Format results for plotting
        median_per_month = (
            (pd.DataFrame(index=np.arange(1, 13)).join(median_per_month).fillna(0.0))
            .rename_axis(index="Month", columns="Fiscal Year")
            .stack()
            .swaplevel()
            .sort_index()
            .rename("Median signups")
        )

        return median_per_month


class LinearRegMonthly:
    def __init__(self) -> None:
        self.model = LinearRegression(fit_intercept=False)

        self.indicator_vars = []
        self.response_names = []

    def data_preprocess(
        self, series_list: Iterable[pd.Series], indicator_vars: dict[str, Iterable[str]]
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        """
        Preprocess data for linear regression
        Model has indicators for each month
        and for each relevant change in policy

        Arguments
        ---------
        series_list
            Sequence of series with MultiIndex indicating
            fiscal year and month
        indicator_vars
            Dictionary mapping indicator names to their relevant years
        Returns
        -------
        X
            Data matrix ready for fitting
        y
            Response matrix ready for fitting
        """

        response_cols = pd.concat(series_list, axis=1)

        # Months should have indicators in all models
        month_one_hot = pd.get_dummies(
            response_cols.reset_index("Month")["Month"], dtype="float"
        )

        for ind_name, years in indicator_vars.items():
            # Specify which fiscal periods have an indicator
            hot_years_series = pd.Series(np.repeat([1.0], len(years)), index=years)

            # Add new indicator column based on index
            month_one_hot[ind_name] = hot_years_series

        # All NaNs should be coded as zero
        month_one_hot = month_one_hot.fillna(0.0)

        # Preserve feature names in sklearn by typecasting to string
        month_one_hot.columns = month_one_hot.columns.astype(str)

        self.indicator_vars = list(indicator_vars.keys())

        self.response_names = response_cols.columns

        return month_one_hot, response_cols

    def formatted_coef(self) -> pd.DataFrame:
        df = pd.DataFrame(
            self.model.coef_,
            columns=self.model.feature_names_in_,
            index=self.response_names,
        )

        # Format coef dataframe for easier indicator adding
        df_original = df.drop(columns=self.indicator_vars).T

        # Typecast back to int to get natural ordering of months
        df_original.index = df_original.index.astype(int)

        for ind_name in self.indicator_vars:
            # Add indicator estimate to all coef values
            df_ind_add = df_original + df[ind_name]

        # Format dataframes for plotting purposes
        df_original = df_original.stack().swaplevel().sort_index().rename("Average")

        df_ind_add = (
            df_ind_add.stack().swaplevel().sort_index().rename("Average after change")
        )

        df_all = pd.concat([df_original, df_ind_add], axis=1).clip(lower=0.0)

        return df_all


def event_poll_durations_and_signups(
    db: EventDataBase, conn: Connection, csv_file: str, period_names: Iterable[str]
) -> pd.DataFrame:
    """
    Extract durations between polls and events and signup amounts

    """
    periods = utils_analysis.get_periods(db, conn, period_names)

    periods = utils_analysis.generate_pd_periods(periods, "D")

    event_data = pd.read_csv(csv_file)

    event_data["Period"] = pd.to_datetime(
        event_data["date_event"], dayfirst=True
    ).dt.to_period("D")

    event_data = event_data.merge(periods, on="Period")

    poll_periods = pd.to_datetime(
        event_data["poll_date"], format="%Y-%m-%dT%X"
    ).dt.to_period("D")

    event_data["poll_day_offset"] = (event_data["Period"] - poll_periods).apply(
        lambda x: x.n
    )

    event_data["active_voters"] = event_data["signup_count"] - event_data["En pääse"]

    poll_offsets_voters = event_data.set_index(["period_name", "name_event"]).loc[
        :, ("poll_day_offset", "active_voters")
    ]

    return poll_offsets_voters
