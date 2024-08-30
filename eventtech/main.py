from pathlib import Path
import os

from sqlalchemy import URL
import boto3
import pandas as pd
import numpy as np

from data.db_metadata import EventDataBase
import eventtech.analysis_func as analysis_func
import eventtech.plotting_tools as plotting_tools
from config.config import parse_db_config, parse_storage_config


config_file_path = str(Path(__file__).parent.parent / "config" / "config.ini")
conn_string = URL.create(**parse_db_config(config_file_path))

db = EventDataBase(conn_string)

storage_config = parse_storage_config(config_file_path)

plot_file_dir = os.path.expanduser(storage_config["local_plot_dir"])

storage_config["local_plot_dir"] = plot_file_dir

if "s3_bucket_name" in storage_config:
    session = boto3.Session()
    s3_client = session.client("s3")

else:
    s3_client = None

csv_file_path = (
    os.path.expanduser(storage_config["csv_file_path"])
    if "csv_file_path" in storage_config
    else None
)

with db.engine.begin() as conn:
    ### Plot how many events for each month and year ###

    fiscal_years = {"db": set(("2021-2022", "2022-2023")), "csv": set(("2023-2024",))}

    standard_jobs = ("Kasaus", "Veto", "Purku")

    event_counts = analysis_func.monthly_event_counts(
        db, conn, fiscal_years, csv_file_path
    )

    plotting_tools.barplot(
        event_counts, "Number of events", "event_counts.pdf", storage_config, s3_client
    )

    ### Analysis of technician signup data
    TechnicianSignUps = analysis_func.AllTechnicianSignups(db, conn, fiscal_years)

    ### Plot how many signups each technician has for each year ###

    annual_signup_counts = TechnicianSignUps.yearly_technician_signups()

    plotting_tools.barplot(
        annual_signup_counts,
        "Number of signups",
        "signup_counts.pdf",
        storage_config,
        s3_client=s3_client,
    )

    ### Compute and plot the distribution of technicians each fiscal year ###

    tech_annual_dist = TechnicianSignUps.technician_annual_distribution()

    plotting_tools.barplot(
        tech_annual_dist,
        "Technician distribution",
        "tech_dist.pdf",
        storage_config,
        stacked=True,
        s3_client=s3_client,
    )

    ### Analysis of event signup data

    EventSignUps = analysis_func.EventSignups(
        db, conn, fiscal_years, standard_jobs, csv_file_path
    )

    ### Plot most popular event signup counts for each job for each year ###
    signup_counts_per_event = EventSignUps.popular_event_signups_per_job(5)

    plotting_tools.outer_index_barplot(
        signup_counts_per_event,
        "top_event_signups.pdf",
        "Most popular events by signup",
        config=storage_config,
        s3_client=s3_client,
        nrows=1,
        ncols=signup_counts_per_event.index.get_level_values(0).unique().__len__(),
    )

    ### Compute median signups across jobs and months

    median_signups_monthly = EventSignUps.event_signup_medians_per_month()

    event_counts_stacked = (
        event_counts.stack().swaplevel().sort_index().rename("Event counts")
    )

    plotting_tools.outer_index_barplot(
        event_counts_stacked,
        "event_counts_per_year.pdf",
        "Event counts for each fiscal year",
        storage_config,
        df_line_plot=median_signups_monthly,
        nrows=1,
        ncols=event_counts_stacked.index.get_level_values(0).unique().__len__(),
    )

    ### Model monthly event counts and median signups via linear regression

    monthly_series = [median_signups_monthly, event_counts_stacked]

    change_indicators = {
        "Delegation": pd.MultiIndex.from_product([("2023-2024",), np.arange(1, 13)])
    }

    change_indicators_monthly = {
        "_".join(["Delegation", str(i)]): pd.MultiIndex.from_tuples([("2023-2024", i)])
        for i in range(1, 13)
    }

    monthly_model = analysis_func.LinearRegMonthly()

    event_signups_changes = monthly_model.fit_and_get_coef(
        monthly_series, change_indicators
    )

    plotting_tools.outer_index_barplot(
        event_signups_changes,
        "LinearRegression/changes_in_events_and_signups.pdf",
        "Linear model effect estimate",
        storage_config,
        nrows=1,
        ncols=event_signups_changes.index.get_level_values(0).unique().__len__(),
    )

    event_signups_changes = monthly_model.fit_and_get_coef(
        monthly_series, change_indicators_monthly
    )

    plotting_tools.outer_index_barplot(
        event_signups_changes,
        "LinearRegression/changes_in_events_and_signups_monthly.pdf",
        "Linear model effect estimate",
        storage_config,
        nrows=1,
        ncols=event_signups_changes.index.get_level_values(0).unique().__len__(),
    )
