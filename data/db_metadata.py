from sqlalchemy import create_engine, Table, Column, MetaData, ForeignKey, Insert
from sqlalchemy import Integer, String, Date, URL
import pandas as pd


class EventDataBase:
    def __init__(self, connection_url: URL) -> None:
        self.engine = create_engine(connection_url)

        self.metadata = MetaData()

        self.names = Table(
            "Names",
            self.metadata,
            Column("id_name", Integer, primary_key=True),
            Column("name_tech", String),
        )

        self.events = Table(
            "Events",
            self.metadata,
            Column("id_event", Integer, primary_key=True),
            Column("name_event", String),
            Column("date_event", Date),
            Column("location_event", String),
            Column("description_event", String),
        )

        self.jobs = Table(
            "Jobs",
            self.metadata,
            Column("id_job", Integer, primary_key=True),
            Column("name_job", String),
        )

        self.signups = Table(
            "Signups",
            self.metadata,
            Column("id_signup", Integer, primary_key=True),
            Column("event_id", Integer, ForeignKey("Events.id_event")),
            Column("job_id", Integer, ForeignKey("Jobs.id_job")),
            Column("name_id", Integer, ForeignKey("Names.id_name")),
            Column("answer", String),
        )

        self.periods = Table(
            "Periods",
            self.metadata,
            Column("id_period", Integer, primary_key=True),
            Column("period_name", String),
            Column("start_date", Date),
            Column("end_date", Date),
        )

    def _create_tables(
        self,
        df_names: pd.DataFrame,
        df_events: pd.DataFrame,
        df_jobs: pd.DataFrame,
        df_signups: pd.DataFrame,
    ):
        self.metadata.drop_all(self.engine)
        self.metadata.create_all(self.engine)

        db_data_pairs = zip(
            (df_names, df_events, df_jobs, df_signups),
            (self.names, self.events, self.jobs, self.signups),
        )

        with self.engine.begin() as conn:
            for db_data, db in db_data_pairs:
                conn.execute(Insert(db), db_data.to_dict(orient="records"))

        with self.engine.begin() as conn:
            conn.execute(
                Insert(self.periods),
                [
                    {
                        "id_period": 1,
                        "period_name": "2021-2022",
                        "start_date": pd.Timestamp(year=2021, month=7, day=1),
                        "end_date": pd.Timestamp(year=2022, month=6, day=30),
                    },
                    {
                        "id_period": 2,
                        "period_name": "2022-2023",
                        "start_date": pd.Timestamp(year=2022, month=7, day=1),
                        "end_date": pd.Timestamp(year=2023, month=6, day=30),
                    },
                ],
            )
