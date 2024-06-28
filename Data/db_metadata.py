from sqlalchemy import create_engine, Table, Column, MetaData, ForeignKey, Insert, Select
from sqlalchemy import Integer, String, DateTime
import pandas as pd

class EventDataBase():

    def __init__(self, connection_url: str) -> None:

        self.engine = create_engine(connection_url)

        self.metadata = MetaData()

        self.names = Table('Names',
                           self.metadata,
                           Column('id_name', Integer, primary_key=True),
                           Column('name_tech', String))
        self.events = Table('Events',
                            self.metadata,
                            Column('id_event', Integer, primary_key=True),
                            Column('name_event', String),
                            Column('date_event', DateTime),
                            Column('location_event', String),
                            Column('description_event', String))
        self.jobs = Table('Jobs',
                          self.metadata,
                          Column('id_job', Integer, primary_key=True),
                          Column('name_job', String))
        self.signups = Table('Signups',
                             self.metadata,
                             Column('id_signup', Integer, primary_key=True),
                             Column('event_id', Integer, ForeignKey('Events.id_event')),
                             Column('job_id', Integer, ForeignKey('Jobs.id_job')),
                             Column('name_id', Integer, ForeignKey('Names.id_name')),
                             Column('answer', String))
    
    def _create_tables(self, df_names: pd.DataFrame,
                       df_events: pd.DataFrame,
                       df_jobs: pd.DataFrame,
                       df_signups: pd.DataFrame):
        
        self.metadata.drop_all(self.engine)
        self.metadata.create_all(self.engine)

        db_data_pairs = zip((df_names, df_events, df_jobs, df_signups),
                            (self.names, self.events, self.jobs, self.signups))

        with self.engine.begin() as conn:

            for db_data, db in db_data_pairs:
                conn.execute(Insert(db),
                             db_data.to_dict(orient='records'))