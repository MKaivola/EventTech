from pathlib import Path

import pandas as pd
from numpy import nan
from sqlalchemy import URL

import data.preprocess_utils as utils
from data.db_metadata import EventDataBase
from config.config import parse_db_config

events_2021_2022_filter = {'Kuvaus': ['Peruttiin']}

events_2021_2022_replace_dict = {'Keikka': {'soihtukulkue': nan, 
                                            '(Wappuriehan julistus)': nan}}

events_2021_2022_responses = utils.preprocess_event_csv('data/Tapahtumat_2021_2022.csv',
                                                        events_2021_2022_filter,
                                                        events_2021_2022_replace_dict)

events_2022_2023_replace_dict = {'Keikka': {'Vetovastuu Kuuralla': nan}}

events_2022_2023_column_rename = {'Mikko K': 'Mikko'}

events_2022_2023_responses = utils.preprocess_event_csv('data/Tapahtumat_2022_2023.csv',
                                                        replace_dict=events_2022_2023_replace_dict,
                                                        column_rename=events_2022_2023_column_rename)

events_responses = pd.concat((events_2021_2022_responses, events_2022_2023_responses))

events_colnames = {'Keikka': 'name_event', 'Paikka': 'location_event',
                   'Päiväys': 'date_event', 'Kuvaus': 'description_event'}
events = utils.df_db_preprocessing(events_responses,
                                   events_colnames,
                                   'id_event',
                                   'Keikka')

names_colnames = {'Nimi':'name_tech'}
names = utils.df_db_preprocessing(events_responses,
                                  names_colnames,
                                  'id_name',
                                  'Nimi')

jobs_colnames = {'Homma': 'name_job'}
jobs = utils.df_db_preprocessing(events_responses,
                                 jobs_colnames,
                                 'id_job',
                                 'Homma')

signups_colnames = {'Vastaus':'answer', 'Keikka': 'name_event'} | names_colnames | jobs_colnames

signups = utils.df_db_preprocessing(events_responses,
                                    signups_colnames,
                                    'id_signup')

signups = (signups.merge(events, on='name_event', validate='m:1')
           .merge(names, on='name_tech', validate='m:1')
           .merge(jobs, on='name_job', validate='m:1')
           .loc[:, ['id_signup', 'id_event', 'id_name', 'id_job',
                    'answer']]
           .rename(columns={'id_event': 'event_id',
                            'id_name': 'name_id',
                            'id_job': 'job_id'}))

config_file_path = str(Path(__file__).parent.parent / 'config' / 'config.ini')

conn_string = URL.create(**parse_db_config(config_file_path))

db = EventDataBase(conn_string)

db._create_tables(names, events, jobs, signups)


