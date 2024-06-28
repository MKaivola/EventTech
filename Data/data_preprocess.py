import pandas as pd
from numpy import nan

import preprocess_utils as utils
from db_metadata import EventDataBase

events_2021_2022_filter = {'Kuvaus': ['Peruttiin']}

events_2021_2022_replace_dict = {'Keikka': {'soihtukulkue': nan, 
                                            '(Wappuriehan julistus)': nan}}

events_2021_2022_responses = utils.preprocess_event_csv('Data/Tapahtumat_2021_2022.csv',
                                                        events_2021_2022_filter,
                                                        events_2021_2022_replace_dict)

events_2022_2023_replace_dict = {'Keikka': {'Vetovastuu Kuuralla': nan}}

events_2022_2023_column_rename = {'Mikko K': 'Mikko'}

events_2022_2023_responses = utils.preprocess_event_csv('Data/Tapahtumat_2022_2023.csv',
                                                        replace_dict=events_2022_2023_replace_dict,
                                                        column_rename=events_2022_2023_column_rename)

events_responses = pd.concat((events_2021_2022_responses, events_2022_2023_responses))

