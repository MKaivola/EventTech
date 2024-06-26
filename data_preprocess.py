import pandas as pd

import preprocess_utils as utils

events_2021_2022_filter = {'Kuvaus': ['Peruttiin']}

events_2021_2022_nan_mask = {'Keikka': ['soihtukulkue', '(Wappuriehan julistus)']}

events_2021_2022_responses = utils.preprocess_event_csv('Data/Tapahtumat_2021_2022.csv',
                                                        events_2021_2022_filter,
                                                        events_2021_2022_nan_mask)

events_2022_2023_nan_mask = {'Keikka': ['Vetovastuu Kuuralla']}

events_2022_2023_responses = utils.preprocess_event_csv('Data/Tapahtumat_2022_2023.csv',
                                                        nan_mask=events_2022_2023_nan_mask)