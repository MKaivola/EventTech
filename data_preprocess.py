import pandas as pd

events_2021_2022 = pd.read_csv("Data/Tapahtumat_2021_2022.csv")

colNames_2021_2022 = events_2021_2022.columns

unnamedCols_2021_2022 = colNames_2021_2022[colNames_2021_2022.str.contains("Unnamed", regex=False)]

# Drop unnamed columns and empty rows
events_2021_2022_proc = (events_2021_2022.drop(unnamedCols_2021_2022, axis=1)
                         .dropna(axis=0, how='all', subset='Homma')
                         .drop("Aikaikkuna", axis=1))

# Fill relevant columns
fillcols = ['Keikka', 'Paikka', 'Päiväys', 'Kuvaus']
events_2021_2022_proc.loc[:, fillcols] = events_2021_2022_proc.loc[:, fillcols].ffill(axis=0)

# Transform date column to datetime format
events_2021_2022_proc.loc[:, 'Päiväys'] = pd.to_datetime(events_2021_2022_proc.loc[: ,'Päiväys'], 
                                                         dayfirst=True,
                                                         format="mixed")

# Melt the table into a response format: Name columns are transformed into (Name, Response) pairs
# Filter empty responses
events_2021_2022_responses = (events_2021_2022_proc.melt(id_vars=['Keikka','Paikka',
                                                                 'Päiväys', 'Homma',
                                                                 'Kuvaus'],
                                                                 var_name='Nimi',
                                                                 value_name='Vastaus')
                                                                 .dropna(axis=0, 
                                                                        how='all',
                                                                        subset='Vastaus'))
# Filter canceled events
kuvaus_filter = events_2021_2022_responses['Kuvaus'] == "Peruttiin"
kuvaus_filter = events_2021_2022_responses[kuvaus_filter].index

events_2021_2022_responses = events_2021_2022_responses.drop(index=kuvaus_filter)

                                                         

