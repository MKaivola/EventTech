import pandas as pd

from collections.abc import Iterable

def preprocess_event_csv(path_to_csv: str, 
                         filter: dict[str, Iterable[str]] | None = None,
                         replace_dict: dict[str, dict[str, str]] | None = None,
                         column_rename: dict[str, str] | None = None) -> pd.DataFrame:
    """
    Preprocess event data csv file to the desired format

    Arguments
    ---------
    path_to_csv:
        A string specifying the path to the event csv file
    filter:
        A dictionary containing (column name, values) pairs to filter out rows
    replace_dict:
        A nested dictionary. Outer dict specifies column, inner dict specifies
        a replacement value for a given value inside a given column
    column_rename
        A dictionary specifying (column_old, column_new) label pairs
    """

    df = pd.read_csv(path_to_csv)

    df_colNames = df.columns

    df_unnamedColumns = df_colNames[df_colNames.str.contains('Unnamed', regex=False)]

    # Drop unnamed columns, unused columns and empty rows
    df_proc = (df.drop(df_unnamedColumns, axis=1)
                .drop('Aikaikkuna', axis=1)
                .dropna(axis=0, how='all', subset='Homma'))

    # Replace values in each column before forward filling
    if replace_dict is not None:
        df_proc = df_proc.replace(replace_dict)

    # Rename columns
    if column_rename is not None:
        df_proc = df_proc.rename(columns=column_rename)
    
    # Fill relevant columns
    fillcols = ['Keikka', 'Paikka', 'Päiväys', 'Kuvaus']
    df_proc.loc[:, fillcols] = df_proc.loc[:, fillcols].ffill(axis=0)

    # Filter rows based on column specific values
    if filter is not None:
        for column, values in filter.items():
            bool_where = df_proc[column].isin(values)
            index_to_drop = df_proc.loc[bool_where,:].index

            df_proc = df_proc.drop(index=index_to_drop)

    # Transform date column to datetime format
    df_proc['Päiväys'] = pd.to_datetime(df_proc.loc[: ,'Päiväys'], 
                                                            dayfirst=True,
                                                            format='mixed')
    
    # Concatenate year at the end of every event
    df_proc.loc[:, 'Keikka'] = (df_proc.loc[:, 'Keikka'].str.cat(
        df_proc.loc[:, 'Päiväys'].dt.year.astype('string'), sep=" ")
        )

    # Melt the table into a response format: Name columns are transformed into (Name, Response) pairs
    # Filter empty responses
    df_responses = (df_proc.melt(id_vars=['Keikka',
                                        'Paikka',
                                        'Päiväys', 
                                        'Homma',
                                        'Kuvaus'],
                                        var_name='Nimi',
                                        value_name='Vastaus')
                                        .dropna(axis=0, 
                                                how='all',
                                                subset='Vastaus'))
    
    # Clean the answers

    df_responses.loc[:, 'Vastaus'] = df_responses.loc[:, 'Vastaus'].str.strip().str.lower()

    return df_responses

def df_db_preprocessing(dataframe: pd.DataFrame, colnames: dict[str,str],
                        id_name: str, duplicate_col: str | None = None) -> pd.DataFrame:
    """
    Preprocess dataframes into a format suitable for database insertion

    Arguments
    ---------
    dataframe
        A pandas dataframe to be processed
    colnames
        A dictionary containing (colname, db_colname) pairs, colnames
        is the original column name and db_colname the corresponding db column name
    id_name
        A string specifying the database index column name
    duplicate_col
        An optional column label which is used to filter for duplicates
    """

    df = dataframe.loc[:, list(colnames)]

    if duplicate_col is not None:
        df = df.drop_duplicates(subset=duplicate_col, ignore_index=True)
    
    df = (df.reset_index(drop=True).rename_axis(index=id_name).reset_index()
          .rename(columns=colnames))
    
    return df
