import re
import json
from collections.abc import Iterable

import pandas as pd


def preprocess_event_csv(
    path_to_csv: str,
    filter: dict[str, Iterable[str]] | None = None,
    replace_dict: dict[str, dict[str, str]] | None = None,
    column_rename: dict[str, str] | None = None,
) -> pd.DataFrame:
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

    df_unnamedColumns = df_colNames[df_colNames.str.contains("Unnamed", regex=False)]

    # Drop unnamed columns, unused columns and empty rows
    df_proc = (
        df.drop(df_unnamedColumns, axis=1)
        .drop("Aikaikkuna", axis=1)
        .dropna(axis=0, how="all", subset="Homma")
    )

    # Replace values in each column before forward filling
    if replace_dict is not None:
        df_proc = df_proc.replace(replace_dict)

    # Rename columns
    if column_rename is not None:
        df_proc = df_proc.rename(columns=column_rename)

    # Fill relevant columns
    fillcols = ["Keikka", "Paikka", "Päiväys", "Kuvaus"]
    df_proc.loc[:, fillcols] = df_proc.loc[:, fillcols].ffill(axis=0)

    # Filter rows based on column specific values
    if filter is not None:
        for column, values in filter.items():
            bool_where = df_proc[column].isin(values)
            index_to_drop = df_proc.loc[bool_where, :].index

            df_proc = df_proc.drop(index=index_to_drop)

    # Transform date column to datetime format
    df_proc["Päiväys"] = pd.to_datetime(
        df_proc.loc[:, "Päiväys"], dayfirst=True, format="mixed"
    )

    # Concatenate year at the end of every event
    df_proc.loc[:, "Keikka"] = df_proc.loc[:, "Keikka"].str.cat(
        df_proc.loc[:, "Päiväys"].dt.year.astype("string"), sep=" "
    )

    # Melt the table into a response format: Name columns are transformed into (Name, Response) pairs
    # Filter empty responses
    df_responses = df_proc.melt(
        id_vars=["Keikka", "Paikka", "Päiväys", "Homma", "Kuvaus"],
        var_name="Nimi",
        value_name="Vastaus",
    ).dropna(axis=0, how="all", subset="Vastaus")

    # Clean the answers

    df_responses.loc[:, "Vastaus"] = (
        df_responses.loc[:, "Vastaus"].str.strip().str.lower()
    )

    return df_responses


def df_db_preprocessing(
    dataframe: pd.DataFrame,
    colnames: dict[str, str],
    id_name: str,
    duplicate_col: str | None = None,
) -> pd.DataFrame:
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

    df = (
        df.reset_index(drop=True)
        .rename_axis(index=id_name)
        .reset_index()
        .rename(columns=colnames)
    )

    return df


def extract_poll_results(msg_dict: dict) -> dict:
    """
    Extract poll results from a Telegram message dictionary

    Arguments
    ---------
    msg_dict
        A dictionary containing the Telegram message

    Returns
    -------
    A dictionary with keys question, date, total votes, answers
    """

    return {
        "date": msg_dict["date"],
        "question": msg_dict["poll"]["question"],
        "total_voters": msg_dict["poll"]["total_voters"],
        "answers": msg_dict["poll"]["answers"],
    }


def is_empty_poll(poll_dict: dict) -> bool:
    """
    Returns true if the poll is empty i.e. all choices have zero responses

    Arguments
    ---------
    poll_dict
        A dictionary generated by extract_poll_results
    """

    answers = poll_dict["answers"]

    for answer in answers:
        if answer["voters"] != 0:
            return False

    return True


def is_not_event_poll(poll_dict: dict, substrings: Iterable[str]) -> bool:
    """
    Returns true if the poll is not an event registeration poll.
    Heuristically every event registeration poll should have choices
    like "kasaamaan", "ajamaan", "purkamaan" etc.

    Arguments
    ---------
    poll_dict
        A dictionary generated by extract_poll_results
    substrings
        An iterable containing substrings that determine event polls
    """

    answers = poll_dict["answers"]

    for answer in answers:
        if any(substr in answer["text"].lower().strip() for substr in substrings):
            return False

    return True


def extract_unique_answers(poll_dicts: Iterable[dict]) -> set[str]:
    """
    Returns all unique answers provided to Telegram polls

    Arguments
    ---------
    poll_dicts
        An iterable containing poll message dictionaries
    """

    answer_set = set()

    for poll_dict in poll_dicts:
        for answer in poll_dict["answers"]:
            answer_set.add(answer["text"])

    return answer_set


def answer_to_category_dict(
    answer_set: set[str], categories: dict[str, str]
) -> dict[str, str]:
    """
    Map answers to given categories based on substring matching

    Arguments
    ---------
    answer_set
        A set of answers to Telegram polls
    categories
        A dictionary of substrings to categories to map to
    """

    answer_cat_dict = {}

    substrings = list(categories.keys())

    for answer in answer_set:
        answer_clean = answer.lower().strip()
        substr_index = next(
            (i for i, v in enumerate(substrings) if v in answer_clean), None
        )

        match substr_index is None:
            case True:
                answer_cat_dict[answer] = None
            case False:
                answer_cat_dict[answer] = categories[substrings[substr_index]]

    return answer_cat_dict


def map_poll_answers_to_categories(
    poll_dicts: Iterable[dict], answer_cat_dict: dict[str, str]
) -> Iterable[dict]:
    """
    Map poll dictionaries based on an answer category dictionary

    Arguments
    ---------
    poll_dicts
        An iterable containing poll message dictionaries
    answer_cat_dict
        A dictionary containing answer category pairs
    """

    def _flatten_poll(poll_dict: dict) -> dict:
        flat_poll = {
            "poll_date": poll_dict["date"],
            "event_date": None,
            "name_event": poll_dict["question"],
            "total_voters": poll_dict["total_voters"],
        }

        for value in answer_cat_dict.values():
            if value is not None:
                flat_poll[value] = 0

        for answer in poll_dict["answers"]:
            category = answer_cat_dict[answer["text"]]

            if category is not None:
                flat_poll[category] += answer["voters"]

        return flat_poll

    return [_flatten_poll(poll_dict) for poll_dict in poll_dicts]


def update_event_names(flat_polls: Iterable[dict[str, str]]) -> None:
    """
    Update event_names by searching for date string using regex

    Arguments
    ---------
    flat_polls:
        An iterable of flattened poll dictionaries
    """

    for flat_poll in flat_polls:
        event_name = flat_poll["name_event"]

        m = re.search(r"\d{1,2}\.\d{1,2}", event_name)

        if m is not None:
            flat_poll["name_event"] = event_name[: m.start()].strip()
            flat_poll["event_date"] = "".join(
                (event_name[m.start() : m.end()], ".", flat_poll["poll_date"][:4])
            )


def extract_flat_event_polls(
    msgs_json: str,
    substr_category_dict: dict[str, str],
    event_iden_substrs: Iterable[str] = None,
) -> dict[str, str]:
    """
    Extract flattened poll dictionaries from Telegram message json data

    Arguments
    ---------
    msgs_json
        Path to the json file
    substr_category_dict
        Dictionary mapping substrings to categories
    event_iden_substrs:
        Iterable of substrings used to identify event polls
        If None, all polls are considered event polls
    """
    with open(msgs_json) as json_file:
        general_chat = json.load(json_file)

        messages = general_chat["messages"]

        polls = [
            extract_poll_results(message) for message in messages if "poll" in message
        ]

        nonempty_polls = [poll for poll in polls if not is_empty_poll(poll)]

        nonempty_event_polls = nonempty_polls

        if event_iden_substrs is not None:
            nonempty_event_polls = [
                poll
                for poll in nonempty_polls
                if not is_not_event_poll(poll, substrings=event_iden_substrs)
            ]

        answer_cat_dict = answer_to_category_dict(
            extract_unique_answers(nonempty_event_polls),
            substr_category_dict,
        )

        flat_polls = map_poll_answers_to_categories(
            nonempty_event_polls, answer_cat_dict
        )

        return flat_polls
