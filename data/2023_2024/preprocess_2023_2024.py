from pathlib import Path

import pandas as pd

import data.preprocess_utils as utils

data_path_2023_2024 = Path(__file__).parent

data_path = data_path_2023_2024.parent

flat_polls_general_chat = utils.extract_flat_event_polls(
    str(data_path_2023_2024 / "Tekniikkasektori_2023_2024.json"),
    {
        "kasa": "Kasaus",
        "pur": "Purku",
        "aja": "Veto",
        "ajo": "Veto",
        "veto": "Veto",
        "ei": "En pääse",
        "en": "En pääse",
    },
    ("kasa", "pur", "aja", "vet"),
)

utils.update_event_names(flat_polls_general_chat)

event_data_2023_2024 = pd.DataFrame(flat_polls_general_chat)

events_to_drop = ["Smökrok", "Orientaatioviikon keikat (kaikki Smökissä)"]

is_event_to_drop = event_data_2023_2024["name_event"].isin(events_to_drop)

index_to_drop = event_data_2023_2024.loc[is_event_to_drop, :].index

event_data_2023_2024 = event_data_2023_2024.drop(index=index_to_drop)

event_data_2023_2024.to_csv(data_path / "Tapahtumat_2023_2024.csv", index=False)
