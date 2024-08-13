import json

import pandas as pd

import data.preprocess_utils as utils

with open("data/2023_2024/Tekniikkasektori_2023_2024.json") as general_chat:
    general_chat = json.load(general_chat)

    messages = general_chat["messages"]

    polls = [
        utils.extract_poll_results(message) for message in messages if "poll" in message
    ]

    nonempty_event_polls = [
        poll
        for poll in polls
        if not utils.is_empty_poll(poll)
        and not utils.is_not_event_poll(poll, substrings=("kasa", "pur", "aja", "vet"))
    ]

    answer_cat_dict = utils.answer_to_category_dict(
        utils.extract_unique_answers(nonempty_event_polls),
        {"kasa": "Kasaus", "pur": "Purku", "aja": "Veto", "veto": "Veto"},
    )

    flat_polls = utils.map_poll_answers_to_categories(
        nonempty_event_polls, answer_cat_dict
    )

    utils.update_event_names(flat_polls)

event_data_2023_2024 = pd.DataFrame(flat_polls)

event_data_2023_2024 = event_data_2023_2024.drop(
    index=event_data_2023_2024[event_data_2023_2024["name_event"] == "Smökrok"].index
)
