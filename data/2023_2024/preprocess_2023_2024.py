import json

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
        and not utils.is_not_event_poll(poll, substrings=("kasa", "purk", "aja"))
    ]
