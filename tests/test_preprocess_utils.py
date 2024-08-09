import pytest

import data.preprocess_utils as utils


class TestIsEmptyPoll:
    def test_empty_poll(self):
        empty_poll = {
            "date": "???",
            "question": "???.",
            "total_voters": 10,
            "answers": [
                {"text": "Kasaus (12-> ???)", "voters": 0, "chosen": False},
                {"text": "Soundcheck (16/17-> ???)", "voters": 0, "chosen": False},
                {"text": "Alkaa 20.00", "voters": 0, "chosen": False},
                {"text": "Purku 03.00", "voters": 0, "chosen": False},
            ],
        }

        assert utils.is_empty_poll(empty_poll)

    def test_non_empty_poll(self):
        non_empty_poll = {
            "date": "???",
            "question": "???.",
            "total_voters": 10,
            "answers": [
                {"text": "Kasaus (12-> ???)", "voters": 0, "chosen": False},
                {"text": "Soundcheck (16/17-> ???)", "voters": 0, "chosen": False},
                {"text": "Alkaa 20.00", "voters": 0, "chosen": False},
                {"text": "Purku 03.00", "voters": 2, "chosen": False},
            ],
        }

        assert not utils.is_empty_poll(non_empty_poll)


class TestIsNotEventPoll:
    def test_not_event_poll(self):
        not_event_poll = {
            "date": "???",
            "question": "???.",
            "total_voters": 10,
            "answers": [
                {"text": "Monday", "voters": 0, "chosen": False},
                {"text": "Thursday", "voters": 0, "chosen": False},
                {"text": "Friday", "voters": 0, "chosen": False},
                {"text": "Sunday", "voters": 2, "chosen": False},
            ],
        }

        assert utils.is_not_event_poll(not_event_poll)

    def test_event_poll(self):
        event_poll = {
            "date": "???",
            "question": "???.",
            "total_voters": 10,
            "answers": [
                {"text": "Kasaus (12-> ???)", "voters": 0, "chosen": False},
                {"text": "Soundcheck (16/17-> ???)", "voters": 0, "chosen": False},
                {"text": "Alkaa 20.00", "voters": 0, "chosen": False},
                {"text": "Purku 03.00", "voters": 0, "chosen": False},
            ],
        }

        assert not utils.is_not_event_poll(event_poll)
