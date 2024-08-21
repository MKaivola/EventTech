import pytest
import pandas as pd
import numpy as np

import eventtech.analysis_func as analysis_func
import eventtech.utils_analysis as utils_analysis

from data.db_metadata import EventDataBase


@pytest.fixture
def mock_get_periods_only_db(monkeypatch):
    df = pd.DataFrame(
        {
            "period_name": ["2021-2022", "2022-2023"],
            "start_date": [
                pd.Timestamp(year=2021, month=7, day=1),
                pd.Timestamp(year=2022, month=7, day=1),
            ],
            "end_date": [
                pd.Timestamp(year=2022, month=6, day=30),
                pd.Timestamp(year=2023, month=6, day=30),
            ],
        }
    )

    def mock_return(*args, **kwargs):
        return df

    monkeypatch.setattr(utils_analysis, "get_periods", mock_return)


@pytest.fixture
def mock_get_periods_only_csv(monkeypatch):
    df = pd.DataFrame(
        {
            "period_name": ["2022-2023", "2023-2024"],
            "start_date": [
                pd.Timestamp(year=2022, month=7, day=1),
                pd.Timestamp(year=2023, month=7, day=1),
            ],
            "end_date": [
                pd.Timestamp(year=2023, month=6, day=30),
                pd.Timestamp(year=2024, month=6, day=30),
            ],
        }
    )

    def mock_return(*args, **kwargs):
        return df

    monkeypatch.setattr(utils_analysis, "get_periods", mock_return)


@pytest.fixture
def get_periods_db_data():
    return pd.DataFrame(
        {
            "period_name": ["2021-2022", "2022-2023"],
            "start_date": [
                pd.Timestamp(year=2021, month=7, day=1),
                pd.Timestamp(year=2022, month=7, day=1),
            ],
            "end_date": [
                pd.Timestamp(year=2022, month=6, day=30),
                pd.Timestamp(year=2023, month=6, day=30),
            ],
        }
    )


@pytest.fixture
def get_periods_csv_data():
    return pd.DataFrame(
        {
            "period_name": ["2023-2024"],
            "start_date": [pd.Timestamp(year=2023, month=7, day=1)],
            "end_date": [
                pd.Timestamp(year=2024, month=6, day=30),
            ],
        }
    )


@pytest.fixture
def mock_get_periods_data(monkeypatch):
    def _method(df_db, df_csv):
        def mock_periods(db, conn, datasource):
            if "db" in datasource:
                return df_db
            else:
                return df_csv

        monkeypatch.setattr(utils_analysis, "get_periods", mock_periods)

    return _method


@pytest.fixture
def mock_utils_analysis_method(monkeypatch):
    def _method(df, method_name):
        def mock_return(*args, **kwargs):
            return df

        monkeypatch.setattr(utils_analysis, method_name, mock_return)

    return _method


@pytest.fixture
def mock_EventDataBase():
    return EventDataBase("postgresql+psycopg2://user:pass@notahost/test")


@pytest.fixture
def mock_EventSignups_db_only(
    mock_get_periods_only_db, mock_EventDataBase, mock_utils_analysis_method
):
    df = pd.DataFrame(
        {
            "name_event": ["Wedding", "Wedding", "Party", "Party", "Show"],
            "date_event": [
                pd.Timestamp(year=2021, month=9, day=1),
                pd.Timestamp(year=2021, month=9, day=1),
                pd.Timestamp(year=2022, month=3, day=1),
                pd.Timestamp(year=2022, month=10, day=1),
                pd.Timestamp(year=2023, month=2, day=1),
            ],
            "name_job": ["Kasaus", "Purku", "Kasaus", "Veto", "Purku"],
            "signup_count": [2, 2, 3, 4, 1],
        }
    )

    mock_utils_analysis_method(df, "get_and_concat_periods")

    return analysis_func.EventSignups(
        mock_EventDataBase,
        None,
        {"db": set(("2021-2022", "2022-2023"))},
        [None],
    )


@pytest.fixture
def mock_EventSignups_db_and_csv(
    mock_get_periods_data,
    get_periods_db_data,
    get_periods_csv_data,
    mock_EventDataBase,
    mock_utils_analysis_method,
):
    df = pd.DataFrame(
        {
            "name_event": ["Wedding", "Wedding", "Party", "Party", "Show"],
            "date_event": [
                pd.Timestamp(year=2021, month=9, day=1),
                pd.Timestamp(year=2021, month=9, day=1),
                pd.Timestamp(year=2022, month=3, day=1),
                pd.Timestamp(year=2022, month=10, day=1),
                pd.Timestamp(year=2023, month=2, day=1),
            ],
            "name_job": ["Kasaus", "Purku", "Kasaus", "Veto", "Purku"],
            "signup_count": [2, 2, 3, 4, 1],
        }
    )

    mock_utils_analysis_method(df, "get_and_concat_periods")

    mock_get_periods_data(get_periods_db_data, get_periods_csv_data)

    return analysis_func.EventSignups(
        mock_EventDataBase,
        None,
        {"db": "db", "csv": "csv"},
        ("Kasaus", "Veto", "Purku"),
        "tests/data/event_csv_mock.csv",
    )


class TestMonthlyEventCounts:
    def test_monthly_event_counts(
        self, mock_get_periods_only_db, mock_utils_analysis_method, mock_EventDataBase
    ):
        df = pd.DataFrame(
            {"year": [2021, 2022, 2023], "month": [10, 6, 3], "event_count": [6, 2, 1]}
        )

        mock_utils_analysis_method(df, "get_and_concat_periods")

        df_expected = pd.DataFrame(
            data={
                "2021-2022": [
                    0.0,
                    0.0,
                    0.0,
                    0.0,
                    0.0,
                    2.0,
                    0.0,
                    0.0,
                    0.0,
                    6.0,
                    0.0,
                    0.0,
                ],
                "2022-2023": [
                    0.0,
                    0.0,
                    1.0,
                    0.0,
                    0.0,
                    0.0,
                    0.0,
                    0.0,
                    0.0,
                    0.0,
                    0.0,
                    0.0,
                ],
            },
            index=np.arange(1, 13),
        ).rename_axis(index="Month", columns="Fiscal Year")

        periods_placeholder = {"db": set(("2021-2022", "2022-2023"))}

        df_result = analysis_func.monthly_event_counts(
            mock_EventDataBase, None, periods_placeholder
        )

        assert df_expected.equals(df_result)

    def test_monthly_event_counts_csv(self):
        period_data = pd.DataFrame(
            {
                "period_name": ["2022-2023", "2023-2024"],
                "start_date": [
                    pd.Timestamp(year=2022, month=7, day=1),
                    pd.Timestamp(year=2023, month=7, day=1),
                ],
                "end_date": [
                    pd.Timestamp(year=2023, month=6, day=30),
                    pd.Timestamp(year=2024, month=6, day=30),
                ],
            }
        )

        df_expected = pd.DataFrame(
            data={
                "2022-2023": [
                    1.0,
                    0.0,
                    0.0,
                    0.0,
                    1.0,
                    0.0,
                    2.0,
                    0.0,
                    0.0,
                    1.0,
                    0.0,
                    0.0,
                ],
                "2023-2024": [
                    1.0,
                    3.0,
                    0.0,
                    0.0,
                    0.0,
                    0.0,
                    0.0,
                    0.0,
                    0.0,
                    0.0,
                    0.0,
                    1.0,
                ],
            },
            index=np.arange(1, 13),
        ).rename_axis(index="Month", columns="Fiscal Year")

        df_result = analysis_func._monthly_event_counts_csv(
            "tests/data/event_csv_mock.csv", period_data
        )

        assert df_expected.equals(df_result)

    def test_monthly_event_counts_db_and_csv(
        self,
        mock_get_periods_data,
        get_periods_db_data,
        get_periods_csv_data,
        mock_utils_analysis_method,
        mock_EventDataBase,
    ):
        df = pd.DataFrame(
            {"year": [2021, 2022, 2023], "month": [10, 6, 3], "event_count": [6, 2, 1]}
        )

        mock_utils_analysis_method(df, "get_and_concat_periods")

        mock_get_periods_data(get_periods_db_data, get_periods_csv_data)

        df_expected = pd.DataFrame(
            data={
                "2021-2022": [
                    0.0,
                    0.0,
                    0.0,
                    0.0,
                    0.0,
                    2.0,
                    0.0,
                    0.0,
                    0.0,
                    6.0,
                    0.0,
                    0.0,
                ],
                "2022-2023": [
                    0.0,
                    0.0,
                    1.0,
                    0.0,
                    0.0,
                    0.0,
                    0.0,
                    0.0,
                    0.0,
                    0.0,
                    0.0,
                    0.0,
                ],
                "2023-2024": [
                    1.0,
                    3.0,
                    0.0,
                    0.0,
                    0.0,
                    0.0,
                    0.0,
                    0.0,
                    0.0,
                    0.0,
                    0.0,
                    1.0,
                ],
            },
            index=np.arange(1, 13),
        ).rename_axis(index="Month", columns="Fiscal Year")

        period_datasource = {"db": "db", "csv": "csv"}

        df_result = analysis_func.monthly_event_counts(
            mock_EventDataBase, None, period_datasource, "tests/data/event_csv_mock.csv"
        )

        assert df_expected.equals(df_result)


def test_yearly_technician_signups(
    mock_get_periods_only_db, mock_utils_analysis_method, mock_EventDataBase
):
    df = pd.DataFrame(
        {
            "name_tech": ["Jane", "John", "John", "Michael", "Jane"],
            "date_event": [
                pd.Timestamp(year=2021, month=9, day=1),
                pd.Timestamp(year=2021, month=12, day=12),
                pd.Timestamp(year=2022, month=3, day=9),
                pd.Timestamp(year=2022, month=10, day=1),
                pd.Timestamp(year=2023, month=2, day=2),
            ],
        }
    )

    mock_utils_analysis_method(df, "get_and_concat_periods")

    df_expected = pd.DataFrame(
        data={"2021-2022": [1, 2, 0], "2022-2023": [1, 0, 1]},
        index=["Jane", "John", "Michael"],
    ).rename_axis(index="name_tech", columns="Fiscal Year")

    df_result = analysis_func.yearly_technician_signups(mock_EventDataBase, None, None)

    assert df_expected.equals(df_result)


class TestEventSignups:
    def test_popular_event_signups_per_job(self, mock_EventSignups_db_only):
        df_expected = pd.DataFrame(
            {
                "Kasaus": [2.0, 3.0, 0.0, 0.0],
                "Purku": [2.0, 0.0, 0.0, 1.0],
                "Veto": [0.0, 0.0, 4.0, 0.0],
            },
            index=pd.MultiIndex.from_tuples(
                [
                    ("2021-2022", "Wedding"),
                    ("2021-2022", "Party"),
                    ("2022-2023", "Party"),
                    ("2022-2023", "Show"),
                ],
                names=("period_name", "name_event"),
            ),
        ).rename_axis(columns="Job")

        df_result = mock_EventSignups_db_only.popular_event_signups_per_job(2)

        assert df_expected.equals(df_result)

    def test_popular_event_signups_per_job_db_and_csv(
        self, mock_EventSignups_db_and_csv
    ):
        df_expected = pd.DataFrame(
            {
                "Kasaus": [2.0, 3.0, 0.0, 0.0, 10.0, 10.0],
                "Purku": [2.0, 0.0, 0.0, 1.0, 8.0, 8.0],
                "Veto": [0.0, 0.0, 4.0, 0.0, 5.0, 4.0],
            },
            index=pd.MultiIndex.from_tuples(
                [
                    ("2021-2022", "Wedding"),
                    ("2021-2022", "Party"),
                    ("2022-2023", "Party"),
                    ("2022-2023", "Show"),
                    ("2023-2024", "Meioosi_1"),
                    ("2023-2024", "Meioosi_2"),
                ],
                names=("period_name", "name_event"),
            ),
        ).rename_axis(columns="Job")

        df_result = mock_EventSignups_db_and_csv.popular_event_signups_per_job(2)

        assert df_expected.equals(df_result)


def test_event_poll_durations_and_signups(
    mock_get_periods_only_csv, mock_EventDataBase
):
    df_expected = pd.DataFrame(
        {
            "poll_day_offset": [2, 11, 10, 2, 23, 1, 7, 7, 13, 27],
            "active_voters": [9, 7, 8, 6, 8, 6, 5, 10, 10, 10],
        },
        index=pd.MultiIndex.from_tuples(
            [
                ("2022-2023", "Alvarin Approjen Jatkot"),
                ("2022-2023", "Neon Rave 2023"),
                ("2022-2023", "Pääsen Smökrökkiin"),
                ("2022-2023", "Kiima"),
                ("2022-2023", "Sikajuhlat"),
                ("2023-2024", "Huomenna"),
                ("2023-2024", "Talvipäivän jatkot"),
                ("2023-2024", "Meioosi_1"),
                ("2023-2024", "Meioosi_2"),
                ("2023-2024", "Meioosi_3"),
            ]
        ),
    )

    df_result = analysis_func.event_poll_durations_and_signups(
        mock_EventDataBase, None, "tests/data/event_csv_mock.csv", None
    )

    assert df_expected.equals(df_result)
