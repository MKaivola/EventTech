import pytest
import pandas as pd
import numpy as np

import analysis_func
import utils_analysis

from Data.db_metadata import EventDataBase

@pytest.fixture
def mock_get_periods(monkeypatch):
    df = pd.DataFrame({"period_name": ["2021-2022", "2022-2023"],
                        "start_date": [pd.Timestamp(year=2021,month=7,day=1),
                                      pd.Timestamp(year=2022,month=7,day=1)],
                        "end_date": [pd.Timestamp(year=2022,month=6,day=30),
                                     pd.Timestamp(year=2023,month=6,day=30)]})
    
    def mock_return(*args, **kwargs):
        return df
    
    monkeypatch.setattr(utils_analysis, "get_periods", mock_return)

@pytest.fixture
def mock_events_per_month(monkeypatch):
    df = pd.DataFrame({"year": [2021,2022,2023],
                       "month": [10,6,3],
                       "event_count": [6,2,1]})
    
    def mock_return(*args, **kwargs):
        return df
    
    monkeypatch.setattr(utils_analysis, "get_and_concat_periods", mock_return)

@pytest.fixture
def mock_EventDataBase():
    return EventDataBase("postgresql+psycopg2://user:pass@notahost/test")

def test_monthly_event_counts(mock_get_periods, 
                              mock_events_per_month,
                              mock_EventDataBase):
    
    df_expected = pd.DataFrame(data={'2021-2022': [0.0, 0.0, 0.0, 0.0, 0.0, 2.0, 0.0, 0.0, 0.0, 6.0, 0.0, 0.0],
                                     '2022-2023': [0.0, 0.0, 1.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0 ,0.0 ,0.0]},
                                index=np.arange(1,13)).rename_axis(index='Month',columns='Fiscal Year')
    
    df_result = analysis_func.monthly_event_counts(mock_EventDataBase,
                                                   None,
                                                   None)
    
    assert df_expected.equals(df_result)