import pandas as pd
from sqlalchemy import Select

from Data.db_metadata import EventDataBase

db = EventDataBase("postgresql://Mikko:password@localhost:5432/TechEventData")
