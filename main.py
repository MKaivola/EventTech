import pandas as pd
from sqlalchemy import Select
from sqlalchemy import func

from Data.db_metadata import EventDataBase

import analysis_func

db = EventDataBase("postgresql://Mikko:password@localhost:5432/TechEventData")

### Plot how many events for each month and year ###

event_counts = analysis_func.monthly_event_counts(db)

event_counts.plot(kind='bar',
                  title='Number of events').figure.savefig('Plots/event_counts.pdf')


