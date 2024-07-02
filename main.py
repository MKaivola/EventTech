import pandas as pd
from sqlalchemy import Select
from sqlalchemy import func

from Data.db_metadata import EventDataBase

import analysis_func

db = EventDataBase("postgresql://Mikko:password@localhost:5432/TechEventData")

### Plot how many events for each month and year ###

event_counts = analysis_func.monthly_event_counts(db,
                                                  ('2021-2022','2022-2023'))

event_counts.plot(kind='bar',
                  title='Number of events').figure.savefig('Plots/event_counts.pdf')

### Plot how many signups each technician has for each year ###
signup_counts = analysis_func.yearly_technician_signups(db,
                                                  ('2021-2022','2022-2023'))

signup_counts.plot(kind='bar',
                   title='Number of signups').figure.savefig('Plots/signup_counts.pdf')


