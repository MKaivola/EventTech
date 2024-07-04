from Data.db_metadata import EventDataBase

import analysis_func
import plotting_tools

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

### Plot most popular event signup counts for each job for each year ###

signup_counts_per_event = analysis_func.popular_event_signups_per_job(db,
                                                                      ('2021-2022','2022-2023'),
                                                                      ('Kasaus', 'Veto', 'Purku'))

plotting_tools.outer_index_barplot(signup_counts_per_event,
                                   'Plots/top_event_signups.pdf')

