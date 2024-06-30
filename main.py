import pandas as pd
from sqlalchemy import Select
from sqlalchemy import func

from Data.db_metadata import EventDataBase

db = EventDataBase("postgresql://Mikko:password@localhost:5432/TechEventData")

# Calculate how many events for each month and year

events_sbq = (Select(func.extract('year', db.events.c.date_event).label('year'),
                    func.extract('month', db.events.c.date_event).label('month'))
                    .subquery())

events_per_month_stmt = (Select(events_sbq.c['year', 'month'], func.count().label('event_count'))
                        .group_by(events_sbq.c['year', 'month'])
                        .order_by(events_sbq.c['year', 'month'])
                        )
with db.engine.begin() as conn:
    events_per_month = pd.read_sql(events_per_month_stmt,
                                   conn)    

event_counts = pd.DataFrame(index=pd.period_range('2021-07', periods=24, freq='M'))

periods_in_data = pd.to_datetime(events_per_month.drop(columns='event_count').assign(day = 1)).dt.to_period('M')

events_per_month = (events_per_month.assign(period = periods_in_data)
                    .loc[:, ['period', 'event_count']].set_index('period'))

event_counts = event_counts.join(events_per_month).fillna(0)

event_counts.plot(kind='bar').figure.savefig('Plots/event_counts.pdf')


