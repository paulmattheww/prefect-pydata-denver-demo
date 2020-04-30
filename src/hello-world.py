# see config files
import json
import requests
from collections import namedtuple
from contextlib import closing
import sqlite3
import datetime

from prefect import task, Flow
from prefect.engine import signals
from prefect.engine.result_handlers import LocalResultHandler
from prefect.tasks.database.sqlite import SQLiteScript
from prefect.schedules import IntervalSchedule

# Handle changes in state
def alert_failed(obj, old_state, new_state):
    '''Must match this signature with obj, old, new'''
    if new_state.is_failed():
        print("!!! Failed !!!")

# Setup
create_table = SQLiteScript(
    db='cfpbcomplaints.db',
    script="""
    CREATE TABLE IF NOT EXISTS complaint 
    (timestamp TEXT, state TEXT, product TEXT, company TEXT, complaint_what_happened TEXT)
    """
)


# Extract
@task(cache_for=datetime.timedelta(days=1), state_handlers=[alert_failed], result_handler=LocalResultHandler())
def get_complaint_data():
    r = requests.get('https://www.consumerfinance.gov/data-research/consumer-complaints/search/api/v1/',
                     params={'size': 10})
    response_json = json.loads(r.text)
    print("Actually requested this time!")
    return response_json['hits']['hits']


# Transform
# Uncaught exceptions trigger a failed state in Prefect
@task(state_handlers=[alert_failed])
def parse_complaint_data(raw):
    # raise signals.FAIL signals.SUCCESS etc look it up
    complaints = list()
    Complaint = namedtuple('Complaint', ['date_received', 'state', 'product', 'company', 'complaint_what_happened'])
    for row in raw:
        source = row.get('_source')
        this_complaint = Complaint(
            date_received=source.get('date_received'),
            state=source.get('state'),
            product=source.get('product'),
            company=source.get('company'),
            complaint_what_happened=source.get('complaint_what_happened')
        )
        complaints.append(this_complaint)
    return complaints


# Results handler - how and where to persist outputs, lots of types for different backends


# Load
@task(state_handlers=[alert_failed])
def store_complaints(parsed):
    insert_cmd = 'INSERT INTO complaint VALUES (?, ?, ?, ?, ?)'
    with closing(sqlite3.connect('cfpbcomplaints.db')) as conn:
        with closing(conn.cursor()) as cursor:
            cursor.executemany(insert_cmd, parsed)
            conn.commit()

# Set schedule to pass into Flow object
schedule = IntervalSchedule(interval=datetime.timedelta(minutes=1))

# How tasks are related w/ Flow object
with Flow('My ETL Flow', state_handlers=[alert_failed]) as f:
    db_table = create_table() # Must happen first
    raw = get_complaint_data()
    parsed = parse_complaint_data(raw)
    populated_table = store_complaints(parsed)
    populated_table.set_upstream(db_table) # Imperative API not functional, one state depends on the other (not share data)


f.register()
# f.run()
# f.visualize()

# Can mix and match, functional api better for data and state dependencies
# Imperative API better for only state dependencies

# Schedules for Batch ETL

# Host OS Registers flow for graphQL layer so setup knows about it
# Then start an Agent (Python process) that pulls Postgres through GraphQL for for tasks
# When it receives Y, runs
# Most basic is LocalAgent (pulls and does work itself), but can have distributed Dask clusters
#