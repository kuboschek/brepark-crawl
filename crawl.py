#!/usr/bin/env python3

"""
    Scrapes the Bremen parking garage page for free spots data.
    Saves raw response to .json files. One folder per day.
    Reports errors from HTTP, and if update timestamps are too old.
"""

import os
import requests
import datetime
import json

BREPARK_URL = "https://www.brepark.de/fileadmin/freespaces.json"
TS_FORMAT = "%a %b %d %H:%M:%S %Y"
ISO_FORMAT = "%Y-%m-%dT%H-%M-%S"
ISO_DATE = "%Y-%m-%d"
OUTPUT_DIR = os.getenv('OUTPUT_DIR', './data')

base_dir = os.path.abspath(OUTPUT_DIR)

# Create the output directories
try:
    os.mkdir(base_dir)
except FileExistsError:
    pass

r = requests.get(BREPARK_URL)
now = datetime.datetime.now()

today_dir = os.path.join(base_dir, now.strftime(ISO_DATE))

try:
    os.mkdir(today_dir)
except FileExistsError:
    pass

j = r.json()

# Extract timestamps
last_update = datetime.datetime.strptime(j['lastUpdate'], TS_FORMAT)
server_time = datetime.datetime.strptime(j['serverTime'], TS_FORMAT)

# Save crawled file
with open(os.path.join(today_dir, now.strftime(ISO_FORMAT) + '.json'), 'w') as f:
    f.write(r.text)

if r.status_code != 200:
    print(json.dumps({'ok': False, 'reason': r.reason, 'status': r.status_code, 'text': r.text}))
    exit(1)

if len(r.text) < 512:
    print(json.dumps({'ok': False, 'reason': 'Response Too Short', 'text': r.text}))
    exit(2)

# Also fail if the last update is way too old
if last_update - now > datetime.timedelta(minutes=2):
    print(json.dumps({'ok': False, 'reason': 'Last Update Too Old', 'text': r.text}))
    exit(3)

if server_time - now > datetime.timedelta(seconds=10):
    print(json.dumps({'ok': False, 'reason': 'Server Time Diverged', 'text': r.text}))
    exit(4)
