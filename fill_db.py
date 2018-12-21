#!/usr/bin/env python3

# Check if flag for most recent DB fill present in DB

    # If today's date is more recent than flag in DB
        # iterate over archive files from then until now
        # Add records to DB for all files in all matched archives
        # Update flag according to records processed

    # Iterate over files from current day to fill DB

import os
import datetime
import records
import tarfile
import json
import codecs

DATABASE_URL = os.getenv('DATABASE_URL', 'postgres://crawldb:crawldb@localhost:5432/crawl')
LATEST_UPDATE_Q = 'SELECT * FROM latest_update ORDER BY time DESC LIMIT 1;'
INSERT_UPDATE_Q = 'INSERT INTO latest_update (id, time) VALUES (default, default);'
INSERT_ROW_Q = 'INSERT INTO vacant_spaces (free_spots, key, time) VALUES (:free_spots, :key, :time);'

OUTPUT_DIR = os.getenv('OUTPUT_DIR', './data')
ARCHIVE_DIR = os.getenv('ARCHIVE_DIR', os.path.join(OUTPUT_DIR, 'archive'))

ISO_DATE = "%Y-%m-%d"

def read_crawl_result(fp):
    """ Return a list of (timestamp, free_spots, key) tuples with records from the JSON file """
    IGNORE = ('serverTime', 'lastUpdate')

    r = codecs.getreader('utf-8')
    res = json.load(r(fp))

    ts = res['lastUpdate']

    out = []

    for k, v in res.items():
        if k in IGNORE:
            continue

        out.append((ts, v['free'], k))
    
    return out


def main():
    archive_dir = os.path.abspath(ARCHIVE_DIR)

    db = records.Database(DATABASE_URL)

    # Get most recent update timestamp
    try:
        last_update = db.query(LATEST_UPDATE_Q)[0].time
    except IndexError:
        last_update = datetime.datetime(1,1,1)

    archive_files = os.scandir(archive_dir)

    to_create = []

    for obj in archive_files:
        if obj.is_file():
            archival_date = datetime.datetime.strptime(obj.name.replace('.tgz', ''), ISO_DATE)

            if archival_date.date() > last_update.date():
                tar = tarfile.open(obj.path)

                for member in tar.getmembers():
                    if member.isreg():
                        fp = tar.extractfile(member)
                        new_records = read_crawl_result(fp)
                        to_create.extend(new_records)

    fail_count = 0
    total_count = len(to_create)

    for row in to_create:
        try:
            db.query(INSERT_ROW_Q, time=row[0], free_spots=row[1], key=row[2])
        except Exception as ex:
            print("Failed to insert: {}".format(ex))
            fail_count += 1
    
    db.query(INSERT_UPDATE_Q)

    print("{} of {} processed.".format(total_count - fail_count, total_count))



if __name__ == '__main__':
    main()