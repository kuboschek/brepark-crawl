#!/usr/bin/env python3
import os
import datetime
import subprocess
import shutil

ISO_FORMAT = "%Y-%m-%dT%H:%M:%S"
ISO_DATE = "%Y-%m-%d"
OUTPUT_DIR = os.getenv('OUTPUT_DIR', './data')
ARCHIVE_DIR = os.getenv('ARCHIVE_DIR', os.path.join(OUTPUT_DIR, 'archive'))
TAR_CMD = 'tar -czf {}.tgz {}'

output_dir = os.path.abspath(OUTPUT_DIR)
archive_dir = os.path.abspath(ARCHIVE_DIR)
date_today = datetime.datetime.now().strftime(ISO_DATE)

try:
    os.mkdir(archive_dir)
except FileExistsError:
    pass

w = os.scandir(OUTPUT_DIR)

for obj in w:
    # Today's folder is still being added to, so we don't tarball it.
    if obj.is_dir() and obj.name != date_today and obj.name != os.path.basename(ARCHIVE_DIR):
        cmd = TAR_CMD.format(os.path.join(ARCHIVE_DIR, obj.name), obj.path)
        print(cmd)
        ret = subprocess.call(cmd.split(' '))

        if ret != 0:
            print("Tar returned with errors. Archival interrupted.")
            exit(1)
        else:
            shutil.rmtree(obj.path)