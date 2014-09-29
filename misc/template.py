#!/usr/bin/env python

import psycopg2
import config
from ia_parse import ParseArchiveData, clean_string

NUM_ROWS = 100

try:
    conn = psycopg2.connect(config.PG_CONNECT)
    conn2 = psycopg2.connect(config.PG_CONNECT)
except psycopg2.OperationalError as err:
    print "Cannot connect to database: %s" % err
    sys.exit(-1)

offset = 0
while True:
    cur = conn.cursor()

    cur.execute("""SELECT iaid, ia_data 
                     FROM match
                    WHERE err = 'incomplete tracklist' 
                   OFFSET %s 
                    LIMIT %s""", (offset, NUM_ROWS))
    if cur.rowcount == 0:
        break

    while True:
        row = cur.fetchone()
        if not row:
            break

        iaid = row[0]
        data = row[1]
        release, err = ParseArchiveData().parse(iaid, data) 
        if not err:
            print iaid
            print release

    offset += NUM_ROWS
