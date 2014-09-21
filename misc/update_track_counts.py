#!/usr/bin/env python

import psycopg2
import config
from ia_parse import ParseArchiveData, clean_string

NUM_ROWS = 1000

try:
    conn = psycopg2.connect(config.PG_CONNECT)
    conn2 = psycopg2.connect(config.PG_CONNECT)
except psycopg2.OperationalError as err:
    print "Cannot connect to database: %s" % err
    sys.exit(-1)

cur = conn.cursor()

count = 0
while True:
    cur.execute("""SELECT iaid, ia_data 
                     FROM match
                    WHERE mbid is not null and tracks = -1
                    LIMIT %s""", (NUM_ROWS,))

    if cur.rowcount == 0:
        break

    cur2 = conn2.cursor()
    for i in xrange(cur.rowcount):
        row = cur.fetchone()
        iaid = row[0]
        data = row[1]
        release, err = ParseArchiveData().parse(iaid, data) 
        if err:
            continue

        n = len(release['tracks'])
        if n > 0:
            cur2.execute("UPDATE match SET tracks = %s WHERE iaid = %s", (n, iaid))

    conn2.commit()
    count += NUM_ROWS
    print "%d" % (count)

cur.close()
cur2.close()
