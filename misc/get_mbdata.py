#!/usr/bin/env python

import psycopg2
import config
import musicbrainzngs

NUM_ROWS = 100

try:
    conn = psycopg2.connect(config.PG_CONNECT)
    conn2 = psycopg2.connect(config.PG_CONNECT)
except psycopg2.OperationalError as err:
    print "Cannot connect to database: %s" % err
    sys.exit(-1)

mbdata = ""
def callme(data):
    global mbdata
    mbdata = data

musicbrainzngs.set_useragent(config.USER_AGENT_STRING, config.USER_AGENT_VERSION, config.USER_AGENT_USER)
musicbrainzngs.set_format(fmt='json')
musicbrainzngs.set_parser(callme)
musicbrainzngs.set_hostname("musicbrainz.org")
#musicbrainzngs.set_hostname("musicb-1.us.archive.org:5060")
#musicbrainzngs.set_rate_limit(False)

offset = 0
while True:
    cur = conn.cursor()

    cur.execute("""SELECT mbid
                     FROM match
                    WHERE mbid IS NOT NULL 
                      AND mb_data IS NULL
                   OFFSET %s 
                    LIMIT %s""", (offset, NUM_ROWS))
#    if cur.rowcount == 0:
#        break

    while True:
        row = cur.fetchone()
        if not row:
            break

        mbid = row[0]
        try:
            rel = musicbrainzngs.get_release_by_id(mbid, includes=['recordings'])
        except musicbrainzngs.WebServiceError as exc:
            print "Something went wrong with the request: %s" % exc 
            continue

        cur2 = conn2.cursor()
        cur2.execute("UPDATE match SET mb_data = %s where mbid = %s", (mbdata, mbid))
        conn2.commit()
        print mbid

    offset += NUM_ROWS
