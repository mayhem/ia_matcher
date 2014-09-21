#!/usr/bin/env python

import psycopg2

DRY_RUN = 0

def set_error(conn, iaid, err):
    if not DRY_RUN:
        cur = conn.cursor()
        cur.execute('UPDATE match SET ts = now(), err = %s where iaid = %s', (err, iaid))
        conn.commit()
    return err

def update_timestamp(conn, iaid):
    if not DRY_RUN:
        cur = conn.cursor()
        cur.execute('UPDATE match SET ts = now() where iaid = %s', (iaid,))
        conn.commit()

def set_mbid(conn, iaid, mbid, tracks):
    if not DRY_RUN:
        cur = conn.cursor()
        cur.execute("UPDATE match SET ts = now(), mbid = %s, err = '', tracks = %s where iaid = %s", (mbid, tracks, iaid))
        conn.commit()
