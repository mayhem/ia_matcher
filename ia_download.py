#!/usr/bin/env python

import urllib2
import sys
import json
import os
from time import sleep
from operator import itemgetter
import Levenshtein
import re
import psycopg2
import socket
import config

if len(sys.argv) < 2:
    print "Usage: ia_download.py <id file>"
    sys.exit(-1)

id_file = sys.argv[1]

def download(conn, iaid):

    cur = conn.cursor()
    cur.execute("SELECT count(*) FROM match WHERE iaid = %s", (iaid,))
    count = cur.fetchone()[0]
    if count != 0:
        return True

    try:
        response = urllib2.urlopen("http://archive.org/metadata/" + iaid, timeout = 5)
        data = response.read()
    except urllib2.URLError:
        data = ""
    except urllib2.HTTPError:
        data = ""
    except socket.timeout:
        data = ""

    while True:
        try:
            cur.execute("INSERT INTO match (iaid, ia_data) values (%s, %s)", (iaid, data))
            conn.commit()
            break
        except psycopg2.OperationalError, e:
            print "Postgres seyz: %s" % e
            return False

    return data != ""

try:
    f = open(id_file, "r")
except IOError:
    print "Cannot open id file: %s" % id_file
    sys.exit(-1)

try:
    conn = psycopg2.connect(config.PG_CONNECT)
except psycopg2.OperationalError as err:
    print "Cannot connect to database: %s" % err
    sys.exit(-1)

while True:
    line = f.readline()
    if not line:
        break
    ia_id = line.strip()
    ok = download(conn, ia_id)
    if ok:
        print "ok  ",
    else:
        print "fail",

    print ia_id

f.close()
