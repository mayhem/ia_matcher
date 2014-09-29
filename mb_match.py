#!/usr/bin/env python

import urllib2
import sys
import json
import os
from time import sleep, time
from operator import itemgetter
import Levenshtein
import re
import psycopg2
import config
import musicbrainzngs
from ia_parse import ParseArchiveData, clean_string
from match_utils import set_error, set_mbid, update_timestamp

MAX_TOC_MATCHES = 10
MATCH_THRESHOLD = .4
MIN_NUM_TRACKS = 4

# Found track naming schemes
# Various Artists - Casa De Mi Padre (Original Motion Picture Soundtrack) (2012) [FLAC]/01 - Christina Aguilera - Casa De Mi Padre.flac
# Wild's Reprisal - Cascadia Rising (2012) [FLAC]/Wild's Reprisal - Cascadia Rising - 01 A fierce green fire dying in her eyes.flac
# The Deadly Gentlemen - Carry Me To Home [FLAC]/The Deadly Gentlemen - Carry Me To Home - 01 Sober Cure.flac
# The New York Lounge Society - Christmas Chill (2010) [FLAC]/01 - Joy to the World.flac
# Twista-Category_F5-2009-H3X/06-twista-walking_on_ice_ft._gucci_mane_and_oj_da_juiceman.mp3

query_find_unprocessed = """SELECT iaid, ia_data 
                              FROM match 
                             WHERE mb_data IS NULL 
                               AND mbid IS NULL 
                               AND err IS NULL 
                          ORDER BY ts ASC 
                             LIMIT 100"""

query_rerun_low_matches= """SELECT iaid, ia_data 
                              FROM match 
                             WHERE mb_data IS NULL 
                               AND mbid IS NULL 
                               AND err = 'match below threshold'
                          ORDER BY ts ASC 
                             LIMIT 100"""

def match_tracks_to_release(title, tracks, mbid, name):
    '''
       Given a list of tracks, a candidate release mbid and name this function should return
       a score (0.0-1.0) of how well this list of tracks compares to the MB release.
    '''

    try:
        musicbrainzngs.set_useragent(config.USER_AGENT_STRING, config.USER_AGENT_VERSION, config.USER_AGENT_USER)
        rel = musicbrainzngs.get_release_by_id(mbid, includes=['recordings'])
    except musicbrainzngs.WebServiceError as exc:
        print "Something went wrong with the request: %s" % exc 
        return -1

#    print "--- %-40s %s" % (title.encode('utf-8'), name.encode('utf-8'))
    matches = []
    total = 0.0
    print 
    for i, t in enumerate(rel["release"]["medium-list"][0]["track-list"]):
        try:
            d = Levenshtein.ratio(clean_string(t['recording']['title']), tracks[i]['clean'])
            print "%.3f %2d %-40s | %s" % (d, i+1, clean_string(t['recording']['title']), tracks[i]['clean'])
            total += d
        except IndexError:
            return -1

    return total / len(tracks)

def mb_match(conn, iaid, data):

    release, err = ParseArchiveData().parse(iaid, data) 
    if err:
        return set_error(conn, iaid, err)

    tracks = release['tracks']

#    print iaid
#    for t in tracks:
#        print "%d. %s - %.3f" % (t['num'], t['name'], t['duration'])

    if len(tracks) < 1:
        return set_error(conn, iaid, "0 tracks found in json")
    if len(tracks)  < MIN_NUM_TRACKS:
            return set_error(conn, iaid, "too few tracks on this release")

    ms_durations = [ "%d" % int(t['duration'] * 1000) for t in tracks]
    dur_str = ",".join(ms_durations)
    dur_str = "'{" + dur_str + "}'"

    query = """SELECT gid, name, cube_distance(toc, create_cube_from_durations(%s)) AS distance
                 FROM release_toc 
                WHERE toc <@ create_bounding_cube(%s, %d)
             ORDER BY distance
                LIMIT %s""" % (dur_str, dur_str, 3000, MAX_TOC_MATCHES)
    cur = conn.cursor()
    cur.execute(query)
    rows = cur.fetchall()
    cur.close()

    if rows:
        hi = -1
        hi_index = -1
        for i, row in enumerate(rows):
            score = match_tracks_to_release(release['name'], tracks, row[0], row[1])
            if score > hi:
                hi = score
                hi_index = i

        if hi_index >= 0:
            if hi > MATCH_THRESHOLD:
                mbid = rows[hi_index][0]
                print "picking %s - %.3f" % (mbid, hi)
                set_mbid(conn, iaid, mbid, len(tracks))
                return mbid
            else:
                return set_error(conn, iaid, "match below threshold")

    return set_error(conn, iaid, "no toc match")

try:
    conn = psycopg2.connect(config.PG_CONNECT)
    conn2 = psycopg2.connect(config.PG_CONNECT)
except psycopg2.OperationalError as err:
    print "Cannot connect to database: %s" % err
    sys.exit(-1)

while True:
    cur = conn.cursor()
    cur.execute(query_rerun_low_matches)
    rows = cur.fetchall()
    cur.close()

    if len(rows) == 0:
        break

    for row in rows:
        ret = mb_match(conn, row[0], row[1])
        print "%-37s %s" % (ret, row[0])

print "done"
