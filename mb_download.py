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

MUSICBRAINZ_HOST = "asterix.mb"
MUSICBRAINZ_PORT = 80

def clean_string(s):
    return re.sub('[-\W_]', '', s).lower()

def pick_duration(tracks):
    '''Pick the most plausible duration. Fun guessing for all!'''

    hi = 0.0
    for t in tracks:
        d = t['duration']
        if hi > d:
            hi = d
        if d == 0.0 or d == 30.0 or d == 60.0:
            continue
        return d

    return hi

def set_error(conn, iaid, err):
    cur = conn.cursor()
    cur.execute('UPDATE match SET ts = now(), err = %s where iaid = %s', (err, iaid))
    conn.commit()
    return err

def update_timestamp(conn, iaid):
    cur = conn.cursor()
    cur.execute('UPDATE match SET ts = now() where iaid = %s', (iaid))
    conn.commit()

next_delay = 0.0
def mb_download(conn, iaid, data):
    global next_delay

    release = { 'durations' : [], 'tracks' : [] }
    num_tracks = 0

    js = json.loads(data)

    tracks = {}
    try:    
        for f in js['files']:
            try:
                if f['length'] == '00:30':
                    continue
            except KeyError:
                continue

            try:
                raw_track_text = f['track']
            except KeyError:
                continue

            try:
                track, num_tracks = raw_track_text.split("/")
            except ValueError:
                track = raw_track_text

            try:
                track = int(track) - 1
            except ValueError:
                continue
            try:
                duration = float(f['length'])
            except:
                if f['length'].find(':') >= 0:
                    m, s = f['length'].split(':')
                    duration = float(m) * 60.0 + float(s)
                else:
                    set_error(conn, iaid, "Uknown length format: '%s'" % f['length'])

            try:
                name = f['name'].split('/')[1]
            except IndexError:
                name = f['name']

            try:
                name = name.split(".")[0]
            except:
                pass

            clean = clean_string(name)
            if not tracks.has_key(track):
                tracks[track] = []
            tracks[track].append(dict(num=track, duration=duration, name=name, clean=clean, source=f['source']))

    except KeyError:
        return set_error(conn, iaid, "key error in ia json")

#    for k in sorted(tracks.keys()):
#        for t in tracks[k]:
#            print t
#        print

    if len(tracks) > 0:
        new_list = [ ]
        for i in xrange(len(tracks)):
            try:
                dummy = tracks[i]
            except KeyError:
                return set_error(conn, iaid, "incomplete tracklist");

            for t in tracks[i]:
                if t['source'] == 'original':
                    if t['duration'] == 0.0:
                        t['duration'] = pick_duration(tracks[i])
                        if t['duration'] == 0.0:
                            return set_error(conn, iaid, "release with missing track duration")

                    new_list.append(t)

        tracks = new_list

#    print iaid
#    for t in tracks:
#        print "%d. %s - %.3f" % (t['num'], t['name'], t['duration'])

    if len(tracks) < 5:
        if len(tracks) < 1:
            return set_error(conn, iaid, "0 tracks found in json")
        else:
            return set_error(conn, iaid, "too few tracks on this release")

    total_sectors = 150
    for t in tracks:
        total_sectors += int(t['duration'] * 75)

    toc = "1+%d+%d+150" % (len(tracks), total_sectors)
    offset = 150
    for t in tracks[:-1]:
        toc += "+%d" % (offset + int(t['duration'] * 75))
        offset += int(t['duration'] * 75)

    url = 'http://musicbrainz.org/ws/2/discid/-?toc=%s&media-format=all&fmt=json&inc=recordings' % toc

    sleep(next_delay)

    timeout_delay = 5
    while True:
        try:    
            t0 = time()
            opener = urllib2.build_opener()
            opener.addheaders = [('User-agent', "ruaoks IA matcher (1.00)")]
            response = opener.open(url, timeout = 15)
            break
        except urllib2.HTTPError, e:
            if e.code == 503:
                print "Got 503. Sleeping %s seconds" % timeout_delay
                sleep(timeout_delay)
                timeout_delay *= 2
                continue
            if e.code == 400:
                set_error(conn, iaid, "400 on fetch from mb")
                continue

            print "HTTP error: " , e
            return False
        except urllib2.URLError, e:
            print "URL error: " , e
            return False

    data = response.read()
    t1 = time()
    next_delay = max(0, 1.0 - (t1 - t0))

    cur = conn.cursor()
    cur.execute("UPDATE match SET mb_data = %s, ts = now() WHERE iaid = %s", (data, iaid))
    conn.commit()

    js = json.loads(data)
    if len(js['releases']) == 0:
        return "zero mb hits"

    return "ok"

try:
    conn = psycopg2.connect(config.PG_CONNECT)
    conn2 = psycopg2.connect(config.PG_CONNECT)
except psycopg2.OperationalError as err:
    print "Cannot connect to database: %s" % err
    sys.exit(-1)

while True:
    cur = conn.cursor()
    cur.execute("""SELECT iaid, ia_data 
                     FROM match 
                    WHERE mb_data IS NULL 
                      AND mbid IS NULL 
                      AND err IS NULL 
                 ORDER BY ts ASC 
                    LIMIT 100""")
    rows = cur.fetchall()
    cur.close()

    for row in rows:
        ret = mb_download(conn, row[0], row[1])
        print "%-30s %s" % (ret, row[0])
        sleep(.75)

print "done"
