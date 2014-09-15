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
import config

MUSICBRAINZ_HOST = "asterix.mb"
MUSICBRAINZ_PORT = 80

def clean_string(s):
    return re.sub('[-\W_]', '', s).lower()

def match(conn, iaid, data):
    cur = conn.cursor()
    release = { 'durations' : [], 'tracks' : [] }
    num_tracks = 0

    js = json.loads(data)

    tracks = []
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
                duration = int(float(f['length']))
                if duration < 1000:
                    duration = duration * 75
            except:
                m, s = f['length'].split(':')
                duration = (int(m) * 60 + int(s)) * 75

            try:
                name = f['name'].split('/')[1]
            except IndexError:
                name = f['name']

            try:
                name = name.split(".")[0]
            except:
                pass

            clean = clean_string(name)
            tracks.append(dict(num=track, duration=duration, name=name, clean=clean))
    except KeyError:
        cur.execute('UPDATE match SET ts = now() where iaid = %s', (iaid, ))
        conn.commit()
        return (iaid, "")

    if len(tracks) == 0:
        cur.execute('UPDATE match SET ts = now() where iaid = %s', (iaid, ))
        conn.commit()
        return (iaid, "")

    tracks = sorted(tracks, key=itemgetter('num', 'name'))
    new_list = [ tracks[0] ]
    for i in xrange(len(tracks)):
        if i == 0: 
            continue

        if  tracks[i]['num'] == tracks[i-1]['num']:
            continue

        new_list.append(tracks[i])

    tracks = new_list

#    for t in tracks:
#        print "%d. %s - %d:%02d (%d)" % (t['num'], t['name'], t['duration'] / 75 / 60, t['duration'] / 75 % 60, t['duration'])

    if len(tracks) < 5:
        if len(tracks) < 1:
            print "Failed to parse data for %s" % iaid
        else:
            if len(tracks) < 5:
                print "too few tracks on this release"

        cur.execute('UPDATE match SET ts = now() where iaid = %s', (iaid, ))
        conn.commit()
        return (iaid, "")

    total_sectors = 150
    for t in tracks:
        total_sectors += t['duration']

    toc = "1+%d+%d+150" % (len(tracks), total_sectors)
    offset = 150
    for t in tracks[:-1]:
        toc += "+%d" % (offset + t['duration'])
        offset += t['duration']

    url = 'http://musicbrainz.org/ws/2/discid/-?toc=%s&media-format=all&fmt=json&inc=recordings' % toc

    opener = urllib2.build_opener()
    opener.addheaders = [('User-agent', "ruaoks IA matcher (1.00)")]
    while True:
        try:    
            response = opener.open(url, timeout = 15)
            break
        except urllib2.HTTPError, e:
            if e.code == 503:
                print "Got 503. Sleeping!"
                sleep(9)

            print "HTTP error: " , e
            return (iaid, "")
        except urllib2.URLError, e:
            print "URL error: " , e
            return (iaid, "")

    data = response.read()
    js = json.loads(data)

    matches = []
    for r in js['releases']:
        for m in r['media']:
            total = 0.0
            for i, t in enumerate(m['tracks']):
                try:
                    d = Levenshtein.ratio(clean_string(t['recording']['title']), tracks[i]['clean'])
                    print "%.3f %s %s" % (d, clean_string(t['recording']['title']), tracks[i]['clean'])
                    total += d
                except IndexError:
                    continue
            matches.append(dict(score = total / len(tracks), mbid = r['id']))

    mbid = ""
    if len(matches) > 0:
        top = sorted(matches, key=itemgetter('score'), reverse=True)[0]
        if top['score'] > .8:
            print "%d %s" % (top['score'] * 100, top['mbid'])
            # We found data, and got a match!
            mbid = top['mbid']

        print

    # Update the data we have. If the mbid has been filled out, save it. In either case, update timestamp
    try:
        cur.execute('UPDATE match SET ts = now(), mbid = %s where iaid = %s', (mbid, iaid))
        conn.commit()
        return (iaid, mbid)
    except psycopg2.OperationalError as err:
        print "Cannot save match. ", err

    return (iaid, "")

try:
    conn = psycopg2.connect(config.PG_CONNECT)
    conn2 = psycopg2.connect(config.PG_CONNECT)
except psycopg2.OperationalError as err:
    print "Cannot connect to database: %s" % err
    sys.exit(-1)

while True:
    cur = conn.cursor()
    cur.execute("SELECT iaid, ia_data FROM match WHERE mbid = '' ORDER BY ts ASC LIMIT 100")
    for row in cur:
        iaid, mbid = match(conn2, row[0], row[1])
        print "%s -> %s" % (iaid, mbid)

print "done"
