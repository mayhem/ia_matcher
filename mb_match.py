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

MATCH_THRESHOLD = .4
MIN_NUM_TRACKS = 3

# Found track naming schemes
# Various Artists - Casa De Mi Padre (Original Motion Picture Soundtrack) (2012) [FLAC]/01 - Christina Aguilera - Casa De Mi Padre.flac
# Wild's Reprisal - Cascadia Rising (2012) [FLAC]/Wild's Reprisal - Cascadia Rising - 01 A fierce green fire dying in her eyes.flac
# The Deadly Gentlemen - Carry Me To Home [FLAC]/The Deadly Gentlemen - Carry Me To Home - 01 Sober Cure.flac
# The New York Lounge Society - Christmas Chill (2010) [FLAC]/01 - Joy to the World.flac
# Twista-Category_F5-2009-H3X/06-twista-walking_on_ice_ft._gucci_mane_and_oj_da_juiceman.mp3

def clean_string(s):
    return unicode(re.sub('[-\W_]', '', s).lower())

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
    cur.execute('UPDATE match SET ts = now() where iaid = %s', (iaid,))
    conn.commit()

def set_mbid(conn, iaid, mbid):
    cur = conn.cursor()
    cur.execute('UPDATE match SET ts = now(), mbid = %s where iaid = %s', (mbid, iaid))
    conn.commit()

def match_tracks_to_release(title, tracks, mbid, name):
    '''
       Given a list of tracks, a candidate release mbid and name this function should return
       a score (0.0-1.0) of how well this list of tracks compares to the MB release.
    '''

    try:
        musicbrainzngs.set_useragent("ruaok's IA matcher", "0.1", "rob@")
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

next_delay = 0.0
def mb_download(conn, iaid, data):
    global next_delay

    release = { 'durations' : [], 'tracks' : [] }
    num_tracks = 0

    try:
        js = json.loads(data)
    except ValueError:
        return set_error(conn, iaid, "cannot parse ia json")

    tracks = {}
    release_name = ""
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

            if not release_name:
                try:
                    release_name = f['name'].split('/')[0]
                    release_name = release_name.split('-')[1].strip()
                except IndexError:
                    pass

#            print "1", name

            # remove the file extension
            ri = name.rfind(".")
            if ri != -1:
                name = name[:ri]

#            print "2", name

            # remove everything before the last '-'
            ri = name.rfind("-")
            if ri != -1:
                name = name[(ri+1):]

#            print "3", name


            # if the track number is at the beginning of the track text, nuke it.
            name = name.strip()
            if name.startswith("%02d" % (int(track) + 1)):
                name = name[2:].strip()

#            print "4", name
#            print 

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
             ORDER BY distance""" % (dur_str, dur_str, 3000)
    cur = conn.cursor()
    cur.execute(query)
    rows = cur.fetchall()
    cur.close()

    if rows:
        hi = -1
        hi_index = -1
        for i, row in enumerate(rows):
            score = match_tracks_to_release(release_name, tracks, row[0], row[1])
            if score > hi:
                hi = score
                hi_index = i

        if hi_index >= 0:
            if hi > MATCH_THRESHOLD:
                mbid = rows[hi_index][0]
                print "picking %s - %.3f" % (mbid, hi)
                set_mbid(conn, iaid, mbid)
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
    cur.execute("""SELECT iaid, ia_data 
                     FROM match 
                    WHERE mb_data IS NULL 
                      AND mbid IS NULL 
                      AND err IS NULL 
                 ORDER BY ts ASC 
                    LIMIT 100""")
    rows = cur.fetchall()
    cur.close()

    if len(rows) == 0:
        break

    for row in rows:
        ret = mb_download(conn, row[0], row[1])
        print "%-37s %s" % (ret, row[0])

print "done"
