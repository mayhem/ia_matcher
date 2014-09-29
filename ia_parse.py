#!/usr/bin/env python

import re
import json

def clean_string(s):
    return unicode(re.sub('[-\W_]', '', s).lower())

class ParseArchiveData(object):

    def __init__(self):
        pass

    @staticmethod
    def _pick_duration(tracks):
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

    def parse(self, iaid, data):
        num_tracks = 0
        try:
            js = json.loads(data)
        except ValueError:
            return ({}, "cannot parse ia json")

        tracks = {}
        release_name = ""
        try:    
            for f in js['files']:
                if f.get('source', 'derivative') == 'derivative':
                    continue

                if not f['name'].endswith(".mp3") and not f['name'].endswith(".flac"):
                    continue

                track = ""
                # If we have track field, parse it
                if f.has_key('track'):
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

                # see if the track starts with a number
                if not track: 
                    m = re.match('^[0-9]+', f['name'])
                    if m:
                        try:
                            track = int(m.group(0))
                        except ValueError:
                            pass

                if not track: 
                    continue

                try:
                    duration = float(f['length'])
                except:
                    if f['length'].find(':') >= 0:
                        m, s = f['length'].split(':')
                        duration = float(m) * 60.0 + float(s)
                    else:
                        return ({}, "Uknown length format: '%s'" % f['length'])

                if f.has_key('album') and f['album']:
                    release_name = f['album']

                if not release_name:
                    try:
                        release_name = f['name'].split('/')[0]
                        release_name = release_name.split('-')[1].strip()
                    except IndexError:
                        pass

                if f.has_key('title') and f['title']:
                    name = f['title']
                else:
                    try:
                        name = f['name'].split('/')[1]
                    except IndexError:
                        name = f['name']

                    # remove the file extension
                    ri = name.rfind(".")
                    if ri != -1:
                        name = name[:ri]

                    # remove everything before the last '-'
                    ri = name.rfind("-")
                    if ri != -1:
                        name = name[(ri+1):]

                    # if the track number is at the beginning of the track text, nuke it.
                    name = name.strip()
                    if name.startswith("%02d" % (int(track) + 1)):
                        name = name[2:].strip()

                clean = clean_string(name)
                if not tracks.has_key(track):
                    tracks[track] = []
                tracks[track].append(dict(num=track, duration=duration, name=name, clean=clean, source=f['source']))

        except KeyError:
            return ({}, "key error in ia json")

        track_list = [ ]
        for i in xrange(len(tracks)):
            try:
                dummy = tracks[i]
            except KeyError:
                return ({}, "incomplete tracklist")

            for t in tracks[i]:
                if t['source'] == 'original':
                    if t['duration'] == 0.0:
                        t['duration'] = self._pick_duration(tracks[i])
                        if t['duration'] == 0.0:
                            return ({}, "release with missing track duration")

                    track_list.append(t)

        if len(track_list) == 0:
            return ({}, "0 tracks found in json")

        return (dict(tracks=track_list, name=release_name), "")
