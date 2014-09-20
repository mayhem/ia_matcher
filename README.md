ia_matcher
==========

Internet Archive audio track to MusicBrainz release matching tool

This tool uses ia_download.py to download data from the Internet Archive for a list of items into a local postgres db.
Then mb_matcher.py picks items from the DB, tries to match them and saves matches/errors back to the DB.

The err field in the match table of the DB stores a free form text string of what happened during the last time that
this "match" was worked on. Possible values include:

 cannot parse ia json
 too few tracks on this release
 0 tracks found in json
 release with missing track duration
 incomplete tracklist
 key error in ia json
 match below threshold
 no toc match
 
 Knowing the error, the script can be improved to try and match more data or improve the parsing scripts to deal
 the shitty inbound data.
 
 When a match is found, the result is written to the mbid column in the match table.
