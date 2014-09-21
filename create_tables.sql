BEGIN;

CREATE TABLE match (
        iaid text not null, 
        mbid text default null, 
        ts timestamp default current_timestamp,
        ia_data text default null, 
        mb_data text default null,
        err text default null,
        score float default 0.0,
        tracks integer default 0
);
CREATE UNIQUE INDEX iaid_ndx on match (iaid);
CREATE INDEX iaid_mbid_ndx on match (iaid, mbid);
CREATE INDEX mbid_ndx on match (mbid);
CREATE INDEX score_ndx on match (score);

COMMIT;
