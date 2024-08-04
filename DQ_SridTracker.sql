CREATE DATABASE dq;

CREATE TABLE dq.srid_tracker engine = ReplacingMergeTree order by dt_hour as
select toStartOfHour(toDateTime(dt)) dt_hour
     , uniq(place_cod) qty_place_cod
     , uniq(action_id) qty_action
     , uniq(rid_hash) qty_rid
     , round(qty_rid/qty_place_cod,2) rids_per_place_cod
     , countIf(place_cod = 0) qty_place_null
     , count() qty
     , round(qty/qty/qty_place_null,4) nulls_per_record
from stage.srid_tracker
group by dt_hour;
