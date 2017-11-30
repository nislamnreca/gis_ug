--*************************************************************************************
CREATE OR REPLACE FUNCTION geotools.update_prj( in_loc TEXT, in_rev TEXT)
  RETURNS BIGINT AS
  $$
  DECLARE
    rt_val BIGINT:=0;
    myrec record;
  BEGIN

for myrec in (
with RECURSIVE q10 (prj,objectid,ar_objectid,source,target) as (
  SELECT
    l.objectid prj,
    l.objectid objectid,
    ARRAY [l.objectid] ar_objectid,
    source,
    target
  from geotools.nodes n
    JOIN geotools.new_lines l on st_dwithin(l.geom,n.geom,1)
  WHERE n.node_type in ('rd_line', 'line')
    --and n.objectid=27627100--27328984
    and n.loc=in_loc and n.rev=in_rev and l.loc=in_loc and l.rev=in_rev
    and l.target<>0 and l.source<>0 and l.v_level='mv'
  UNION
    SELECT   prj,
    nl.objectid objectid, ar_objectid||array[nl.objectid] ar_object, nl.source,nl.target
    from geotools.new_lines  nl
    JOIN q10 r on ( r.source in(nl.source,nl.target) or r.target in(nl.source,nl.target))
	and not ar_objectid && ARRAY[nl.objectid] and nl.target<>0 and nl.source<>0 and nl.v_level='mv'
   where     nl.loc=in_loc and nl.rev=in_rev
)
  select * from q10 ) LOOP
  UPDATE geotools.new_lines l SET l.proj=q.prj
  where l.objectid = myrec.objectid;
  rt_val := rt_val+1;
END LOOP;


    RETURN  rt_val;
END ;

    $$ LANGUAGE 'plpgsql';


TRUNCATE geotools.roads;
  WITH q10 AS (--1m 4s 142ms
    SELECT
      row_to_json(rd.*) original_record,
      geotools.explode_linestring((st_dump(st_transform(rd.the_geom,3857))).geom)  geom
    FROM import_data.merged_roads rd
)
,q20 AS (
    SELECT
      array_agg(l.name_id)                                          loc,
      (array_agg(rd.original_record))[1]                 original_record,
      rd.geom geom
    FROM q10 rd
    LEFT JOIN geotools.loc l ON st_intersects(l.geom, rd.geom) and l.loc_type='district'
   GROUP BY rd.geom
)
INSERT INTO geotools.roads  ( loc, original_record, geom)
select * from q20;