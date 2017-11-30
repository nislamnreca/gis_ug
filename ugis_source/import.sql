/*
select
  table_name,
  cnt_row(t.table_schema,t.table_name) row_cnt,
  sum(cnt_row(t.table_schema,t.table_name))
  over(partition by case when t.table_name not in ('structures','all_structures_merge') then 'a' else t.table_name END)
from information_schema.tables t
WHERE t.table_schema='import_data' and t.table_name ~'.*structure.*';
;
*/

-- Import Locations into loc table

--truncate geotools.loc;

WITH q_districts AS (
    SELECT
      regexp_replace(
          regexp_replace(
              lower(dname_2011),
              '^[^\da-z_]|[^\da-z_]$',
              '',
              'g'),
          '[^\da-z_]{1,}',
          '_',
          'g')                                                  name_id,
      dname_2011                                                lbl,
      'district'::TEXT                                                loc_type,
      st_transform(
          st_setsrid((st_dump(st_forcecollection(wkb_geometry))).geom, 21096)
          , 3857) geom
    FROM import_data.district d
)
  , q_zones AS (
    SELECT
      regexp_replace(
          regexp_replace(
              lower(srvc_ter_n),
              '^[^\da-z_]|[^\da-z_]$',
              '',
              'g'),
          '[^\da-z_]{1,}',
          '_',
          'g')                                                  name_id,
      srvc_ter_n                                               lbl,
      'region'::TEXT                                              loc_type,

      st_transform(
          st_setsrid((st_dump(st_forcecollection(wkb_geometry))).geom, 21096)
          , 3857) geom
    FROM import_data.service_territory v
)
,q_union AS (
  SELECT * from q_districts
  UNION ALL
  SELECT * from q_zones
)
,q10 as (
  SELECT
    name_id,
    row_number() OVER (PARTITION BY name_id) name_id_suffix,
    count(name_id) OVER (PARTITION BY name_id) name_id_cnt,
    lbl,
    loc_type,
    geom
  FROM q_union
  WHERE st_area(geom)>=50^2 and name_id is not NULL
)
,q20 AS (
    SELECT
    name_id || CASE WHEN name_id_cnt>1 THEN '_'||name_id_suffix ELSE '' END name_id,
    lbl,
    loc_type,
    geom
    from q10
)
 INSERT INTO geotools.loc (name_id, name_id_list, fk_objectid, lbl, loc_type,  geom)
SELECT name_id, array[name_id], null, lbl, loc_type, geom from q20;--runtim 1m 22s

with q10 as (
select
  d.objectid obj_id,
  r.name_id r_nameid,
  d.name_id d_nameid
FROM geotools.loc d
join geotools.loc r on d.geom&&r.geom and st_area(st_intersection(d.geom, r.geom)) >= st_area(d.geom)*0.60 and d.loc_type='district' and r.loc_type='region' )
update geotools.loc l
  set name_id_list = l.name_id_list||ARRAY[a.r_nameid]
FROM  q10 a where l.objectid = a.obj_id;

--select * from q10 ;


     VACUUM ANALYSE geotools.loc ;
--########################################################
--Import buildings
--truncate geotools.bldgs ;

WITH q10 AS
(
  SELECT DISTINCT
    st_transform(
       st_setsrid((st_dump(st_forcecollection(wkb_geometry))).geom, 21096)
       , 3857) geom
FROM import_data.structure
)
, q20 as (
  SELECT
     geom,
     st_snaptogrid( geom, GeomFromEWKT('SRID=3857;POINT(0 0)'), 50, 50, 0, 0) snap_geom
  from q10
)
INSERT INTO geotools.bldgs ( geom, snap_geom)
SELECT * FROM q20;

VACUUM ANALYSE geotools.bldgs;
REFRESH MATERIALIZED VIEW geotools.mvw_loc;
VACUUM ANALYSE geotools.mvw_loc;

--#####################  Update BLDG loc field  #####################################################
WITH q10 as( --7m 56s 492ms
    select
     b.objectid,
     l.name_id,
      l.loc_type,
     row_number() OVER (PARTITION BY b.objectid, l.loc_type ORDER BY b.geom<->l.geom  asc) rnk
from geotools.bldgs b
join  geotools.mvw_loc l on st_dwithin(b.geom,l.geom,1000)
)
, q20 AS (
      SELECT
        objectid,
        (array_agg(name_id)  FILTER (WHERE loc_type = 'district' AND rnk = 1)) [1] loc,
        array_agg(name_id)   FILTER (WHERE rnk = 1)                        loc_list
      FROM q10
      GROUP BY objectid
  )
UPDATE geotools.bldgs b SET loc=z.loc , loc_list=z.loc_list
FROM q20 z
where b.objectid=z.objectid;


--##################################################################################

TRUNCATE geotools.dtr ;

  with q20 AS (
      SELECT
      equip_size :: NUMERIC kva,
      st_transform( (st_dump(st_forcecollection(st_setsrid(wkb_geometry,21096)))).geom , 3857) geom
      FROM import_data.poles z
      WHERE trim(lower(equip_type)) IN ('transformer')

  )
  INSERT INTO geotools.dtr ( kva, geom)
  SELECT kva, geom from q20 q ON CONFLICT  DO NOTHING ;


--##########################################################################


update  geotools.bldgs b set sts='cluster' where b.loc_list&&ARRAY['rwenzori','western','south_western'] ;

update  geotools.bldgs b set sts='cluster' where b.loc_list&&ARRAY['north_eastern', 'mid_western','central_north'] and sts <> 'energized' ;




--##########################################################################
update  geotools.bldgs set sts='energized' --  24s 542ms
 where objectid in (
 select b.objectid from geotools.bldgs b
   join geotools.dtr t on st_dwithin(b.geom, t.geom,1000));

--##########################################################################


-- UPDATE geotools.bldgs b SET hhr=z.hhr --42s
-- FROM (
-- select
--   b.objectid,
--   count(b.objectid) over (PARTITION BY sub_loc) cnt,
--   (original_record->>'hh16')::NUMERIC hh16,
--    round( (original_record->>'hh16')::NUMERIC / (count(b.objectid) over (PARTITION BY sub_loc)),2) hhr
--
-- from geotools.bldgs b
-- join geotools.loc l on b.sub_loc=l.lbl and l.ab_type='village'
-- ) z
-- where b.objectid=z.objectid and ( b.sts is  NULL or b.sts<>'skip') ;

--SELECT * from geotools.bldgs b where  ( b.sts is  NULL or b.sts<>'skip') ;
--##########################################################################
-- UPDATE geotools.bldgs b SET hhr=z.hhr --2m 42s
-- FROM (
-- select
--   b.objectid,
--   count(b.objectid) over (PARTITION BY loc) cnt,
--   (original_record->>'hh16')::NUMERIC hh16,
--    round( (original_record->>'hh16')::NUMERIC / (count(b.objectid) over (PARTITION BY loc)),2) hhr
--
-- from geotools.bldgs b
-- join geotools.loc l on b.loc=l.lbl and l.ab_type='district'
-- ) z
-- where b.objectid=z.objectid and (b.hhr<0.2 or b.hhr>10)  and ( b.sts is NULL or b.sts<>'skip');

--SELECT * from geotools.bldgs b where   (b.hhr<0.2 or b.hhr>10)  and ( b.sts is  NULL and b.sts<>'skip');
  --##########################################################################
--UPDATE geotools.bldgs b SET hhr=1 --where hhr is null ;
-- select * from geotools.bldgs b  where hhr is null;
--
-- update  geotools.bldgs set hhr=0 where sts='skip';
-- update  geotools.bldgs set hhr=1 where hhr<0.1 or hhr>10 ;


--######################################################################################
--DELETE  FROM geotools.roads WHERE loc @> '{"MASAKA"}';
TRUNCATE geotools.roads;
  WITH q10 AS (--1m 4s 142ms
    SELECT
    geotools.explode_linestring((st_dump(st_transform(rd.wkb_geometry,3857))).geom)  geom
    FROM import_data.roads rd
)
,q20 AS (
    SELECT
         rd.geom geom
    FROM q10 rd

)
INSERT INTO geotools.roads  ( geom )
select ST_Force2D(geom) from q10 on CONFLICT  DO NOTHING ;


 --#################################################

  TRUNCATE geotools.lines ;
 WITH q10 AS (
    SELECT
  substation sub,
  feeder fd,
  line_voltage volt,
      geotools.explode_linestring((st_dump(st_transform(ln.wkb_geometry,3857))).geom)  geom
    FROM import_data.lines ln
)

INSERT INTO geotools.lines (sub, fd, volt,  geom)
select * from q10 ;

--##########################################################################
--SELECT * FROM geotools.demand_curve d WHERE d.kwh = 100 AND cust=650 ORDER BY demand ASC limit 1 ;


