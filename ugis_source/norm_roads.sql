
-- updates location of roads with region_name by comparing geometry with loc table
/*
with q10 as
(
  select rd.objectid, l.name_id from geotools.roads rd
  join geotools.loc l on st_intersects(l.geom, rd.geom) and l.name_id in ('rwenzori','western','south_western')
)
update geotools.roads r set loc = ARRAY[q.name_id]
from q10 q where q.objectid = r.objectid ;
*/

-- following snippet was already executed before loop
/*
with q10 as
(
  select rd.objectid, l.name_id from geotools.roads rd
  join geotools.loc l on st_intersects(l.geom, rd.geom) and l.name_id in ('north_eastern', 'mid_western','central_north')
)
update geotools.roads r set loc = ARRAY[q.name_id]
from q10 q where q.objectid = r.objectid ;
*/


--RAISE NOTICE ' func : %', geotools.explode_rds('MASAKA'); -- no need import explode the roads
-- this part was executed one after another foar each region of ['rwenzori','western','south_western']

DO LANGUAGE 'plpgsql'
$$
DECLARE
cnt INTEGER:=0;
i   INTEGER:=0;
t1 TIMESTAMP:=clock_timestamp();
t2 TIMESTAMP:=clock_timestamp();
arr_loc TEXT[];
in_loc TEXT;
BEGIN
  RAISE NOTICE 'Script started at %', t1;
--arr_loc := ARRAY['rwenzori','western','south_western'];
  arr_loc := ARRAY['north_eastern', 'mid_western','central_north'];
  --arr_loc := ARRAY['south_western'];
  --needs to run one by one for location, otherwise took long time
  arr_loc := ARRAY['central_north'];

  FOREACH in_loc in ARRAY arr_loc

  LOOP
    RAISE NOTICE '--------------------------%------------------------------',in_loc;
    RAISE NOTICE '01 - del_rds_by_len: %',geotools.del_rds_by_len(2,in_loc);
    RAISE NOTICE '02 - del_overlaped_rds_2: %', geotools.del_overlaped_rds_2(in_loc);
    RAISE NOTICE '03 - split_rd_by_line: %', geotools.split_rd_by_line(in_loc); --investigate why after the first call the roads are further split

    RAISE NOTICE '04 - snap_rds : %', geotools.snap_rds (15,in_loc);
    RAISE NOTICE '05 - del_overlaped_rds_2 : %', geotools.del_overlaped_rds_2(in_loc);
    RAISE NOTICE '06 - node_rd: %',  geotools.node_rd(in_loc);
    RAISE NOTICE '07 - segment_rd : %', geotools.segment_rd(50,in_loc);
    RAISE NOTICE '08 - del_rds_by_len : %', geotools.del_rds_by_len(2,in_loc);
    RAISE NOTICE '09 - del_overlaped_rds_2 : %', geotools.del_overlaped_rds_2(in_loc);
    RAISE NOTICE '10 - snap_rds : %', geotools.snap_rds (5,in_loc);
    RAISE NOTICE '11 - del_rds_by_len : %', geotools.del_rds_by_len(0,in_loc);
    t2 :=clock_timestamp();
END LOOP ;
  END ;
$$ ;

-- following needs run after loop

-- updates location of roads by adding district name in the location array with district name by comparing geometry with loc table
-- for the district name
with q10 as
(
  select rd.objectid, l.name_id from geotools.roads rd
  join geotools.loc l on st_intersects(l.geom, rd.geom) and
                         l.name_id

                         in (select distinct d.name_id from geotools.loc r join geotools.loc d on  st_intersects(d.geom,r.geom) and  st_area(st_intersection(d.geom,r.geom)) > st_area(d.geom)*.55 WHERE d.loc_type='district'
and r.loc_type='region' and r.name_id in ('north_eastern', 'mid_western','central_north') )
)
update geotools.roads r set loc = loc||ARRAY[q.name_id]
from q10 q where q.objectid = r.objectid ;
/*
select distinct d.name_id from geotools.loc r join geotools.loc d on  st_intersects(d.geom,r.geom) and  st_area(st_intersection(d.geom,r.geom)) > st_area(d.geom)*.55 WHERE d.loc_type='district'
and r.loc_type='region' and r.name_id in ('north_eastern', 'mid_western','central_north') ;
*/
with q10 as (
select distinct d.name_id from geotools.loc r join geotools.loc d on  st_intersects(d.geom,r.geom) and  st_area(st_intersection(d.geom,r.geom)) > st_area(d.geom)*.55 WHERE d.loc_type='district'
and r.loc_type='region' and r.name_id in ('north_eastern', 'mid_western','central_north') )
select array_agg(name_id) from q10;

-- for checking the region that has area not in any district
--select a.*,array_length(loc,1) from geotools.roads a where loc &&ARRAY['buhweju'] and array_length(loc,1) < 2;
