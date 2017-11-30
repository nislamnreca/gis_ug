-- prj and summary view
CREATE OR REPLACE FUNCTION geotools.update_prj_nzi(in_loc TEXT, in_rev TEXT)
  RETURNS BIGINT
LANGUAGE plpgsql
AS $$
DECLARE
  rt_val     BIGINT :=0;
  tmp_val    BIGINT :=0;

  myrec      geotools.new_lines;
  array_cmp BIGINT[];
  ucount bigint :=0;
  precount bigint :=0;
  postcount bigint :=0;
  prj_done BIGINT:=0;
  prj_count bigint :=0;


start_time TIMESTAMP;
time_passed text;
BEGIN
   start_time := CURRENT_TIMESTAMP;

--     RAISE NOTICE 'Time passed % ',clock_timestamp()::TIMESTAMP- start_time::TIMESTAMP ;

   update geotools.new_lines n set proj = null
     where  n.loc = in_loc AND n.rev = in_rev and n.v_level = 'mv';
    prj_done := 1::BIGINT;
  SELECT
                  count(*) into prj_count
                FROM geotools.nodes n
                  JOIN geotools.new_lines l ON st_dwithin(l.geom, n.geom, 1)
                WHERE n.node_type IN ('rd_line', 'line')
                      AND n.loc = in_loc AND n.rev = in_rev AND l.loc = in_loc AND l.rev = in_rev
                      AND l.target <> 0 AND l.source <> 0 AND l.v_level = 'mv' ;
  --cur_project_no := 1::BIGINT;


  FOR myrec IN (SELECT
                  l.*
                FROM geotools.nodes n
                  JOIN geotools.new_lines l ON st_dwithin(l.geom, n.geom, 1)
                WHERE n.node_type IN ('rd_line', 'line')
                      AND n.loc = in_loc AND n.rev = in_rev AND l.loc = in_loc AND l.rev = in_rev
                      AND l.target <> 0 AND l.source <> 0 AND l.v_level = 'mv'
                     --and l.objectid in ( 11622247::BIGINT, 11454925::BIGINT)

  )

  LOOP


    UPDATE geotools.new_lines l
    SET proj = myrec.objectid :: TEXT
    WHERE objectid = myrec.objectid ;
    tmp_val := 2;
    ucount :=2;

    select count(*) into precount from geotools.new_lines l  where proj = myrec.objectid :: TEXT;
    postcount :=0;

    WHILE ( tmp_val > 0 ) LOOP
      --RAISE NOTICE 'Current object id in  % completed  %', myrec.objectid, prj_done ;
      tmp_val :=0;


      SELECT array_agg(n1.source) || array_agg(n1.target) into array_cmp
        FROM geotools.new_lines n1
        WHERE  n1.loc = in_loc AND n1.rev = in_rev  and n1.v_level = 'mv' and coalesce(n1.proj,'none') = myrec.objectid :: TEXT  AND n1.target <> 0 AND n1.source <> 0;

       --RAISE NOTICE 'comparing  in loc % and revision % # ',  in_loc, in_rev;

        WITH q10 AS (
         select * from  geotools.new_lines l

        WHERE l.objectid IN (
          SELECT a.objectid
          FROM geotools.new_lines a
          WHERE (a.loc = in_loc AND a.rev = in_rev and a.v_level = 'mv' and a.proj is null
                )  AND (ARRAY [a.source, a.target] &&    array_cmp   )   ))
          , q_update AS (
          UPDATE geotools.new_lines l
          SET proj = myrec.objectid :: TEXT
          FROM q10 q
          WHERE l.objectid = q.objectid
          RETURNING 1 )
        SELECT count(*)
        INTO tmp_val
        FROM q_update;
         select count(*) into postcount from geotools.new_lines l  where proj = myrec.objectid :: TEXT;
        --tmp_val := 0;
        time_passed := clock_timestamp()::TIMESTAMP- start_time::TIMESTAMP ;
         RAISE NOTICE '% segment added to project# % of % in location: % with projectid: % making % segment total, time passed: % ', tmp_val, prj_done,prj_count, in_loc , myrec.objectid :: TEXT, postcount,time_passed ;

        rt_val := rt_val + tmp_val;
      END LOOP; -- myrec2
      prj_done := prj_done+1;


  END LOOP;


  RETURN rt_val;
END;
$$;

create or replace view results_export.vw_proj_summary as
WITH q10 AS (
    SELECT
      l.proj,
      l.v_level,
      l.voltage,
      l.phase,
      l.conductor,
      eqp,
      ceil(sum(st_length(l.geom))) line_length
    FROM geotools.new_lines l
    GROUP BY l.proj, l.v_level, l.voltage, l.phase, l.conductor, eqp
    ORDER BY l.proj, l.v_level, l.voltage, l.phase, l.conductor
)

  , q20 AS (
    SELECT
      a.proj,
      b.u_cost,
      case when a.v_level='mv' then a.line_length else 0 end mv_length,
      case when a.v_level='lv' then a.line_length else 0 end lv_length,
      (a.line_length * b.u_cost) / 1000          total_cost
    FROM q10 a LEFT JOIN config.equipment b ON a.eqp = b.lbl

)
  , q25 AS ( SELECT
               a.proj,
               ceil(sum(a.mv_length)/1000) mv_length,
               ceil(sum(a.lv_length)/1000) lv_length,
               sum(total_cost) lcost
             FROM q20 a
             WHERE a.proj IS NOT NULL
             GROUP BY a.proj
)
  , q30 AS (
    SELECT
      cl.proj,
      cl.tr_size,
      eqp,
      count(*)       tr_cnt_by_proj,
      sum(cl.hh_cnt) hh_cnt
    FROM geotools.bldgs_cl cl
    GROUP BY cl.proj, cl.tr_size, eqp
    ORDER BY cl.proj, cl.tr_size
),
    q40 AS
  (
      SELECT
        a.*,
        b.u_cost,
        b.u_cost * a.tr_cnt_by_proj total_tr_cost,
        e.u_cost                    unit_svc_cost,
        e.u_cost * a.hh_cnt         total_svc_cost
      FROM q30 a LEFT JOIN config.equipment b ON a.eqp = b.lbl
        CROSS JOIN config.equipment e
      WHERE e.lbl = 'svc_unk'

  )
  , q45 AS (
    SELECT
      a.proj,
      a.tr_cnt_by_proj,
      sum(hh_cnt)                                                   hh_cnt,
      sum(coalesce(total_tr_cost, 0) + coalesce(total_svc_cost, 0)) trcost
    FROM q40 a
    WHERE a.proj IS NOT NULL
    GROUP BY a.proj, a.tr_cnt_by_proj)
  ,q50  as (
SELECT
  l.proj,
  tr.hh_cnt,
  tr.tr_cnt_by_proj,
  l.lv_length,
  l.mv_length,
  ceil(l.lcost + tr.trcost)               total_cost,
  ceil((l.lcost + tr.trcost) / tr.hh_cnt) cost_per_hh
FROM q25 l
  JOIN q45 tr ON l.proj = tr.proj)
select * from q50;

-- output-1
create or replace view results_export.vw_line_summary as
with q10 as (
select l.proj, l.v_level, l.voltage, l.phase, l.conductor, eqp,ceil(sum(st_length(l.geom))) line_length
from geotools.new_lines l
group by l.proj, l.v_level, l.voltage, l.phase, l.conductor,eqp
order by l.proj, l.v_level, l.voltage, l.phase, l.conductor)
  select a.*, b.u_cost, (a.line_length* b.u_cost) /1000 total_cost from q10 a left join config.equipment b on a.eqp =b.lbl ;

-- output 2
create or replace view results_export.vw_transformer_summary as
with q10 as (
select cl.proj, cl.tr_size, eqp, count(*) tr_cnt_by_proj , sum(cl.hh_cnt) hh_cnt
from geotools.bldgs_cl cl
group by cl.proj, cl.tr_size, eqp
order by cl.proj, cl.tr_size)
select a.*,b.u_cost, b.u_cost*a.tr_cnt_by_proj total_tr_cost, e.u_cost unit_svc_cost, e.u_cost*a.hh_cnt total_svc_cost  from q10 a  left join config.equipment b on a.eqp =b.lbl
cross join config.equipment e where e.lbl='svc_unk' and b.u_cost is null;

--*************************************************************************************

--road norm
create or replace function geotools.exec_tmp0(in_loc TEXT, kwh_month NUMERIC,min_tr_sz NUMERIC) returns void
LANGUAGE 'plpgsql'
AS
$$
DECLARE
cnt INTEGER:=0;
i   INTEGER:=0;
t1 TIMESTAMP:=clock_timestamp();
t2 TIMESTAMP:=clock_timestamp();
arr_loc TEXT[];
in_loc TEXT;
BEGIN

--========
with q10 as
(
  select rd.objectid, l.name_id from geotools.roads rd
  join geotools.loc l on st_intersects(l.geom, rd.geom)
                         and l.name_id in (in_loc)
)
update geotools.roads r set loc = ARRAY[q.name_id]
from q10 q where q.objectid = r.objectid ;

--========


  RAISE NOTICE 'Script started at %', t1;
--arr_loc := ARRAY['rwenzori','western','south_western'];

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

with q10 as
(
  select rd.objectid, l.name_id from geotools.roads rd
  join geotools.loc l on st_intersects(l.geom, rd.geom) and
                         l.name_id

                         in (select distinct d.name_id from geotools.loc r join geotools.loc d on  st_intersects(d.geom,r.geom) and  st_area(st_intersection(d.geom,r.geom)) > st_area(d.geom)*.55 WHERE d.loc_type='district'
and r.loc_type='region' and r.name_id in (in_loc) )
)
update geotools.roads r set loc = loc||ARRAY[q.name_id]
from q10 q where q.objectid = r.objectid ;


  END ;
$$ ;

----------------------- end of road normalization1-------------------

--in_len numeric, in_loc text
-- clustering
create or replace function geotools.exec_tmp1(in_loc TEXT, kwh_month NUMERIC,min_tr_sz NUMERIC) returns void
LANGUAGE 'plpgsql'
AS
$$
DECLARE
  visited_targets BIGINT [] :=ARRAY [-1];
  dist DECIMAL;--150
  dist_incerment DECIMAL:=100;--50
  dist_max DECIMAL :=600;
  dbscan_max_distance DECIMAL :=600;


--  kwh_month NUMERIC:=18;--30
--  min_tr_sz NUMERIC:=10;--26


  max_tr_sz NUMERIC:=315;
  min_hh_cnt INTEGER ;
  max_hh_cnt INTEGER ;
  dbscan_return NUMERIC;
  max_dbscan_runs NUMERIC :=5;
  kmean_req NUMERIC;
  mean_shift_ctr INTEGER:=1000;
  lp_ctr INTEGER;


  in_rev TEXT;
  in_loc TEXT;--'NTUNGAMO','ISINGIRO','RAKAI','MASAKA'



--select geotools.Voronoi_bldg_cl (600,MASAKA, 26_kVA_30kWh);
--geotools.get_rev(); '26_kVA_30kWh'
  snap_grid_size NUMERIC :=50;
  debug_bigint BIGINT;
  kmean_debug_counter integer :=0;
  start_time TIMESTAMP;

  current_location_no integer;
  total_location_no integer;
BEGIN
--  SET CONSTRAINTS ALL DEFERRED;
  start_time := CURRENT_TIMESTAMP;
  total_location_no := 1;
 current_location_no := 1;

  in_rev := min_tr_sz||'_kVA_'||kwh_month ||'kWh';
  --in_rev := '2.5_kVA_18kWh_snapped';
  --time_diff := EXTRACT(EPOCH FROM (now() - start_time))/60;
  RAISE NOTICE 'Statring script with active revision % @ %', in_rev, now() ;

  min_hh_cnt:= geotools.get_cust_cnt(kwh_month, min_tr_sz*0.9*0.5);
  max_hh_cnt:= geotools.get_cust_cnt(kwh_month, max_tr_sz*0.9*0.8);
  RAISE NOTICE 'min_hh_cnt is % and max_hh_cnt is %',min_hh_cnt,max_hh_cnt;

    --  time_diff := EXTRACT(EPOCH FROM (now() - start_time))/60;
    RAISE NOTICE '**********Statring script with location %************ % of % at: %',in_loc, current_location_no, total_location_no, clock_timestamp();
    current_location_no := current_location_no+1;
    dist :=600;

    IF (SELECT count(*) FROM geotools.bldgs WHERE rev=in_rev)=0 THEN
        RAISE NOTICE 'Duplicating base rev into % revision',in_rev;
      INSERT INTO geotools.bldgs (loc,loc_list,proj,rev,hhr,cl_id,sts,adj_cnt,adj_cnt_dist,original_record,geom,snap_geom)
        SELECT loc,loc_list,proj,in_rev,hhr,cl_id,sts,adj_cnt,adj_cnt_dist,original_record,geom,snap_geom FROM geotools.bldgs WHERE rev='base';
    END IF;

    RAISE NOTICE 'Clearing all cl_id and setting them to NULL';
    PERFORM geotools.clear_cl_id(in_loc,in_rev);

    RAISE NOTICE 'Reverting clustered sts and setting them to cluster';
    PERFORM geotools.reset_sts_clustered(in_loc,in_rev);

    RAISE NOTICE 'Delete any existing bldg clusters';
    PERFORM geotools.reset_bldg_cl(in_loc, in_rev);

    WHILE dist <= dbscan_max_distance LOOP
      lp_ctr:=0;
      dbscan_return:=-1;
      WHILE dbscan_return <>0 and lp_ctr <= max_dbscan_runs LOOP
        RAISE NOTICE 'Starting DBSCAN run % clustering at dist % m and hh %', lp_ctr, dist, min_hh_cnt;
        dbscan_return:= geotools.dbscan(dist, min_hh_cnt, (dist+lp_ctr) :: TEXT, in_loc, in_rev);
              RAISE NOTICE 'DBSCAN clustered % rows', dbscan_return;
        kmean_req:=geotools.kmeanRequired(dist_max, max_hh_cnt,snap_grid_size, in_loc, in_rev);
        kmean_debug_counter := 0;
        WHILE kmean_req <> 0 LOOP
                dbscan_return:=-1;
          RAISE NOTICE 'Performing Kmean clustering on % clusters', kmean_req;
          PERFORM geotools.kmean(dist_max, min_hh_cnt, max_hh_cnt,snap_grid_size, in_loc, in_rev);
          kmean_req:=geotools.kmeanRequired(dist_max, max_hh_cnt, snap_grid_size, in_loc, in_rev);
          kmean_debug_counter := kmean_debug_counter+1;

--           if (kmean_debug_counter > 6 ) then return ;
--             end if;

        END LOOP;
                 RAISE NOTICE 'KMeans finished';

              if dbscan_return <>0 then
         debug_bigint:=geotools.clear_clid_outside_buffer( dist, snap_grid_size/2, in_loc, in_rev);
         RAISE NOTICE ' % bldgs had there cl_id cleared for being too far from centroid', debug_bigint;
              END IF;
              lp_ctr:= lp_ctr+1;
              --RAISE NOTICE 'lp_ctr %',lp_ctr;
      END LOOP;
      dist:=dist + dist_incerment;
      --RAISE NOTICE 'dist %',dist;
    END LOOP;

    RAISE NOTICE 'Delete any existing bldg clusters';
    PERFORM geotools.reset_bldg_cl(in_loc, in_rev);

    RAISE NOTICE 'Init bldg clusters';
    PERFORM geotools.init_bldg_cl(snap_grid_size,in_loc, in_rev);

    RAISE NOTICE 'Update adjacent count within %',dist_max;
    PERFORM geotools.update_adj_cnt (dist_max,in_loc,in_rev);

    RAISE NOTICE 'creating Oversized Vornoi cells';
    PERFORM geotools.Voronoi_bldg_cl (dist_max*2,in_loc, in_rev);



       RAISE NOTICE 'Updating bldgs in cl';
      PERFORM geotools.dense_update_bldg_cl (min_hh_cnt*0.1,in_loc, in_rev);

    RAISE NOTICE 'Find new centroid';
       mean_shift_ctr:=geotools.update_bldg_cl_centroid(snap_grid_size,in_loc, in_rev);
       RAISE NOTICE'Updated clusters centroid:%', mean_shift_ctr;

    mean_shift_ctr:=-1;
    lp_ctr:=0;
    WHILE mean_shift_ctr <>0 and lp_ctr<=1 LOOP
      RAISE NOTICE 'creating Vornoi cells';
      PERFORM geotools.Voronoi_bldg_cl (dist_max,in_loc, in_rev);

      RAISE NOTICE 'Updating bldgs in cl';
      PERFORM geotools.greedy_update_bldg_cl (in_loc, in_rev);

      RAISE NOTICE 'Find new centroid';
      mean_shift_ctr:=geotools.update_bldg_cl_centroid(snap_grid_size,in_loc, in_rev);

      debug_bigint:=geotools.clear_clid_outside_buffer( dist_max, snap_grid_size/2, in_loc, in_rev);
      RAISE NOTICE ' % bldgs had there cl_id cleared for being too far from centroid', debug_bigint;

      lp_ctr:=lp_ctr+1;
      RAISE NOTICE'Updated clusters centroid:%   Number of runs: %', mean_shift_ctr,lp_ctr;
    END LOOP;

    RAISE NOTICE 'Updating bldg_cl';
    PERFORM geotools.update_bldg_cl_fields(snap_grid_size,kwh_month,in_loc, in_rev);

     --time_diff := EXTRACT(EPOCH FROM (now() - start_time))/60;
     RAISE NOTICE 'Time passed % ',clock_timestamp()::TIMESTAMP- start_time::TIMESTAMP ;

END;
$$;

----------
--RAISE NOTICE ' func : %', geotools.explode_rds('MASAKA'); -- no need import explode the roads
-- lvrouting
create or replace function geotools.exec_tmp2(in_loc TEXT, kwh_month NUMERIC,min_tr_sz NUMERIC) returns void
LANGUAGE 'plpgsql'
AS
$$
DECLARE
i INTEGER:=1;
t1 TIMESTAMP:=clock_timestamp();
t2 TIMESTAMP:=clock_timestamp();
--in_loc TEXT ;
in_rev TEXT ;
start_time TIMESTAMP;
time_passed text;
  iter_count integer :=1;
BEGIN
  start_time := CURRENT_TIMESTAMP;
  RAISE NOTICE 'Script started at %', t1;
--    in_rev := '10_kVA_18kWh';

	in_rev := min_tr_sz||'_kVA_'||kwh_month ||'kWh';

    RAISE NOTICE '--------------------------%------------------------------',in_loc;

    RAISE NOTICE '1-snap_bldg_node_to_roads : %', geotools.snap_bldg_node_to_roads ( in_loc,in_rev,100);
    RAISE NOTICE '2-init_nodes : %', geotools.insert_pole_nodes ( in_loc,in_rev);

     RAISE NOTICE '3-insert_rd_to_rd_nodes : %', geotools.insert_rd_to_rd_nodes ( in_loc,in_rev);
     RAISE NOTICE '4-update_rd_nodes_cl : %', geotools.update_rd_nodes_cl ( in_loc,in_rev);

      RAISE NOTICE '5-snap_cegeom_to_roads: %', geotools.snap_cegeom_to_roads(in_loc,in_rev,200);
     RAISE NOTICE '6-update_tr_nodes: %', geotools.update_tr_nodes(in_loc,in_rev);

      RAISE NOTICE '7-insert_rd_to_rd_edges : %', geotools.insert_rd_to_rd_edges(in_loc,in_rev,1);

     RAISE NOTICE '8-insert_pole_to_tr_edges : %', geotools.insert_pole_to_tr_edges ( in_loc,in_rev,0);
     RAISE NOTICE '9-insert_pole_to_pole_edges : %', geotools.insert_pole_to_pole_edges ( in_loc,in_rev,2);
     RAISE NOTICE '10-insert_pole_to_rd_edges : %', geotools.insert_pole_to_rd_edges ( in_loc,in_rev,2);
     RAISE NOTICE '11-insert_tr_to_rd_in_cl_edges : %', geotools.insert_tr_to_rd_in_cl_edges ( in_loc,in_rev,2);
     RAISE NOTICE '12-insert_lv_new_lines_2 : %', geotools.insert_lv_new_lines_2 ( in_loc,in_rev);
  -- added by nzi
    --    commit;
     time_passed := clock_timestamp()::TIMESTAMP- start_time::TIMESTAMP ;
        iter_count := iter_count+1;
    RAISE NOTICE '% iteration completed , time passed: % ', iter_count, time_passed ;

  END ;
$$ ;
------------------------

--RAISE NOTICE ' func : %', geotools.explode_rds('MASAKA'); -- no need import explode the roads
-- mv routing
create or replace function geotools.exec_tmp3(in_loc TEXT, kwh_month NUMERIC,min_tr_sz NUMERIC) returns void
LANGUAGE 'plpgsql'
AS
$$
DECLARE
i INTEGER:=1;
t1 TIMESTAMP:=clock_timestamp();
t2 TIMESTAMP:=clock_timestamp();
in_loc TEXT;
in_rev TEXT:='10_kVA_18kWh';
start_time TIMESTAMP;
BEGIN
in_rev := min_tr_sz||'_kVA_'||kwh_month ||'kWh';
   start_time := CURRENT_TIMESTAMP;
  RAISE NOTICE 'Script started at %', t1;



   RAISE NOTICE '--------------------------%------------------------------',in_loc;

            t1 :=clock_timestamp();
     RAISE NOTICE '01 - insert_tr_to_rd_out_cl_edges : %', geotools.insert_tr_to_rd_out_cl_edges( 600, in_loc,in_rev,2);
             t2 :=clock_timestamp();
     RAISE NOTICE 'time ellapsed: %',t2-t1;

-- investigate why this creating long line

             t1 :=clock_timestamp();
    RAISE NOTICE '02 - insert_tr_to_tr_edges : %', geotools.insert_tr_to_tr_edges( 600, in_loc,in_rev,2);
             t2 :=clock_timestamp();
     RAISE NOTICE 'time ellapsed: %',t2-t1;


            t1 :=clock_timestamp();
    RAISE NOTICE '03 - insert_nodes_tr_to_lines : %', geotools.insert_nodes_tr_to_lines( in_loc,in_rev);
            t2 :=clock_timestamp();
    RAISE NOTICE 'time ellapsed: %',t2-t1;


            t1 :=clock_timestamp();
    RAISE NOTICE '04 - insert_tr_to_lines_edges : %', geotools.insert_tr_to_lines_edges( 600,in_loc,in_rev,2);
            t2 :=clock_timestamp();
    RAISE NOTICE 'time ellapsed: %',t2-t1;





            t1 :=clock_timestamp();
    RAISE NOTICE '05 - insert_rd_to_line_edges : %', geotools.insert_rd_to_line_edges( in_loc,in_rev,2);
            t2 :=clock_timestamp();
    RAISE NOTICE 'time ellapsed: %',t2-t1;


            t1 :=clock_timestamp();
    RAISE NOTICE '12 - update_rd_nodes_line : %', geotools.update_rd_nodes_line ( in_loc,in_rev);
            t2 :=clock_timestamp();
    RAISE NOTICE 'time ellapsed: %',t2-t1;
--

            t1 :=clock_timestamp();
    RAISE NOTICE '13 - insert_rd_line_to_common_edges : %', geotools.insert_rd_line_to_common_edges ( in_loc,in_rev);
               t2 :=clock_timestamp();
    RAISE NOTICE 'time ellapsed: %',t2-t1;


   t1 :=clock_timestamp();
    RAISE NOTICE '18 - insert_mv_new_lines_2: %', geotools.insert_mv_new_lines( in_loc,in_rev);
               t2 :=clock_timestamp();
    RAISE NOTICE 'time ellapsed: %',t2-t1;

    t1 :=clock_timestamp();
    RAISE NOTICE '19 - set_tr_size : %',  geotools.set_tr_size(ARRAY [315,200,100,50,25], 'kVA', in_loc,in_rev);
        t2 :=clock_timestamp();
    RAISE NOTICE 'time ellapsed: %',t2-t1;

    t1 :=clock_timestamp();
    RAISE NOTICE '20 - update_prj : %',  geotools.update_prj_nzi( in_loc,in_rev);
        t2 :=clock_timestamp();
    RAISE NOTICE 'time ellapsed: %',t2-t1;

  END ;
$$ ;

---------------

create or replace function geotools.exec_tmp4(kwh_month NUMERIC,min_tr_sz NUMERIC) returns void
LANGUAGE 'plpgsql'
AS
$$
DECLARE
in_loc TEXT;
record1 record;
record2 record;
BEGIN
for record1 in (
SELECT distinct
  name_id
FROM
  geotools.loc where loc_type='region'
) loop
perform exec_tmp0(record1.name_id, kwh_month,min_tr_sz );
end loop;
    FOR in_loc in (SELECT
distinct
  name_id
FROM
  geotools.loc where loc_type='district' and  name_id is not null)
loop
perform geotools.exec_tmp1(record1.name_id, kwh_month,min_tr_sz );
perform  geotools.exec_tmp2(record1.name_id, kwh_month,min_tr_sz );
perform  geotools.exec_tmp3(record1.name_id, kwh_month,min_tr_sz );
end loop;
END;
$$;
-----
create or replace function geotools.exec_algorithm() returns void
LANGUAGE 'plpgsql'
AS
$$

BEGIN
  perform  geotools.exec_tmp4(18,10) ;
END;
$$;
