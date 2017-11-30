--RAISE NOTICE ' func : %', geotools.explode_rds('MASAKA'); -- no need import explode the roads
DO LANGUAGE 'plpgsql'
$$
DECLARE
i INTEGER:=1;
t1 TIMESTAMP:=clock_timestamp();
t2 TIMESTAMP:=clock_timestamp();
in_loc TEXT ;
in_rev TEXT ;
start_time TIMESTAMP;
time_passed text;
  iter_count integer :=1;
BEGIN
  start_time := CURRENT_TIMESTAMP;
  RAISE NOTICE 'Script started at %', t1;
    in_rev := '10_kVA_18kWh';
  /*
  FOR in_loc in select name_id from geotools.loc WHERE  name_id_list&&ARRAY['rwenzori','western','south_western'] and loc_type='district'
    and name_id_list&&ARRAY['buhweju','bundibugyo','bushenyi','mitooma','ntoroko','rubirizi','kisoro','kabale','kabarole','kanungu','kasese','rukungiri','sheema']
*/
      FOR in_loc in select name_id from geotools.loc WHERE  name_id_list&&ARRAY['north_eastern', 'mid_western','central_north'] and loc_type='district'
    and name_id_list&&(with q10 as (
select distinct d.name_id from geotools.loc r join geotools.loc d on  st_intersects(d.geom,r.geom) and  st_area(st_intersection(d.geom,r.geom)) > st_area(d.geom)*.55 WHERE d.loc_type='district'
and r.loc_type='region' and r.name_id in ('north_eastern', 'mid_western','central_north') )
select array_agg(name_id) from q10)

  LOOP




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

END LOOP ;
  END ;
$$ ;
