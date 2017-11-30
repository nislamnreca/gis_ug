--RAISE NOTICE ' func : %', geotools.explode_rds('MASAKA'); -- no need import explode the roads
DO LANGUAGE 'plpgsql'
$$
DECLARE
i INTEGER:=1;
t1 TIMESTAMP:=clock_timestamp();
t2 TIMESTAMP:=clock_timestamp();
in_loc TEXT ;
in_rev TEXT ;
BEGIN
  RAISE NOTICE 'Script started at %', t1;
    in_rev := '10_kVA_18kWh';
  FOR in_loc in select name_id from geotools.loc WHERE  name_id_list&&ARRAY['rwenzori','western','south_western'] and loc_type='district'
 --   and name_id_list&&ARRAY['buhweju']--,'bundibugyo','bushenyi','mitooma','ntoroko','rubirizi','kisoro','kabale','kabarole','kanungu','kasese','rukungiri','sheema']
    and name_id_list&&ARRAY['buhweju','bundibugyo','bushenyi','mitooma','ntoroko','rubirizi','kisoro','kabale','kabarole','kanungu','kasese','rukungiri','sheema']
  --and name_id_list&&ARRAY['kabale']

  LOOP




    RAISE NOTICE '--------------------------%------------------------------',in_loc;

    RAISE NOTICE 'snap_bldg_node_to_roads : %', geotools.snap_bldg_node_to_roads ( in_loc,in_rev,100);
    RAISE NOTICE 'init_nodes : %', geotools.insert_pole_nodes ( in_loc,in_rev);

     RAISE NOTICE 'insert_rd_to_rd_nodes : %', geotools.insert_rd_to_rd_nodes ( in_loc,in_rev);
     RAISE NOTICE 'update_rd_nodes_cl : %', geotools.update_rd_nodes_cl ( in_loc,in_rev);

      RAISE NOTICE 'snap_cegeom_to_roads: %', geotools.snap_cegeom_to_roads(in_loc,in_rev,200);
     RAISE NOTICE 'update_tr_nodes: %', geotools.update_tr_nodes(in_loc,in_rev);

      RAISE NOTICE 'insert_rd_to_rd_edges : %', geotools.insert_rd_to_rd_edges(in_loc,in_rev,1);

     RAISE NOTICE 'insert_pole_to_tr_edges : %', geotools.insert_pole_to_tr_edges ( in_loc,in_rev,0);
     RAISE NOTICE 'insert_pole_to_pole_edges : %', geotools.insert_pole_to_pole_edges ( in_loc,in_rev,2);
     RAISE NOTICE 'insert_pole_to_rd_edges : %', geotools.insert_pole_to_rd_edges ( in_loc,in_rev,2);
     RAISE NOTICE 'insert_tr_to_rd_in_cl_edges : %', geotools.insert_tr_to_rd_in_cl_edges ( in_loc,in_rev,2);
     RAISE NOTICE 'insert_lv_new_lines_2 : %', geotools.insert_lv_new_lines_2 ( in_loc,in_rev);

END LOOP ;
  END ;
$$ ;
