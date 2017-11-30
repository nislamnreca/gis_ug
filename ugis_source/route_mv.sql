--RAISE NOTICE ' func : %', geotools.explode_rds('MASAKA'); -- no need import explode the roads
DO LANGUAGE 'plpgsql'
$$
DECLARE
i INTEGER:=1;
t1 TIMESTAMP:=clock_timestamp();
t2 TIMESTAMP:=clock_timestamp();
in_loc TEXT;
in_rev TEXT:='10_kVA_18kWh';
start_time TIMESTAMP;
BEGIN
   start_time := CURRENT_TIMESTAMP;
  RAISE NOTICE 'Script started at %', t1;

  FOR in_loc in select name_id from geotools.loc WHERE  name_id_list&&ARRAY['rwenzori','western','south_western'] and loc_type='district'
 --   and name_id_list&&ARRAY['buhweju']--,'bundibugyo','bushenyi','mitooma','ntoroko','rubirizi','kisoro','kabale','kabarole','kanungu','kasese','rukungiri','sheema']
    and name_id_list&&ARRAY['buhweju','bundibugyo','bushenyi','mitooma','ntoroko','rubirizi','kisoro','kabale','kabarole','kanungu','kasese','rukungiri','sheema']
  --and name_id_list&&ARRAY['kabale']

  LOOP

   RAISE NOTICE '--------------------------%------------------------------',in_loc;

--             t1 :=clock_timestamp();
--     RAISE NOTICE '01 - insert_tr_to_rd_out_cl_edges : %', geotools.insert_tr_to_rd_out_cl_edges( 600, in_loc,in_rev,2);
--             t2 :=clock_timestamp();
--     RAISE NOTICE 'time ellapsed: %',t2-t1;

-- investigate why this creating long line
/*
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
*/




/*            t1 :=clock_timestamp();
    RAISE NOTICE '05 - insert_rd_to_line_edges : %', geotools.insert_rd_to_line_edges( in_loc,in_rev,2);
            t2 :=clock_timestamp();
    RAISE NOTICE 'time ellapsed: %',t2-t1;*/


/*            t1 :=clock_timestamp();
    RAISE NOTICE '12 - update_rd_nodes_line : %', geotools.update_rd_nodes_line ( in_loc,in_rev);
            t2 :=clock_timestamp();
    RAISE NOTICE 'time ellapsed: %',t2-t1;*/
--
/*
            t1 :=clock_timestamp();
    RAISE NOTICE '13 - insert_rd_line_to_common_edges : %', geotools.insert_rd_line_to_common_edges ( in_loc,in_rev);
               t2 :=clock_timestamp();
    RAISE NOTICE 'time ellapsed: %',t2-t1;
*/

/*    t1 :=clock_timestamp();
    RAISE NOTICE '18 - insert_mv_new_lines_2: %', geotools.insert_mv_new_lines( in_loc,in_rev);
               t2 :=clock_timestamp();
    RAISE NOTICE 'time ellapsed: %',t2-t1;*/
--
--     t1 :=clock_timestamp();
--     RAISE NOTICE '19 - set_tr_size : %',  geotools.set_tr_size(ARRAY [315,200,100,50,25], 'kVA', in_loc,in_rev);
--         t2 :=clock_timestamp();
--     RAISE NOTICE 'time ellapsed: %',t2-t1;

    t1 :=clock_timestamp();
    RAISE NOTICE '20 - update_prj : %',  geotools.update_prj_nzi( in_loc,in_rev);
        t2 :=clock_timestamp();
    RAISE NOTICE 'time ellapsed: %',t2-t1;


END LOOP ;
  END ;
$$ ;

