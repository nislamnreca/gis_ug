DO LANGUAGE 'plpgsql'
$$
DECLARE
  visited_targets BIGINT [] :=ARRAY [-1];
  dist DECIMAL;--150
  dist_incerment DECIMAL:=100;--50
  dist_max DECIMAL :=600;
  dbscan_max_distance DECIMAL :=600;
  kwh_month NUMERIC:=18;--30
  min_tr_sz NUMERIC:=10;--26
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
  total_location_no := 13;
 current_location_no := 1;

  in_rev := min_tr_sz||'_kVA_'||kwh_month ||'kWh';
  --in_rev := '2.5_kVA_18kWh_snapped';
  --time_diff := EXTRACT(EPOCH FROM (now() - start_time))/60;
  RAISE NOTICE 'Statring script with active revision % @ %', in_rev, now() ;

  min_hh_cnt:= geotools.get_cust_cnt(kwh_month, min_tr_sz*0.9*0.5);
  max_hh_cnt:= geotools.get_cust_cnt(kwh_month, max_tr_sz*0.9*0.8);
  RAISE NOTICE 'min_hh_cnt is % and max_hh_cnt is %',min_hh_cnt,max_hh_cnt;
--####################################################################################################
  FOR in_loc in select name_id from geotools.loc WHERE  name_id_list&&ARRAY['rwenzori','western','south_western'] and loc_type='district'
 --   and name_id_list&&ARRAY['buhweju']--,'bundibugyo','bushenyi','mitooma','ntoroko','rubirizi','kisoro','kabale','kabarole','kanungu','kasese','rukungiri','sheema']
    and name_id_list&&ARRAY['buhweju','bundibugyo','bushenyi','mitooma','ntoroko','rubirizi','kisoro','kabale','kabarole','kanungu','kasese','rukungiri','sheema']
  --and name_id_list&&ARRAY['kabale']

  LOOP
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



  --   EXCEPTION
  --   WHEN OTHERS THEN
  --     RAISE WARNING 'Loading of record % failed: %', r.objectid, SQLERRM;
    END loop;
END;
$$;