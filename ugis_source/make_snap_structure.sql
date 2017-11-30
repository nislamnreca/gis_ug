      INSERT INTO geotools.bldgs (loc,loc_list,proj,rev,hhr,cl_id,sts,adj_cnt,adj_cnt_dist,original_record,geom,snap_geom)
        SELECT loc,loc_list,proj,'2.5_kVA_18kWh_snapped',hhr,cl_id,sts,adj_cnt,adj_cnt_dist,original_record,geom,snap_geom FROM geotools.bldgs WHERE rev='base';

      with q10 AS (
    SELECT
      b.objectid,
      CASE WHEN st_distance(b.geom, st_startpoint(r.geom)) < st_distance(b.geom, st_endpoint(r.geom))
        THEN st_startpoint(r.geom)
      ELSE st_endpoint(r.geom) END new_snap_geom,
      row_number()
      OVER (
        PARTITION BY b.objectid
        ORDER BY b.geom <-> r.geom ASC ) rnk
    FROM geotools.bldgs b
      JOIN geotools.roads r ON st_dwithin(b.geom, r.geom, 50)
    WHERE rev = '2.5_kVA_18kWh_snapped'
)
  UPDATE geotools.bldgs b SET snap_geom = new_snap_geom
from q10
where q10.objectid= b.objectid and rnk=1 and not st_equals(new_snap_geom,snap_geom);

