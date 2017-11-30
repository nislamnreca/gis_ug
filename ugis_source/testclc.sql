create or replace function geotools.getNearestPointByCluster( in_loc text, in_rev text)
  RETURNS TABLE (
        cl_id text,
        objectid text,
        ce_geom geometry
)
LANGUAGE plpgsql
AS $$
DECLARE
  u_ce INTEGER :=0;
  clidvar text ;

myrec0 record;
myrec1 record;
myrec2 record;
  cgeom geometry;
BEGIN
for myrec0 in (
        SELECT
       distinct b.cl_id
       FROM geotools.bldgs b
      WHERE b.loc = in_loc AND b.rev = in_rev and b.cl_id <> 'unk'
      GROUP BY b.cl_id
  ) LOOP

  clidvar := myrec0.cl_id;

FOR myrec1 IN (
    SELECT
       st_collect(b.snap_geom) col_geom
      FROM geotools.bldgs b
      WHERE b.loc = in_loc AND b.rev = in_rev and b.cl_id =clidvar
  ) LOOP

  cgeom := myrec1.col_geom;
  end loop;
for myrec2 in (
       SELECT
        b.cl_id,
        b.snap_geom ce_geom,
        b.objectid objectid
      FROM geotools.bldgs b where b.cl_id=clidvar
    order by st_distance(st_centroid (cgeom),b.snap_geom)
  limit 1
  ) LOOP
  cl_id := clidvar ;
  objectid := myrec2.objectid;
  ce_geom := myrec2.ce_geom;
  RETURN NEXT;


END LOOP;
END LOOP ;

END;
$$;
