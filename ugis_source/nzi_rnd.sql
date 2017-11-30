--create index ixcmp on geotools.new_lines(loc,rev,v_level,proj);

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

--copy ( Select * From results_export.vw_proj_summary ) To 'G:\\uganda_data\\uganda_gis_projects\\export\\test.csv' With CSV DELIMITER ',';


--*************************************************************************************
CREATE OR REPLACE FUNCTION geotools.update_prj_nzi_1(in_loc TEXT, in_rev TEXT)
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

   update geotools.new_lines n set proj = 'unk'
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
          WHERE (a.loc = in_loc AND a.rev = in_rev and a.v_level = 'mv'
                )  AND (ARRAY [a.source, a.target] &&    array_cmp) and coalesce(a.proj,'none') <> myrec.objectid :: TEXT    ))
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

--*************************************************************************************

--*************************************************************************************
CREATE OR REPLACE FUNCTION geotools.update_prj_nzi_2(in_loc TEXT, in_rev TEXT)
  RETURNS BIGINT AS
$$
DECLARE
  rt_val          BIGINT :=0;
  cnt_update      BIGINT :=1;
  objectid_ar     BIGINT [];
  objectid_ar_tmp BIGINT [];
  myrec           RECORD;
  myrec1          RECORD;
  myrec2          RECORD;
  rec_line        geotools.NEW_LINES;
  rec_line2       geotools.NEW_LINES;
  objectid_ar2    BIGINT [];
BEGIN
  FOR rec_line IN (SELECT l.*
                   FROM geotools.nodes n
                     JOIN geotools.new_lines l ON st_dwithin(l.geom, n.geom, 1)
                   WHERE n.node_type IN ('rd_line', 'line')
                         AND n.loc = in_loc AND n.rev = in_rev AND l.loc = in_loc AND l.rev = in_rev
                         AND l.target <> 0 AND l.source <> 0 AND l.v_level = 'mv')
  LOOP
    RAISE NOTICE 'Current object id : %', rec_line.objectid;

    UPDATE geotools.new_lines l
    SET proj = rec_line.objectid
    WHERE objectid = rec_line.objectid;


    objectid_ar := ARRAY [rec_line.objectid];
    objectid_ar2 := ARRAY [rec_line.source, rec_line.target];

    cnt_update := 1;

    WHILE (cnt_update > 0) LOOP


      WITH q10 AS (
          SELECT *
          FROM geotools.new_lines l
          WHERE ARRAY [l.source, l.target] && objectid_ar2
                AND NOT objectid_ar && ARRAY [l.objectid] AND l.v_level = 'mv' AND
                l.loc = in_loc AND l.rev = in_rev AND l.target <> 0 AND l.source <> 0 AND l.v_level = 'mv'
                AND proj <> rec_line.objectid :: TEXT
      )

        , q_update AS (
        UPDATE geotools.new_lines l
        SET proj = rec_line.objectid :: TEXT
        FROM q10 q
        WHERE l.objectid = q.objectid

      )
      SELECT
        array_agg(objectid)                        l_objectid,
        array_agg(l.source) || array_agg(l.target) n_objectid
      INTO myrec
      FROM q10 l;
      cnt_update := array_length(myrec.l_objectid, 1);
      objectid_ar :=  objectid_ar || myrec.l_objectid;
      objectid_ar2 := myrec.n_objectid;
      RAISE NOTICE '% record updated for project % in location %', cnt_update, rec_line.objectid, in_loc;

    END LOOP;

  END LOOP;

  rt_val :=  array_length(objectid_ar, 1);
  RETURN rt_val;
END;

$$ LANGUAGE 'plpgsql';
--*************************************************************************************