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

select * from results_export.vw_line_summary;

with q10 as (
select distinct loc, proj from geotools.bldgs_cl)
select loc, count(*) project_count from q10 group by loc

select * from geotools.loc where loc_type='region';

--- 'north_eastern', 'mid_western','central_north'

-----------
--++++++++++++++ R&D for updating project from updating of MV
--inside function geotools.insert_mv_new_lines(in_loc TEXT, in_rev TEXT)

/*

  WITH q10 AS (
      SELECT (pgr_dijkstra(
          'SELECT objectid id, source, target, (cost_mult*cost)::BIGINT "cost" ' ||
          'FROM geotools.master_edge_table ' ,
          array_agg(n.objectid) ,
           0,
          FALSE)).*
      FROM geotools.nodes n
      WHERE   n.node_type = 'tr_pole'
            and n.objectid=12637231
  )
    , q20 AS (
      SELECT DISTINCT
        b.source,
        b.target,
        b.cl_id,
        b.dist,
        b."cost",
        'mv'   v_level,
        b.src_trgt_type,
        in_loc loc,
        in_rev rev,
        b.geom
      FROM q10 a
        JOIN geotools.master_edge_table b ON a.edge = b.objectid
  )
-- partition by startvid order by path, path_seq,  desc and grab the objectid of the edge as project no.
*/

  SELECT r2.objectid FROM geotools.roads r1
  JOIN geotools.roads r2 on r1.objectid<>r2.objectid and  st_contains(r1.geom,r2.geom)
    where r1.loc @> ARRAY ['north_eastern'] and r2.loc @> ARRAY ['north_eastern'];
-------------
begin
--geotools.insert_rd_to_rd_edges(in_loc TEXT, in_rev TEXT, cost_pow NUMERIC DEFAULT 1);
end;


select name_id from geotools.loc WHERE  name_id_list&&ARRAY['north_eastern', 'mid_western','central_north'] and loc_type='district'
    and name_id_list&&(with q10 as (
select distinct d.name_id from geotools.loc r join geotools.loc d on  st_intersects(d.geom,r.geom) and  st_area(st_intersection(d.geom,r.geom)) > st_area(d.geom)*.55 WHERE d.loc_type='district'
and r.loc_type='region' and r.name_id in ('north_eastern', 'mid_western','central_north') )
select array_agg(name_id) from q10);
