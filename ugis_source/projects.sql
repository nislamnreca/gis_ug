
ALTER TABLE geotools.new_lines ADD eqp TEXT NULL;
ALTER TABLE geotools.bldgs_cl ADD eqp TEXT NULL;
create index ixcmp on geotools.new_lines(loc,rev,v_level,proj);
create index ixcmp on geotools.bldgs(loc,rev);


with q10 as ( -- this should not return any record of if mv routed correctly
    SELECT DISTINCT
      l.proj,
      l.cl_id
    FROM geotools.new_lines l
)
select a.* from q10 a join q10 b on a.cl_id=b.cl_id
                                    and a.proj <> b.proj
                                    where a.proj is not null and a.proj <> 'unk';

with q10 as ( -- this should not return any record of projects are assigned correctly
select n.cl_id, l.proj
from geotools.nodes n
  join geotools.new_lines l on st_intersects(l.geom, n.geom)
where  l.v_level='mv' and  n.node_type='tr_pole' )

select a.* from q10 a join q10 b on a.cl_id=b.cl_id
                                    and a.proj <> b.proj
                                    where a.proj is not null and a.proj <> 'unk';

with q10 as (
select n.cl_id, l.proj, l.phase
from geotools.nodes n
  join geotools.new_lines l on st_intersects(l.geom, n.geom)
where  l.v_level='mv' and  n.node_type='tr_pole' )
update  geotools.bldgs_cl cl set phase = q.phase
from q10 q where cl.cl_id = q.cl_id and cl.proj= q.proj;

select count(*) from geotools.bldgs_cl;-- where node_type='tr_pole' ;

with q10 as (
select n.cl_id, l.proj
from geotools.nodes n
  join geotools.new_lines l on st_dwithin(l.geom, n.geom,1)
where  l.v_level='mv' and  n.node_type='tr_pole' )
  update geotools.new_lines l set proj = q.proj
from q10 q
WHERE  l.v_level='lv' and l.cl_id = q.cl_id;

with q10 as (
select n.cl_id, l.proj
from geotools.nodes n
  join geotools.new_lines l on st_intersects(l.geom, n.geom)
where  l.v_level='mv' and  n.node_type='tr_pole' )
  update geotools.bldgs_cl cl set proj = q.proj
from q10 q
WHERE  cl.cl_id = q.cl_id;

with q10 as (
SELECT distinct l.proj, coalesce(el.volt,'33000') volt
FROM geotools.nodes n
  JOIN geotools.new_lines l ON st_dwithin(l.geom, n.geom, 1)
  join geotools.lines el on st_dwithin(l.geom, el.geom,1)
WHERE n.node_type IN ('rd_line', 'line')
      --AND n.loc = in_loc AND n.rev = in_rev AND l.loc = in_loc AND l.rev = in_rev
      AND l.target <> 0 AND l.source <> 0 AND l.v_level = 'mv'
  and l.proj is not null and l.proj <> 'unk')
update geotools.new_lines l set voltage = q.volt
from q10 q
where l.proj = q.proj and l.v_level = 'mv' ;

with q10 as (
select cl.proj, count(*) cnt from geotools.bldgs_cl cl
  GROUP BY cl.proj
)
update geotools.new_lines l set phase = case when q.cnt < 3 then 2 else 3 end,
  conductor = case when q.cnt < 3 then '25mm2' else '50mm2' end
from q10 q where l.proj = q.proj and l.v_level = 'mv';

with q10 as (
select cl.proj, count(*) cnt from geotools.bldgs_cl cl
  GROUP BY cl.proj
)
update geotools.new_lines l set phase = case when q.cnt < 3 then 2 else 3 end,  conductor='25mm2', voltage='400'
from q10 q where l.proj = q.proj and l.v_level = 'lv';

update geotools.new_lines set conductor='25mm2' where v_level='lv';

update geotools.bldgs_cl set phase = 3 where  (substring(tr_size from '\d+'  ))::NUMERIC  > 25 ;
--select substring('100 kVA' from '\d+'  ) ;

update geotools.new_lines set eqp = 'l_'||voltage::integer/1000||'kv_'||phase||'ph_'||v_level||'_al_'||conductor||'_b_wood' where v_level='mv';
update geotools.new_lines set eqp =   'l_'||voltage||'v_'||phase||'ph_'||v_level||'_al_'||conductor||'_abc_wood' where v_level='lv';

update geotools.bldgs_cl set eqp = 'tr_33kv_400v_'||phase||'ph_'|| regexp_replace(tr_size,' kVA', 'kva' );




select cl_id, voltage, phase, v_level, conductor,
  'l_'||voltage||'v_'||phase||'ph_'||v_level||'_al_'||conductor||'_b_wood' from geotools.new_lines where v_level='lv';


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

------------------output3-------
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
SELECT
  l.proj,
  tr.hh_cnt,
  tr.tr_cnt_by_proj,
  l.lv_length,
  l.mv_length,
  ceil(l.lcost + tr.trcost)               total_cost,
  ceil((l.lcost + tr.trcost) / tr.hh_cnt) cost_per_hh
FROM q25 l
  JOIN q45 tr ON l.proj = tr.proj;





--l_33kv_3ph_mv_al_50mm2_b_wood
--update geotools.new_lines set phase = 3 where v_level='mv' and phase is null;
--update geotools.new_lines set voltage = '33000' where v_level='mv' and new_lines.voltage is null;


-- tr_33kv_400v_3ph_315kva
  select DISTINCT  cl_id from geotools.new_lines where proj is null and v_level='lv';

  select cl.cl_id, cl.tr_size, phase, 'tr_33kv_400v_'||phase||'ph_'|| regexp_replace(tr_size,' kVA', 'kva' ) tr_size
    from geotools.bldgs_cl cl;
commit;

