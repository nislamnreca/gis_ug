with qr1 as (
SELECT
                  l.*
                FROM geotools.nodes n
                  JOIN geotools.new_lines l ON st_dwithin(l.geom, n.geom, 1)
                WHERE n.node_type IN ('tr_pole')
                      AND n.loc = 'buhweju' AND n.rev = '10_kVA_18kWh' AND l.loc = 'buhweju' AND l.rev = '10_kVA_18kWh'
                      AND l.target <> 0 AND l.source <> 0 AND l.v_level = 'mv')
  select distinct cl_id, proj from qr1 a where proj is not null;

SELECT
                  l.*
                FROM geotools.nodes n
                  JOIN geotools.new_lines l ON st_dwithin(l.geom, n.geom, 1)
                WHERE n.node_type IN ('tr_pole')
                      AND n.loc = 'buhweju' AND n.rev = '10_kVA_18kWh' AND l.loc = 'buhweju' AND l.rev = '10_kVA_18kWh'
                      AND l.target <> 0 AND l.source <> 0 AND l.v_level = 'lv';

with qr1 as (
    SELECT *
    FROM geotools.new_lines l
    WHERE proj IS NOT NULL AND coalesce(cl_id, 'unk') <> 'unk' AND l.loc = 'buhweju' AND l.rev = '10_kVA_18kWh'
  and v_level='mv'
), qr2 as (
select cl_id, proj, count(*) cnt from qr1 group by cl_id, proj order by 1,3,2),
  qr3 as (
select distinct cl_id, proj from qr2)
  select cl_id, count(*) from qr2 group by cl_id having count(*) > 1;

 SELECT *
    FROM geotools.new_lines l
    WHERE proj IS NOT NULL AND coalesce(cl_id, 'unk') <> 'unk' AND l.loc = 'buhweju' AND l.rev = '10_kVA_18kWh'
and cl_id = 'buhweju-10_kVA_18kWh-600-0-5-0-1' order by proj;

with RECURSIVE q10 (prj,objectid,ar_objectid,source,target) as (
  SELECT
    l.objectid         prj,
    l.objectid         objectid,
    ARRAY [l.objectid] ar_objectid,
    source,
    target
  FROM geotools.new_lines l
  WHERE l.objectid = 11454925
  UNION ALL
  (
    SELECT
      prj,
      nl.objectid                        objectid,
      ar_objectid || ARRAY [nl.objectid] ar_object,
      nl.source,
      nl.target
    FROM geotools.new_lines nl
      JOIN q10 r ON   (r.source IN (nl.source, nl.target) OR r.target IN (nl.source, nl.target))
                    AND (NOT ar_objectid && ARRAY [nl.objectid]) AND nl.target <> 0 AND nl.source <> 0
     WHERE nl.loc = 'buhweju' AND nl.rev = '10_kVA_18kWh' AND nl.v_level = 'mv'

        order BY   ar_objectid || ARRAY [nl.objectid]
  )

)
  select count(*) from q10 ;




SELECT
                  l.*,
                  ARRAY [l.objectid]         ar_objectid,
                  ARRAY [l.source, l.target] src_tgt
                FROM geotools.nodes n
                  JOIN geotools.new_lines l ON st_dwithin(l.geom, n.geom, 1)
                WHERE n.node_type IN ('rd_line', 'line')
                      AND n.loc = 'buhweju' AND n.rev = '10_kVA_18kWh' AND l.loc = 'buhweju' AND l.rev = '10_kVA_18kWh'
                      AND l.target <> 0 AND l.source <> 0 AND l.v_level = 'mv'
                      --and l.objectid = 11454925::BIGINT


--================================ SQL Nov 08, 2017=================
