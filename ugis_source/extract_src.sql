drop table if exists geotools.src_scripts ;
create table geotools.src_scripts as
SELECT

    n.nspname AS schema
      ,proname AS fname
      ,proargnames AS args
      ,t.typname AS return_type
      ,d.description
      ,pg_get_functiondef(p.oid) as definition
  FROM pg_proc p
  JOIN pg_type t
    ON p.prorettype = t.oid
  LEFT OUTER
  JOIN pg_description d
    ON p.oid = d.objoid
  LEFT OUTER
  JOIN pg_namespace n
    ON n.oid = p.pronamespace
 WHERE n.nspname~'geotools';

select * from geotools.src_scripts;