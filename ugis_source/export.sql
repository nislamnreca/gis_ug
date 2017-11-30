copy ( Select * From results_export.vw_proj_summary ) To 'G:\\gis\\uganda_data\\uganda_gis_projects\\export\\project_summary.csv' DELIMITER ',' CSV HEADER;
copy ( Select * From results_export.vw_line_summary ) To 'G:\\gis\\uganda_data\\uganda_gis_projects\\export\\line_summary.csv' DELIMITER ',' CSV HEADER;
copy ( Select * From results_export.vw_transformer_summary ) To 'G:\\gis\\uganda_data\\uganda_gis_projects\\export\\vw_transformer_summary.csv' DELIMITER ',' CSV HEADER;

copy ( Select * From config.equipment ) To 'G:\\gis\\uganda_data\\uganda_gis_projects\\export\\equipment.csv' DELIMITER ',' CSV HEADER;

--COPY products_273 TO '/tmp/products_199.csv' DELIMITER ',' CSV HEADER;
-----------
/*
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
--------------------
copy ( Select * From geotools.src_scripts ) To 'G:\\gis\\uganda_data\\uganda_gis_projects\\export\\src_scripts.csv' With CSV DELIMITER ',';
*/

