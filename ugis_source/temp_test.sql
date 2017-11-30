select geotools.insert_rd_to_rd_edges('mbarara', '10_kVA_18kWh',1);

--CREATE EXTENSION "uuid-ossp";
drop table if exists  config.process_01H;

create table config.process_01H (
 pkey varchar PRIMARY KEY not null default uuid_generate_v1(),
  process_description varchar (255) not null unique
);

drop table if exists  config.process_02S;

create table config.process_02S (
 pkey varchar PRIMARY KEY not null default uuid_generate_v1(),
 fkey varchar ,
 sub_group_name varchar not null unique,
  process_description varchar (255) not null
);

drop table if exists  config.process_03D;

create table config.process_03D (
  fkey varchar,
  pkey varchar PRIMARY KEY not null default uuid_generate_v1(),
  process_sequence_no integer,
  process_notice varchar not null,
  process_call_sql varchar
);

drop table if exists  config.execution_params;

create table config.execution_params (
  pkey varchar PRIMARY KEY not null default uuid_generate_v1(),
  exec_param_description varchar not null unique,
  revision_text varchar,
  location_description varchar,
  loc_cursor_sql varchar
);
--------------

select * from geotools.loc WHERE  name_id_list&&ARRAY['north_eastern', 'mid_western','central_north'] and loc_type='district'
    and name_id_list&&(with q10 as (
select distinct d.name_id from geotools.loc r join geotools.loc d on  st_intersects(d.geom,r.geom) and  st_area(st_intersection(d.geom,r.geom)) > st_area(d.geom)*.55 WHERE d.loc_type='district'
and r.loc_type='region' and r.name_id in ('north_eastern', 'mid_western','central_north') )
select array_agg(name_id) from q10 );