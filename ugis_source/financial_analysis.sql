--CREATE SCHEMA fa AUTHORIZATION postgres;
drop table if exists fa.item_category ;
create table fa.item_category (
cost_category_id text not null primary key,
cat_label  text ,
cat_remarks text
);

drop table if exists fa.cost_items ;
create table fa.cost_items (
cost_category_id text,
cost_item_id text not null primary key,
m_unit TEXT,
unit_cost text,
cost_item_label text );

drop table if exists fa.calc_params ;
create table fa.calc_params (
param_id text not null primary key,
hh_growth_year1 numeric,
init_working_cap_pct numeric,
pre_op_exp_pct numeric,
hh_onward numeric,
power_pur_per_kwh numeric,
o_and_m_per_kwh numeric,
other_per_kwh numeric,
annual_growth_pct numeric,
depr_dinfracture numeric,
depr_ndinfracture numeric,
init_cap_cost_grant_pct numeric,
dsl_interest_pct numeric,
dsl_duration_years numeric,
collection_ration numeric,
discount_pct numeric


);

drop table if exists fa.hh_tariffs;
create table fa.hh_tariffs (
tariff_code text not null primary key,
  init_penetration_pct numeric,
  kwh_permonth numeric,

tariff_label text );

drop table if exists fa.hh_tariffs_penetration;
create table fa.hh_tariffs_penetration (
tariff_code text not null,
from_year integer,
  to_year integer,
growth_pct numeric,
tariff_label text );


drop table if exists fa.hh_tariffs_uedcl;
create table fa.hh_tariffs_uedcl (
tariff_code text not null,
fc_per_month numeric,
cc_per_acc numeric,
ec_per_kwh numeric );