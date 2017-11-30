run.cmd exec_algorithm.sql

pgsql2shp -f export\cl_vornoy -h localhost -u postgres -P mnzryv uganda_gis "SELECT objectid, cl_id, loc, loc_list, proj, rev, b_cnt, hh_cnt, demand, dist, tr_size, v_level, phase, voltage, ver_geom geom FROM geotools.bldgs_cl;"

pgsql2shp -f export\cl_hull -h localhost -u postgres -P mnzryv uganda_gis "SELECT objectid, cl_id, loc, loc_list, proj, rev, b_cnt, hh_cnt, demand, dist, tr_size, v_level, phase, voltage, hull_geom geom FROM geotools.bldgs_cl;"

pgsql2shp -f export\transformers -h localhost -u postgres -P mnzryv uganda_gis "SELECT objectid, cl_id, loc, loc_list, proj, rev, b_cnt, hh_cnt, demand, dist, tr_size, v_level, phase, voltage, ce_geom geom FROM geotools.bldgs_cl;"

pgsql2shp -f export\lines -h localhost -u postgres -P mnzryv uganda_gis "SELECT * from  geotools.new_lines;"

pgsql2shp -f export\lines -h localhost -u postgres -P mnzryv uganda_gis "SELECT * from  geotools.new_lines;"

run.cmd export.sql