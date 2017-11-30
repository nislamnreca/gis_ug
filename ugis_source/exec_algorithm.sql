DO LANGUAGE 'plpgsql'
$$
BEGIN
  perform  geotools.exec_algorithm() ;
END;
$$;
