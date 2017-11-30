@echo off
chcp 1252 >nul
echo [91m------------------------Script Starting--------------------------[0m
echo \timing \\ \i %1 | "C:\Program Files\PostgreSQL\9.6\bin\psql.exe" postgresql://postgres:mnzryv@localhost:5432/uganda_gis
echo [95m------------------------Script Ended-----------------------------[0m
