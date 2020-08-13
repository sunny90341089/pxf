-- @description query04 for JDBC query with superset of columns in different order
SELECT "n@m2", "t", "num 1" FROM pxf_jdbc_superset_of_fields ORDER BY "t";

-- pxf_jdbc_superset_of_fields table is a superset of the source table
-- this will error out
-- start_matchsubs
--
-- # create a match/subs
--
-- m/\/gpdb\/v\d+\//
-- s/v\d+/SOME_VERSION/
--
-- m/file:.*;/
-- s/file:.*; lineNumber: \d+; columnNumber: \d+;/SOME_ERROR_LOCATION/g
--
-- m/Exception report.*/
-- s/report.*/SOME_EXCEPTION/
--
-- m/DETAIL/
-- s/DETAIL/CONTEXT/
--
-- m/pxf:\/\/(.*)\/pxf_automation_data/
-- s/pxf:\/\/.*\/pxf_automation_data/pxf:\/\/pxf_automation_data/
--
-- m/CONTEXT:.*line.*/
-- s/line \d* of //g
--
-- end_matchsubs
SELECT * FROM pxf_jdbc_superset_of_fields ORDER BY "t";
