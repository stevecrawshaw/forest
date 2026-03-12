duckdb 'data/forest.duckdb'

LOAD SPATIAL;

-- credentials are stored in a secret manager
-- ==============================VPN ON!! =============================

ATTACH '' AS weca_postgres (TYPE POSTGRES, SECRET weca_postgres);

-- # 11686 total ha of NFI woodland in LEP area - but this includes some outside the LEP boundary
-- SELECT area_ha.sum().round() hectares FROM weca_postgres.forestry_commission.national_forestry_inventory_woodland_lep;

-- weca_postgres.forestry_commission.national_forestry_inventory_woodland_lep IS of unknown provenance
-- so lets go to the source data and do the clipping ourselves to get an accurate area calculation
-- The geoJSON is downloaded from the Forestry Commission's Open Data Portal
--  Using a habd drawn polygon to minimise file size before clipping to the LEP boundary

-- Use a CTE to read in the data instead of a seprate table

-- CREATE OR REPLACE TABLE nfi AS
-- SELECT CATEGORY, IFT_IOA, OGC_FID,
-- geom.ST_FlipCoordinates().ST_Transform('EPSG:4326', 'EPSG:27700') AS shape 
-- FROM ST_Read('data/NFI_England_IFT_Data_20250826.geojson');

CREATE OR REPLACE TABLE lep_boundary_tbl AS
FROM weca_postgres.os.bdline_ua_lep_diss;

DESCRIBE lep_boundary_tbl;

-- Create a new table 'nfi_clipped' with the results
CREATE OR REPLACE TABLE nfi_clipped AS
SELECT 
    -- -- Calculate the clipped geometry (the intersection)
    ST_Intersection(lep.shape, nfi.shape) AS geom,
    -- -- Select all other columns from the nfi table
    nfi.* EXCLUDE (shape)
FROM
(SELECT CATEGORY, IFT_IOA, OGC_FID,
geom.ST_FlipCoordinates().ST_Transform('EPSG:4326', 'EPSG:27700') AS shape 
FROM ST_Read('data/NFI_England_IFT_Data_20250826.geojson')) AS nfi
JOIN
    lep_boundary_tbl AS lep ON ST_Intersects(nfi.shape, lep.shape);

FROM nfi_clipped LIMIT 5;


-- Verify the results
SET VARIABLE nfi_clipped_m2 = (SELECT (geom.ST_Area().sum()) AS area_hectares_boundary_enclosed FROM nfi_clipped);

SELECT getvariable('nfi_clipped_m2');

CREATE OR REPLACE TABLE tow_tbl AS
SELECT * EXCLUDE SHAPE,
  SHAPE.ST_ASWKB().ST_GeomFromWKB()
  AS geom
FROM ST_Read('data/FR_TOW_V1_South_West.gdb/'); 

SET VARIABLE tow_area_clipped_m2 =
(SELECT ST_Area(t.geom).sum() AS tow_area_m2
FROM tow_tbl t
JOIN lep_boundary_tbl l
  ON ST_Within(t.geom, l.shape));

SELECT getvariable('tow_area_clipped_m2');

SET VARIABLE total_tree_cover = (SELECT getvariable('tow_area_clipped_m2') + getvariable('nfi_clipped_m2'));

SELECT getvariable('total_tree_cover');

SET VARIABLE lep_total_area = (SELECT ST_Area(shape).sum() FROM lep_boundary_tbl);

SELECT getvariable('lep_total_area');

--  THE PERCENTAGE OF TREE COVER FROM NFI + TREES OUTSIDE WOODLAND IN THE LEP AREA
SET VARIABLE tree_cover_percentage = ((getvariable('total_tree_cover') / getvariable('lep_total_area')) * 100).round(2);

SELECT getvariable('tree_cover_percentage');
-- 15.11 %

-- ===================== COPY GEO FILES FOR INSPECTION ====================

COPY 
(SELECT t.*
FROM tow_tbl t
JOIN lep_boundary_tbl l
  ON ST_Within(t.geom, l.shape))
  TO 'data/tow_within_lep_boundary.fgb' WITH (FORMAT 'GDAL', DRIVER 'FlatGeobuf');

COPY nfi_clipped TO 'data/nfi_clipped.fgb' (FORMAT 'GDAL', DRIVER 'FlatGeoBuf');