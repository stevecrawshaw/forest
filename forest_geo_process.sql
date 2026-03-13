duckdb 'data/forest.duckdb'
-- version 1.4.4
LOAD SPATIAL;

-- credentials are stored in a secret manager
-- ==============================VPN ON!! =============================

ATTACH '' AS weca_postgres (TYPE POSTGRES, SECRET weca_postgres);

-- weca_postgres.forestry_commission.national_forestry_inventory_woodland_lep IS of unknown provenance
-- so lets go to the source data and do the clipping ourselves to get an accurate area calculation
-- The geoJSON is downloaded from the Forestry Commission's Open Data Portal
--  Using a hand drawn polygon to minimise file size before clipping to the LEP boundary

-- ========================CREATE TABLES IN THE DATABASE =========================

CREATE OR REPLACE TABLE lep_boundary_tbl AS
FROM weca_postgres.os.bdline_ua_lep_diss;

DESCRIBE lep_boundary_tbl;

-- Create a new table 'nfi_clipped' with the results
CREATE OR REPLACE TABLE nfi_clipped AS
SELECT 
    -- -- Calculate the clipped geometry (the intersection)
    ST_Intersection(ST_GeomFromWKB(lep.shape), nfi.shape) AS geom_shape,
    -- -- Select all other columns from the nfi table
    nfi.* EXCLUDE (geom, shape, COUNTRY, Area_Ha, Shape__Length, Shape__Area),
    geom_shape.ST_Area() AS area_m2
FROM
(SELECT *,
geom.ST_FlipCoordinates().ST_Transform('EPSG:4326', 'EPSG:27700') AS shape 
FROM ST_Read('data/NFI_England_IFT_Data_20250826.geojson')) AS nfi
JOIN
    lep_boundary_tbl AS lep ON ST_Intersects(nfi.shape, ST_GeomFromWKB(lep.shape));

FROM nfi_clipped LIMIT 5;
DESCRIBE nfi_clipped;

-- Verify the results

CREATE OR REPLACE TABLE tow_in_lep_tbl AS
   WITH tow AS (
     SELECT * EXCLUDE SHAPE,
       SHAPE.ST_ASWKB().ST_GeomFromWKB() AS geom
     FROM ST_Read('data/FR_TOW_V1_South_West.gdb/')
   )
   SELECT tow.*, tow.geom.ST_Area() AS tow_area_calc_m2
   FROM tow
   JOIN lep_boundary_tbl l
     ON ST_Within(tow.geom, ST_GeomFromWKB(l.shape)); 

--  =============================== CALCULATE AREAS AND PERCENTAGES =============================

SET VARIABLE nfi_clipped_m2 = (SELECT (geom_shape.ST_Area().sum()) AS area_metres_boundary_enclosed FROM nfi_clipped);

SELECT getvariable('nfi_clipped_m2');

SET VARIABLE tow_area_clipped_m2 =
(SELECT tow_in_lep_tbl.tow_area_calc_m2.sum() AS tow_area_m2
FROM tow_in_lep_tbl);

SELECT getvariable('tow_area_clipped_m2');

SET VARIABLE total_tree_cover = (SELECT getvariable('tow_area_clipped_m2') + getvariable('nfi_clipped_m2'));

SELECT getvariable('total_tree_cover');

SET VARIABLE lep_total_area = (SELECT ST_Area(shape.ST_GeomFromWKB()).sum() FROM lep_boundary_tbl);

SELECT getvariable('lep_total_area');

--  THE PERCENTAGE OF TREE COVER FROM NFI + TREES OUTSIDE WOODLAND IN THE LEP AREA
SET VARIABLE tree_cover_percentage = ((getvariable('total_tree_cover') / getvariable('lep_total_area')) * 100).round(2);

SELECT getvariable('tree_cover_percentage');
-- 15.11 %
-- ===================================TABLE AND COLUMN COMMENTS ==============================

COMMENT ON TABLE nfi_clipped IS 'This table contains the clipped geometries of the NFI data within the LEP boundary, along with all original attributes from the NFI dataset and a new column for the area of each clipped geometry in square meters.';
COMMENT ON COLUMN nfi_clipped.geom_shape IS 'The geometry of the clipped area resulting from the intersection of the NFI data and the LEP boundary.';
COMMENT ON COLUMN nfi_clipped.area_m2 IS 'The area of the clipped geometry in square meters, calculated using the ST_Area function on the geom_shape column.';
COMMENT ON COLUMN nfi_clipped.CATEGORY IS 'The category of the area as defined in the original NFI dataset, indicating the type of land cover or use.';
COMMENT ON COLUMN nfi_clipped.IFT_IOA IS 'The type of land cover or use as defined in the original NFI dataset, providing additional information about the area.';

COMMENT ON TABLE tow_in_lep_tbl IS 'This table contains the geometries of Trees Outside Woodland (TOW) that are located within the LEP boundary, along with all original attributes from the TOW dataset and a new column for the area of each TOW geometry in square meters.';
COMMENT ON COLUMN tow_in_lep_tbl.TOW_ID IS 'The unique identifier for each TOW area as defined in the original TOW dataset.';
COMMENT ON COLUMN tow_in_lep_tbl.Woodland_Type IS 'The category of the area as defined in the original TOW dataset, indicating the type of forested area.';
COMMENT ON COLUMN tow_in_lep_tbl.MEANHT IS 'The mean height of the TOW area as defined in the original TOW dataset, providing information about the average height of the trees in the area.';
COMMENT ON COLUMN tow_in_lep_tbl.MINHT IS 'The minimum height of the TOW area as defined in the original TOW dataset, providing information about the shortest trees in the area.';
COMMENT ON COLUMN tow_in_lep_tbl.MAXHT IS 'The maximum height of the TOW area as defined in the original TOW dataset, providing information about the tallest trees in the area.';
COMMENT ON COLUMN tow_in_lep_tbl.STDVHT IS 'The standard deviation of the height of the TOW area as defined in the original TOW dataset, providing information about the variability in tree height within the area.';
COMMENT ON COLUMN tow_in_lep_tbl.KM1_Tile IS 'The identifier for the 1km tile in which the TOW area is located, as defined in the original TOW dataset, providing information about the spatial location of the area within the dataset.';
COMMENT ON COLUMN tow_in_lep_tbl.KM10_Tile IS 'The identifier for the 10km tile in which the TOW area is located, as defined in the original TOW dataset, providing information about the spatial location of the area within the dataset.';
COMMENT ON COLUMN tow_in_lep_tbl.TOW_AREA_M IS 'The area of the TOW geometry in metres squared as defined in the original TOW dataset';
COMMENT ON COLUMN tow_in_lep_tbl.LiDAR_Survey_Year IS 'The year in which the LiDAR survey was conducted for the TOW area, as defined in the original TOW dataset, providing information about the temporal context of the data.';
COMMENT ON COLUMN tow_in_lep_tbl.geom IS 'The geometry of the TOW area resulting from the intersection of the TOW data and the LEP boundary. Projection British National Grid EPSG:27700';
COMMENT ON COLUMN tow_in_lep_tbl.tow_area_calc_m2 IS 'The area of the TOW geometry in square meters, calculated using the ST_Area function on the geom column.';

-- ===================== COPY GEO FILES FOR INSPECTION ====================

COPY 
tow_in_lep_tbl
TO 'data/tow_within_lep_boundary.geojson'
WITH (FORMAT 'GDAL', DRIVER 'GeoJSON');

COPY 
(SELECT * EXCLUDE (geom_shape) ,
nfi_clipped.geom_shape.ST_Transform('EPSG:27700', 'EPSG:4326').ST_FlipCoordinates() AS geo_shape_2d
FROM nfi_clipped)
TO 'data/nfi_clipped.geojson' (FORMAT 'GDAL', DRIVER 'GeoJSON');
