-- =============================================================================
-- Test script: Compare original vs optimized patient_status_arv procedure
--
-- This script:
--   1. Saves current state of affected tables
--   2. Runs the original procedure
--   3. Captures results
--   4. Restores state
--   5. Runs the optimized procedure
--   6. Compares results and reports differences
--
-- Run this during a maintenance window or on a test database.
-- =============================================================================

use isanteplus;

-- Step 1: Create backup tables to save current state
DROP TABLE IF EXISTS _test_patient_status_arv_backup;
CREATE TABLE _test_patient_status_arv_backup AS SELECT * FROM patient_status_arv;

DROP TABLE IF EXISTS _test_exposed_infants_backup;
CREATE TABLE _test_exposed_infants_backup AS SELECT * FROM exposed_infants;

DROP TABLE IF EXISTS _test_patient_backup;
CREATE TABLE _test_patient_backup AS SELECT patient_id, arv_status FROM patient;

DROP TABLE IF EXISTS _test_patient_on_arv_backup;
CREATE TABLE _test_patient_on_arv_backup AS SELECT * FROM patient_on_arv;

-- Step 2: Run the ORIGINAL procedure
CALL patient_status_arv();

-- Step 3: Capture results from original
DROP TABLE IF EXISTS _test_original_patient_status_arv;
CREATE TABLE _test_original_patient_status_arv AS SELECT * FROM patient_status_arv;

DROP TABLE IF EXISTS _test_original_exposed_infants;
CREATE TABLE _test_original_exposed_infants AS SELECT * FROM exposed_infants;

DROP TABLE IF EXISTS _test_original_patient_arv_status;
CREATE TABLE _test_original_patient_arv_status AS
    SELECT patient_id, arv_status FROM patient WHERE arv_status IS NOT NULL;

-- Step 4: Restore state before running optimized version
TRUNCATE TABLE patient_status_arv;
INSERT INTO patient_status_arv SELECT * FROM _test_patient_status_arv_backup;

TRUNCATE TABLE exposed_infants;
INSERT INTO exposed_infants SELECT * FROM _test_exposed_infants_backup;

UPDATE patient p
INNER JOIN _test_patient_backup b ON p.patient_id = b.patient_id
SET p.arv_status = b.arv_status;

TRUNCATE TABLE patient_on_arv;
INSERT INTO patient_on_arv SELECT * FROM _test_patient_on_arv_backup;

-- Step 5: Run the OPTIMIZED procedure (must be loaded with a different name first)
-- See below for how to load it
CALL patient_status_arv_optimized();

-- Step 6: Capture results from optimized
DROP TABLE IF EXISTS _test_optimized_patient_status_arv;
CREATE TABLE _test_optimized_patient_status_arv AS SELECT * FROM patient_status_arv;

DROP TABLE IF EXISTS _test_optimized_exposed_infants;
CREATE TABLE _test_optimized_exposed_infants AS SELECT * FROM exposed_infants;

DROP TABLE IF EXISTS _test_optimized_patient_arv_status;
CREATE TABLE _test_optimized_patient_arv_status AS
    SELECT patient_id, arv_status FROM patient WHERE arv_status IS NOT NULL;

-- =============================================================================
-- COMPARISON QUERIES
-- =============================================================================

-- Compare patient_status_arv: rows in original but not in optimized
SELECT 'patient_status_arv: in ORIGINAL only' AS comparison;
SELECT o.patient_id, o.id_status, o.start_date
FROM _test_original_patient_status_arv o
LEFT JOIN _test_optimized_patient_status_arv n
    ON o.patient_id = n.patient_id
   AND o.id_status = n.id_status
   AND o.start_date = n.start_date
WHERE n.patient_id IS NULL
LIMIT 100;

-- Compare patient_status_arv: rows in optimized but not in original
SELECT 'patient_status_arv: in OPTIMIZED only' AS comparison;
SELECT n.patient_id, n.id_status, n.start_date
FROM _test_optimized_patient_status_arv n
LEFT JOIN _test_original_patient_status_arv o
    ON n.patient_id = o.patient_id
   AND n.id_status = o.id_status
   AND n.start_date = o.start_date
WHERE o.patient_id IS NULL
LIMIT 100;

-- Compare exposed_infants: rows in original but not in optimized
SELECT 'exposed_infants: in ORIGINAL only' AS comparison;
SELECT o.patient_id, o.condition_exposee, o.visit_date
FROM _test_original_exposed_infants o
LEFT JOIN _test_optimized_exposed_infants n
    ON o.patient_id = n.patient_id
   AND o.condition_exposee = n.condition_exposee
WHERE n.patient_id IS NULL
LIMIT 100;

-- Compare exposed_infants: rows in optimized but not in original
SELECT 'exposed_infants: in OPTIMIZED only' AS comparison;
SELECT n.patient_id, n.condition_exposee, n.visit_date
FROM _test_optimized_exposed_infants n
LEFT JOIN _test_original_exposed_infants o
    ON n.patient_id = o.patient_id
   AND n.condition_exposee = o.condition_exposee
WHERE o.patient_id IS NULL
LIMIT 100;

-- Compare patient.arv_status differences
SELECT 'patient.arv_status: DIFFERENCES' AS comparison;
SELECT
    COALESCE(o.patient_id, n.patient_id) AS patient_id,
    o.arv_status AS original_status,
    n.arv_status AS optimized_status
FROM _test_original_patient_arv_status o
FULL OUTER JOIN _test_optimized_patient_arv_status n ON o.patient_id = n.patient_id
WHERE o.arv_status <> n.arv_status
   OR o.patient_id IS NULL
   OR n.patient_id IS NULL
LIMIT 100;

-- Summary counts
SELECT 'SUMMARY COUNTS' AS comparison;
SELECT
    'patient_status_arv' AS table_name,
    (SELECT COUNT(*) FROM _test_original_patient_status_arv) AS original_count,
    (SELECT COUNT(*) FROM _test_optimized_patient_status_arv) AS optimized_count
UNION ALL
SELECT
    'exposed_infants',
    (SELECT COUNT(*) FROM _test_original_exposed_infants),
    (SELECT COUNT(*) FROM _test_optimized_exposed_infants)
UNION ALL
SELECT
    'patient (with arv_status)',
    (SELECT COUNT(*) FROM _test_original_patient_arv_status),
    (SELECT COUNT(*) FROM _test_optimized_patient_arv_status);

-- =============================================================================
-- CLEANUP (uncomment when done testing)
-- =============================================================================
/*
-- Restore original state
TRUNCATE TABLE patient_status_arv;
INSERT INTO patient_status_arv SELECT * FROM _test_patient_status_arv_backup;

TRUNCATE TABLE exposed_infants;
INSERT INTO exposed_infants SELECT * FROM _test_exposed_infants_backup;

UPDATE patient p
INNER JOIN _test_patient_backup b ON p.patient_id = b.patient_id
SET p.arv_status = b.arv_status;

TRUNCATE TABLE patient_on_arv;
INSERT INTO patient_on_arv SELECT * FROM _test_patient_on_arv_backup;

-- Drop test tables
DROP TABLE IF EXISTS _test_patient_status_arv_backup;
DROP TABLE IF EXISTS _test_exposed_infants_backup;
DROP TABLE IF EXISTS _test_patient_backup;
DROP TABLE IF EXISTS _test_patient_on_arv_backup;
DROP TABLE IF EXISTS _test_original_patient_status_arv;
DROP TABLE IF EXISTS _test_original_exposed_infants;
DROP TABLE IF EXISTS _test_original_patient_arv_status;
DROP TABLE IF EXISTS _test_optimized_patient_status_arv;
DROP TABLE IF EXISTS _test_optimized_exposed_infants;
DROP TABLE IF EXISTS _test_optimized_patient_arv_status;

-- Drop the test procedure
DROP PROCEDURE IF EXISTS patient_status_arv_optimized;
*/
