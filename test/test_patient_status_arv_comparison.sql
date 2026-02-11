-- =============================================================================
-- Test script: Compare current (production) vs new (optimized) ETL script
--
-- This script:
--   1. Saves current state of all affected tables
--   2. Runs the current (production) version
--   3. Captures results
--   4. Restores state
--   5. Runs the new (optimized) version
--   6. Compares results and reports differences
--
-- PREREQUISITES:
--   Before running this script, create two stored procedures by wrapping
--   each flat SQL script's content (excluding the 'use isanteplus;' line)
--   in a procedure:
--
--   DELIMITER $$
--   DROP PROCEDURE IF EXISTS _test_current_version$$
--   CREATE PROCEDURE _test_current_version()
--   BEGIN
--       -- Paste contents of ./patient_status_arv_dml.sql here
--       -- (remove the 'use isanteplus;' line)
--   END$$
--   DELIMITER ;
--
--   DELIMITER $$
--   DROP PROCEDURE IF EXISTS _test_new_version$$
--   CREATE PROCEDURE _test_new_version()
--   BEGIN
--       -- Paste contents of sql_files/patient_status_arv_dml.sql here
--       -- (remove the 'use isanteplus;' line)
--   END$$
--   DELIMITER ;
--
-- Run this during a maintenance window or on a test database.
-- =============================================================================

use isanteplus;

-- =============================================================================
-- Step 1: Create backup tables to save current state
-- =============================================================================
DROP TABLE IF EXISTS _test_patient_status_arv_backup;
CREATE TABLE _test_patient_status_arv_backup AS SELECT * FROM patient_status_arv;

DROP TABLE IF EXISTS _test_exposed_infants_backup;
CREATE TABLE _test_exposed_infants_backup AS SELECT * FROM exposed_infants;

DROP TABLE IF EXISTS _test_patient_backup;
CREATE TABLE _test_patient_backup AS SELECT patient_id, arv_status FROM patient;

DROP TABLE IF EXISTS _test_patient_on_arv_backup;
CREATE TABLE _test_patient_on_arv_backup AS SELECT * FROM patient_on_arv;

DROP TABLE IF EXISTS _test_pepfartable_backup;
CREATE TABLE _test_pepfartable_backup AS SELECT * FROM pepfarTable;

DROP TABLE IF EXISTS _test_alert_backup;
CREATE TABLE _test_alert_backup AS SELECT * FROM alert;

DROP TABLE IF EXISTS _test_patient_immunization_backup;
CREATE TABLE _test_patient_immunization_backup AS SELECT * FROM patient_immunization;

DROP TABLE IF EXISTS _test_immunization_dose_backup;
CREATE TABLE _test_immunization_dose_backup AS SELECT * FROM immunization_dose;

DROP TABLE IF EXISTS _test_isanteplus_patient_arv_backup;
CREATE TABLE _test_isanteplus_patient_arv_backup AS
    SELECT * FROM openmrs.isanteplus_patient_arv;

-- =============================================================================
-- Step 2: Run the CURRENT (production) version
-- =============================================================================
CALL _test_current_version();

-- =============================================================================
-- Step 3: Capture results from current version
-- =============================================================================
DROP TABLE IF EXISTS _test_current_patient_status_arv;
CREATE TABLE _test_current_patient_status_arv AS SELECT * FROM patient_status_arv;

DROP TABLE IF EXISTS _test_current_exposed_infants;
CREATE TABLE _test_current_exposed_infants AS SELECT * FROM exposed_infants;

DROP TABLE IF EXISTS _test_current_patient_arv_status;
CREATE TABLE _test_current_patient_arv_status AS
    SELECT patient_id, arv_status FROM patient WHERE arv_status IS NOT NULL;

DROP TABLE IF EXISTS _test_current_pepfartable;
CREATE TABLE _test_current_pepfartable AS SELECT * FROM pepfarTable;

DROP TABLE IF EXISTS _test_current_alert;
CREATE TABLE _test_current_alert AS SELECT * FROM alert;

DROP TABLE IF EXISTS _test_current_patient_immunization;
CREATE TABLE _test_current_patient_immunization AS SELECT * FROM patient_immunization;

DROP TABLE IF EXISTS _test_current_immunization_dose;
CREATE TABLE _test_current_immunization_dose AS SELECT * FROM immunization_dose;

DROP TABLE IF EXISTS _test_current_isanteplus_patient_arv;
CREATE TABLE _test_current_isanteplus_patient_arv AS
    SELECT * FROM openmrs.isanteplus_patient_arv;

-- =============================================================================
-- Step 4: Restore state before running new version
-- =============================================================================
TRUNCATE TABLE patient_status_arv;
INSERT INTO patient_status_arv SELECT * FROM _test_patient_status_arv_backup;

TRUNCATE TABLE exposed_infants;
INSERT INTO exposed_infants SELECT * FROM _test_exposed_infants_backup;

UPDATE patient p
INNER JOIN _test_patient_backup b ON p.patient_id = b.patient_id
SET p.arv_status = b.arv_status;

TRUNCATE TABLE patient_on_arv;
INSERT INTO patient_on_arv SELECT * FROM _test_patient_on_arv_backup;

TRUNCATE TABLE pepfarTable;
INSERT INTO pepfarTable SELECT * FROM _test_pepfartable_backup;

TRUNCATE TABLE alert;
INSERT INTO alert SELECT * FROM _test_alert_backup;

DELETE FROM patient_immunization;
INSERT INTO patient_immunization SELECT * FROM _test_patient_immunization_backup;

TRUNCATE TABLE immunization_dose;
INSERT INTO immunization_dose SELECT * FROM _test_immunization_dose_backup;

DELETE FROM openmrs.isanteplus_patient_arv;
INSERT INTO openmrs.isanteplus_patient_arv SELECT * FROM _test_isanteplus_patient_arv_backup;

-- =============================================================================
-- Step 5: Run the NEW (optimized) version
-- =============================================================================
CALL _test_new_version();

-- =============================================================================
-- Step 6: Capture results from new version
-- =============================================================================
DROP TABLE IF EXISTS _test_new_patient_status_arv;
CREATE TABLE _test_new_patient_status_arv AS SELECT * FROM patient_status_arv;

DROP TABLE IF EXISTS _test_new_exposed_infants;
CREATE TABLE _test_new_exposed_infants AS SELECT * FROM exposed_infants;

DROP TABLE IF EXISTS _test_new_patient_arv_status;
CREATE TABLE _test_new_patient_arv_status AS
    SELECT patient_id, arv_status FROM patient WHERE arv_status IS NOT NULL;

DROP TABLE IF EXISTS _test_new_pepfartable;
CREATE TABLE _test_new_pepfartable AS SELECT * FROM pepfarTable;

DROP TABLE IF EXISTS _test_new_alert;
CREATE TABLE _test_new_alert AS SELECT * FROM alert;

DROP TABLE IF EXISTS _test_new_patient_immunization;
CREATE TABLE _test_new_patient_immunization AS SELECT * FROM patient_immunization;

DROP TABLE IF EXISTS _test_new_immunization_dose;
CREATE TABLE _test_new_immunization_dose AS SELECT * FROM immunization_dose;

DROP TABLE IF EXISTS _test_new_isanteplus_patient_arv;
CREATE TABLE _test_new_isanteplus_patient_arv AS
    SELECT * FROM openmrs.isanteplus_patient_arv;

-- =============================================================================
-- COMPARISON QUERIES
-- =============================================================================

-- ---- patient_status_arv ----

SELECT 'patient_status_arv: in CURRENT only' AS comparison;
SELECT o.patient_id, o.id_status, o.start_date
FROM _test_current_patient_status_arv o
LEFT JOIN _test_new_patient_status_arv n
    ON o.patient_id = n.patient_id
   AND o.id_status = n.id_status
   AND o.start_date = n.start_date
WHERE n.patient_id IS NULL
LIMIT 100;

SELECT 'patient_status_arv: in NEW only' AS comparison;
SELECT n.patient_id, n.id_status, n.start_date
FROM _test_new_patient_status_arv n
LEFT JOIN _test_current_patient_status_arv o
    ON n.patient_id = o.patient_id
   AND n.id_status = o.id_status
   AND n.start_date = o.start_date
WHERE o.patient_id IS NULL
LIMIT 100;

-- ---- exposed_infants ----

SELECT 'exposed_infants: in CURRENT only' AS comparison;
SELECT o.patient_id, o.condition_exposee, o.visit_date
FROM _test_current_exposed_infants o
LEFT JOIN _test_new_exposed_infants n
    ON o.patient_id = n.patient_id
   AND o.condition_exposee = n.condition_exposee
WHERE n.patient_id IS NULL
LIMIT 100;

SELECT 'exposed_infants: in NEW only' AS comparison;
SELECT n.patient_id, n.condition_exposee, n.visit_date
FROM _test_new_exposed_infants n
LEFT JOIN _test_current_exposed_infants o
    ON n.patient_id = o.patient_id
   AND n.condition_exposee = o.condition_exposee
WHERE o.patient_id IS NULL
LIMIT 100;

-- ---- patient.arv_status ----
-- MySQL does not support FULL OUTER JOIN; emulate with UNION of LEFT JOINs

SELECT 'patient.arv_status: DIFFERENCES' AS comparison;
SELECT
    COALESCE(o.patient_id, n.patient_id) AS patient_id,
    o.arv_status AS current_status,
    n.arv_status AS new_status
FROM _test_current_patient_arv_status o
LEFT JOIN _test_new_patient_arv_status n ON o.patient_id = n.patient_id
WHERE o.arv_status <> n.arv_status
   OR n.patient_id IS NULL
UNION
SELECT
    n.patient_id,
    o.arv_status,
    n.arv_status
FROM _test_new_patient_arv_status n
LEFT JOIN _test_current_patient_arv_status o ON n.patient_id = o.patient_id
WHERE o.patient_id IS NULL
LIMIT 100;

-- ---- pepfarTable ----

SELECT 'pepfarTable: in CURRENT only' AS comparison;
SELECT o.patient_id, o.regimen, o.visit_date
FROM _test_current_pepfartable o
LEFT JOIN _test_new_pepfartable n
    ON o.patient_id = n.patient_id
   AND o.visit_date = n.visit_date
   AND o.regimen = n.regimen
WHERE n.patient_id IS NULL
LIMIT 100;

SELECT 'pepfarTable: in NEW only' AS comparison;
SELECT n.patient_id, n.regimen, n.visit_date
FROM _test_new_pepfartable n
LEFT JOIN _test_current_pepfartable o
    ON n.patient_id = o.patient_id
   AND n.visit_date = o.visit_date
   AND n.regimen = o.regimen
WHERE o.patient_id IS NULL
LIMIT 100;

-- ---- alert ----

SELECT 'alert: in CURRENT only' AS comparison;
SELECT o.patient_id, o.id_alert, o.date_alert, o.encounter_id
FROM _test_current_alert o
LEFT JOIN _test_new_alert n
    ON o.patient_id = n.patient_id
   AND o.id_alert = n.id_alert
   AND o.encounter_id = n.encounter_id
WHERE n.patient_id IS NULL
LIMIT 100;

SELECT 'alert: in NEW only' AS comparison;
SELECT n.patient_id, n.id_alert, n.date_alert, n.encounter_id
FROM _test_new_alert n
LEFT JOIN _test_current_alert o
    ON n.patient_id = o.patient_id
   AND n.id_alert = o.id_alert
   AND n.encounter_id = o.encounter_id
WHERE o.patient_id IS NULL
LIMIT 100;

-- Alert counts by type
SELECT 'ALERT COUNTS BY TYPE' AS comparison;
SELECT
    COALESCE(c.id_alert, n.id_alert) AS alert_type,
    c.current_count,
    n.new_count,
    COALESCE(c.current_count, 0) - COALESCE(n.new_count, 0) AS difference
FROM (
    SELECT id_alert, COUNT(*) AS current_count
    FROM _test_current_alert GROUP BY id_alert
) c
LEFT JOIN (
    SELECT id_alert, COUNT(*) AS new_count
    FROM _test_new_alert GROUP BY id_alert
) n ON c.id_alert = n.id_alert
UNION
SELECT
    n.id_alert,
    c.current_count,
    n.new_count,
    COALESCE(c.current_count, 0) - COALESCE(n.new_count, 0)
FROM (
    SELECT id_alert, COUNT(*) AS new_count
    FROM _test_new_alert GROUP BY id_alert
) n
LEFT JOIN (
    SELECT id_alert, COUNT(*) AS current_count
    FROM _test_current_alert GROUP BY id_alert
) c ON n.id_alert = c.id_alert
WHERE c.id_alert IS NULL
ORDER BY alert_type;

-- ---- immunization_dose ----

SELECT 'immunization_dose: in CURRENT only' AS comparison;
SELECT o.patient_id, o.vaccine_concept_id
FROM _test_current_immunization_dose o
LEFT JOIN _test_new_immunization_dose n
    ON o.patient_id = n.patient_id
   AND o.vaccine_concept_id = n.vaccine_concept_id
WHERE n.patient_id IS NULL
LIMIT 100;

SELECT 'immunization_dose: in NEW only' AS comparison;
SELECT n.patient_id, n.vaccine_concept_id
FROM _test_new_immunization_dose n
LEFT JOIN _test_current_immunization_dose o
    ON n.patient_id = o.patient_id
   AND n.vaccine_concept_id = o.vaccine_concept_id
WHERE o.patient_id IS NULL
LIMIT 100;

-- ---- Summary counts ----

SELECT 'SUMMARY COUNTS' AS comparison;
SELECT
    'patient_status_arv' AS table_name,
    (SELECT COUNT(*) FROM _test_current_patient_status_arv) AS current_count,
    (SELECT COUNT(*) FROM _test_new_patient_status_arv) AS new_count
UNION ALL
SELECT
    'exposed_infants',
    (SELECT COUNT(*) FROM _test_current_exposed_infants),
    (SELECT COUNT(*) FROM _test_new_exposed_infants)
UNION ALL
SELECT
    'patient (with arv_status)',
    (SELECT COUNT(*) FROM _test_current_patient_arv_status),
    (SELECT COUNT(*) FROM _test_new_patient_arv_status)
UNION ALL
SELECT
    'pepfarTable',
    (SELECT COUNT(*) FROM _test_current_pepfartable),
    (SELECT COUNT(*) FROM _test_new_pepfartable)
UNION ALL
SELECT
    'alert',
    (SELECT COUNT(*) FROM _test_current_alert),
    (SELECT COUNT(*) FROM _test_new_alert)
UNION ALL
SELECT
    'patient_immunization',
    (SELECT COUNT(*) FROM _test_current_patient_immunization),
    (SELECT COUNT(*) FROM _test_new_patient_immunization)
UNION ALL
SELECT
    'immunization_dose',
    (SELECT COUNT(*) FROM _test_current_immunization_dose),
    (SELECT COUNT(*) FROM _test_new_immunization_dose);

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

TRUNCATE TABLE pepfarTable;
INSERT INTO pepfarTable SELECT * FROM _test_pepfartable_backup;

TRUNCATE TABLE alert;
INSERT INTO alert SELECT * FROM _test_alert_backup;

DELETE FROM patient_immunization;
INSERT INTO patient_immunization SELECT * FROM _test_patient_immunization_backup;

TRUNCATE TABLE immunization_dose;
INSERT INTO immunization_dose SELECT * FROM _test_immunization_dose_backup;

DELETE FROM openmrs.isanteplus_patient_arv;
INSERT INTO openmrs.isanteplus_patient_arv SELECT * FROM _test_isanteplus_patient_arv_backup;

-- Drop test tables
DROP TABLE IF EXISTS _test_patient_status_arv_backup;
DROP TABLE IF EXISTS _test_exposed_infants_backup;
DROP TABLE IF EXISTS _test_patient_backup;
DROP TABLE IF EXISTS _test_patient_on_arv_backup;
DROP TABLE IF EXISTS _test_pepfartable_backup;
DROP TABLE IF EXISTS _test_alert_backup;
DROP TABLE IF EXISTS _test_patient_immunization_backup;
DROP TABLE IF EXISTS _test_immunization_dose_backup;
DROP TABLE IF EXISTS _test_isanteplus_patient_arv_backup;
DROP TABLE IF EXISTS _test_current_patient_status_arv;
DROP TABLE IF EXISTS _test_current_exposed_infants;
DROP TABLE IF EXISTS _test_current_patient_arv_status;
DROP TABLE IF EXISTS _test_current_pepfartable;
DROP TABLE IF EXISTS _test_current_alert;
DROP TABLE IF EXISTS _test_current_patient_immunization;
DROP TABLE IF EXISTS _test_current_immunization_dose;
DROP TABLE IF EXISTS _test_current_isanteplus_patient_arv;
DROP TABLE IF EXISTS _test_new_patient_status_arv;
DROP TABLE IF EXISTS _test_new_exposed_infants;
DROP TABLE IF EXISTS _test_new_patient_arv_status;
DROP TABLE IF EXISTS _test_new_pepfartable;
DROP TABLE IF EXISTS _test_new_alert;
DROP TABLE IF EXISTS _test_new_patient_immunization;
DROP TABLE IF EXISTS _test_new_immunization_dose;
DROP TABLE IF EXISTS _test_new_isanteplus_patient_arv;

-- Drop the test procedures
DROP PROCEDURE IF EXISTS _test_current_version;
DROP PROCEDURE IF EXISTS _test_new_version;
*/
