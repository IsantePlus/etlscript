-- =============================================================================
-- Test script: Compare original vs optimized alert_viral_load procedure
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
--
-- PREREQUISITE: Load the optimized procedure with name 'alert_viral_load_optimized'
-- before running this script. You can do this by copying the procedure definition
-- from patient_status_arv_dml_optimized.sql and changing the procedure name.
-- =============================================================================

use isanteplus;

-- Step 1: Create backup tables to save current state
DROP TABLE IF EXISTS _test_alert_backup;
CREATE TABLE _test_alert_backup AS SELECT * FROM alert;

-- Step 2: Run the ORIGINAL procedure
CALL alert_viral_load();

-- Step 3: Capture results from original
DROP TABLE IF EXISTS _test_original_alert;
CREATE TABLE _test_original_alert AS SELECT * FROM alert;

-- Step 4: Restore state before running optimized version
TRUNCATE TABLE alert;
INSERT INTO alert SELECT * FROM _test_alert_backup;

-- Step 5: Run the OPTIMIZED procedure (must be loaded with a different name first)
CALL alert_viral_load_optimized();

-- Step 6: Capture results from optimized
DROP TABLE IF EXISTS _test_optimized_alert;
CREATE TABLE _test_optimized_alert AS SELECT * FROM alert;

-- =============================================================================
-- COMPARISON QUERIES
-- =============================================================================

-- Compare alert: rows in original but not in optimized
-- Join on patient_id, id_alert, AND encounter_id since a patient can have multiple
-- alerts of the same type with different encounters
SELECT 'alert: in ORIGINAL only' AS comparison;
SELECT o.patient_id, o.id_alert, o.date_alert, o.encounter_id
FROM _test_original_alert o
LEFT JOIN _test_optimized_alert n
    ON o.patient_id = n.patient_id
   AND o.id_alert = n.id_alert
   AND o.encounter_id = n.encounter_id
WHERE n.patient_id IS NULL
LIMIT 100;

-- Compare alert: rows in optimized but not in original
SELECT 'alert: in OPTIMIZED only' AS comparison;
SELECT n.patient_id, n.id_alert, n.date_alert, n.encounter_id
FROM _test_optimized_alert n
LEFT JOIN _test_original_alert o
    ON n.patient_id = o.patient_id
   AND n.id_alert = o.id_alert
   AND n.encounter_id = o.encounter_id
WHERE o.patient_id IS NULL
LIMIT 100;

-- Compare alert: same patient/alert/encounter but different dates
-- This catches cases where the same encounter is recorded with different dates
SELECT 'alert: DATE DIFFERENCES' AS comparison;
SELECT
    o.patient_id,
    o.id_alert,
    o.date_alert AS original_date,
    n.date_alert AS optimized_date,
    o.encounter_id
FROM _test_original_alert o
INNER JOIN _test_optimized_alert n
    ON o.patient_id = n.patient_id
   AND o.id_alert = n.id_alert
   AND o.encounter_id = n.encounter_id
WHERE o.date_alert <> n.date_alert
LIMIT 100;

-- Summary counts by alert type
SELECT 'SUMMARY COUNTS BY ALERT TYPE' AS comparison;
SELECT
    COALESCE(o.id_alert, n.id_alert) AS alert_type,
    o.original_count,
    n.optimized_count,
    COALESCE(o.original_count, 0) - COALESCE(n.optimized_count, 0) AS difference
FROM (
    SELECT id_alert, COUNT(*) AS original_count
    FROM _test_original_alert
    GROUP BY id_alert
) o
LEFT JOIN (
    SELECT id_alert, COUNT(*) AS optimized_count
    FROM _test_optimized_alert
    GROUP BY id_alert
) n ON o.id_alert = n.id_alert
UNION
SELECT
    n.id_alert,
    o.original_count,
    n.optimized_count,
    COALESCE(o.original_count, 0) - COALESCE(n.optimized_count, 0)
FROM (
    SELECT id_alert, COUNT(*) AS optimized_count
    FROM _test_optimized_alert
    GROUP BY id_alert
) n
LEFT JOIN (
    SELECT id_alert, COUNT(*) AS original_count
    FROM _test_original_alert
    GROUP BY id_alert
) o ON n.id_alert = o.id_alert
WHERE o.id_alert IS NULL
ORDER BY alert_type;

-- Total counts
SELECT 'TOTAL COUNTS' AS comparison;
SELECT
    'alert' AS table_name,
    (SELECT COUNT(*) FROM _test_original_alert) AS original_count,
    (SELECT COUNT(*) FROM _test_optimized_alert) AS optimized_count;

-- Alert type legend
SELECT 'ALERT TYPE LEGEND' AS info;
SELECT 1 AS id_alert, 'Patient on ARV >= 6 months without viral load' AS description
UNION ALL SELECT 2, 'Patient on ARV = 5 months without viral load'
UNION ALL SELECT 3, 'Pregnant woman on ARV >= 4 months without viral load'
UNION ALL SELECT 4, 'Last viral load >= 12 months ago (suppressed)'
UNION ALL SELECT 5, 'Last viral load >= 3 months ago with > 1000 copies/ml'
UNION ALL SELECT 6, 'Last viral load > 1000 copies/ml'
UNION ALL SELECT 7, 'Patient must refill ARV within 30 days'
UNION ALL SELECT 8, 'Patient has no more medications available'
UNION ALL SELECT 9, 'TB/HIV co-infection'
UNION ALL SELECT 10, 'Patient on ARV >= 3 months without viral load'
UNION ALL SELECT 11, 'Patient on ARV without INH prophylaxis'
UNION ALL SELECT 12, 'DDP subscription';

-- =============================================================================
-- CLEANUP (uncomment when done testing)
-- =============================================================================
/*
-- Restore original state
TRUNCATE TABLE alert;
INSERT INTO alert SELECT * FROM _test_alert_backup;

-- Drop test tables
DROP TABLE IF EXISTS _test_alert_backup;
DROP TABLE IF EXISTS _test_original_alert;
DROP TABLE IF EXISTS _test_optimized_alert;

-- Drop the test procedure
DROP PROCEDURE IF EXISTS alert_viral_load_optimized;
*/
