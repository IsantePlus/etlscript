-- =============================================================================
-- Test script: Compare current vs new version of isanteplusreportsdmlscript.sql
--
-- This script:
--   1. Saves current state of all destination tables
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
--   DROP PROCEDURE IF EXISTS _test_reports_current$$
--   CREATE PROCEDURE _test_reports_current()
--   BEGIN
--       -- Paste contents of current isanteplusreportsdmlscript.sql here
--       -- (remove the 'use isanteplus;' line)
--   END$$
--   DELIMITER ;
--
--   DELIMITER $$
--   DROP PROCEDURE IF EXISTS _test_reports_new$$
--   CREATE PROCEDURE _test_reports_new()
--   BEGIN
--       -- Paste contents of the new version here
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

DROP TABLE IF EXISTS _tr_bak_patient;
CREATE TABLE _tr_bak_patient AS SELECT * FROM patient;

DROP TABLE IF EXISTS _tr_bak_patient_visit;
CREATE TABLE _tr_bak_patient_visit AS SELECT * FROM patient_visit;

DROP TABLE IF EXISTS _tr_bak_patient_dispensing;
CREATE TABLE _tr_bak_patient_dispensing AS SELECT * FROM patient_dispensing;

DROP TABLE IF EXISTS _tr_bak_patient_prescription;
CREATE TABLE _tr_bak_patient_prescription AS SELECT * FROM patient_prescription;

DROP TABLE IF EXISTS _tr_bak_health_qual;
CREATE TABLE _tr_bak_health_qual AS SELECT * FROM health_qual_patient_visit;

DROP TABLE IF EXISTS _tr_bak_patient_laboratory;
CREATE TABLE _tr_bak_patient_laboratory AS SELECT * FROM patient_laboratory;

DROP TABLE IF EXISTS _tr_bak_patient_tb_diagnosis;
CREATE TABLE _tr_bak_patient_tb_diagnosis AS SELECT * FROM patient_tb_diagnosis;

DROP TABLE IF EXISTS _tr_bak_patient_nutrition;
CREATE TABLE _tr_bak_patient_nutrition AS SELECT * FROM patient_nutrition;

DROP TABLE IF EXISTS _tr_bak_patient_ob_gyn;
CREATE TABLE _tr_bak_patient_ob_gyn AS SELECT * FROM patient_ob_gyn;

DROP TABLE IF EXISTS _tr_bak_patient_imagerie;
CREATE TABLE _tr_bak_patient_imagerie AS SELECT * FROM patient_imagerie;

DROP TABLE IF EXISTS _tr_bak_discontinuation_reason;
CREATE TABLE _tr_bak_discontinuation_reason AS SELECT * FROM discontinuation_reason;

DROP TABLE IF EXISTS _tr_bak_stopping_reason;
CREATE TABLE _tr_bak_stopping_reason AS SELECT * FROM stopping_reason;

DROP TABLE IF EXISTS _tr_bak_patient_pregnancy;
CREATE TABLE _tr_bak_patient_pregnancy AS SELECT * FROM patient_pregnancy;

DROP TABLE IF EXISTS _tr_bak_alert;
CREATE TABLE _tr_bak_alert AS SELECT * FROM alert;

DROP TABLE IF EXISTS _tr_bak_visit_type;
CREATE TABLE _tr_bak_visit_type AS SELECT * FROM visit_type;

DROP TABLE IF EXISTS _tr_bak_patient_delivery;
CREATE TABLE _tr_bak_patient_delivery AS SELECT * FROM patient_delivery;

DROP TABLE IF EXISTS _tr_bak_virological_tests;
CREATE TABLE _tr_bak_virological_tests AS SELECT * FROM virological_tests;

DROP TABLE IF EXISTS _tr_bak_serological_tests;
CREATE TABLE _tr_bak_serological_tests AS SELECT * FROM serological_tests;

DROP TABLE IF EXISTS _tr_bak_patient_pcr;
CREATE TABLE _tr_bak_patient_pcr AS SELECT * FROM patient_pcr;

DROP TABLE IF EXISTS _tr_bak_pediatric_hiv_visit;
CREATE TABLE _tr_bak_pediatric_hiv_visit AS SELECT * FROM pediatric_hiv_visit;

DROP TABLE IF EXISTS _tr_bak_patient_menstruation;
CREATE TABLE _tr_bak_patient_menstruation AS SELECT * FROM patient_menstruation;

DROP TABLE IF EXISTS _tr_bak_vih_risk_factor;
CREATE TABLE _tr_bak_vih_risk_factor AS SELECT * FROM vih_risk_factor;

DROP TABLE IF EXISTS _tr_bak_vaccination;
CREATE TABLE _tr_bak_vaccination AS SELECT * FROM vaccination;

DROP TABLE IF EXISTS _tr_bak_patient_malaria;
CREATE TABLE _tr_bak_patient_malaria AS SELECT * FROM patient_malaria;

DROP TABLE IF EXISTS _tr_bak_patient_on_art;
CREATE TABLE _tr_bak_patient_on_art AS SELECT * FROM patient_on_art;

DROP TABLE IF EXISTS _tr_bak_patient_on_arv;
CREATE TABLE _tr_bak_patient_on_arv AS SELECT * FROM patient_on_arv;

DROP TABLE IF EXISTS _tr_bak_family_planning;
CREATE TABLE _tr_bak_family_planning AS SELECT * FROM family_planning;

-- =============================================================================
-- Step 2: Run the CURRENT (production) version
-- =============================================================================
SET @_tr_current_start = NOW(6);
CALL _test_reports_current();
SET @_tr_current_end = NOW(6);

-- =============================================================================
-- Step 3: Capture results from current version
-- =============================================================================

DROP TABLE IF EXISTS _tr_cur_patient;
CREATE TABLE _tr_cur_patient AS SELECT * FROM patient;

DROP TABLE IF EXISTS _tr_cur_patient_visit;
CREATE TABLE _tr_cur_patient_visit AS SELECT * FROM patient_visit;

DROP TABLE IF EXISTS _tr_cur_patient_dispensing;
CREATE TABLE _tr_cur_patient_dispensing AS SELECT * FROM patient_dispensing;

DROP TABLE IF EXISTS _tr_cur_patient_prescription;
CREATE TABLE _tr_cur_patient_prescription AS SELECT * FROM patient_prescription;

DROP TABLE IF EXISTS _tr_cur_health_qual;
CREATE TABLE _tr_cur_health_qual AS SELECT * FROM health_qual_patient_visit;

DROP TABLE IF EXISTS _tr_cur_patient_laboratory;
CREATE TABLE _tr_cur_patient_laboratory AS SELECT * FROM patient_laboratory;

DROP TABLE IF EXISTS _tr_cur_patient_tb_diagnosis;
CREATE TABLE _tr_cur_patient_tb_diagnosis AS SELECT * FROM patient_tb_diagnosis;

DROP TABLE IF EXISTS _tr_cur_patient_nutrition;
CREATE TABLE _tr_cur_patient_nutrition AS SELECT * FROM patient_nutrition;

DROP TABLE IF EXISTS _tr_cur_patient_ob_gyn;
CREATE TABLE _tr_cur_patient_ob_gyn AS SELECT * FROM patient_ob_gyn;

DROP TABLE IF EXISTS _tr_cur_patient_imagerie;
CREATE TABLE _tr_cur_patient_imagerie AS SELECT * FROM patient_imagerie;

DROP TABLE IF EXISTS _tr_cur_discontinuation_reason;
CREATE TABLE _tr_cur_discontinuation_reason AS SELECT * FROM discontinuation_reason;

DROP TABLE IF EXISTS _tr_cur_stopping_reason;
CREATE TABLE _tr_cur_stopping_reason AS SELECT * FROM stopping_reason;

DROP TABLE IF EXISTS _tr_cur_patient_pregnancy;
CREATE TABLE _tr_cur_patient_pregnancy AS SELECT * FROM patient_pregnancy;

DROP TABLE IF EXISTS _tr_cur_alert;
CREATE TABLE _tr_cur_alert AS SELECT * FROM alert;

DROP TABLE IF EXISTS _tr_cur_visit_type;
CREATE TABLE _tr_cur_visit_type AS SELECT * FROM visit_type;

DROP TABLE IF EXISTS _tr_cur_patient_delivery;
CREATE TABLE _tr_cur_patient_delivery AS SELECT * FROM patient_delivery;

DROP TABLE IF EXISTS _tr_cur_virological_tests;
CREATE TABLE _tr_cur_virological_tests AS SELECT * FROM virological_tests;

DROP TABLE IF EXISTS _tr_cur_serological_tests;
CREATE TABLE _tr_cur_serological_tests AS SELECT * FROM serological_tests;

DROP TABLE IF EXISTS _tr_cur_patient_pcr;
CREATE TABLE _tr_cur_patient_pcr AS SELECT * FROM patient_pcr;

DROP TABLE IF EXISTS _tr_cur_pediatric_hiv_visit;
CREATE TABLE _tr_cur_pediatric_hiv_visit AS SELECT * FROM pediatric_hiv_visit;

DROP TABLE IF EXISTS _tr_cur_patient_menstruation;
CREATE TABLE _tr_cur_patient_menstruation AS SELECT * FROM patient_menstruation;

DROP TABLE IF EXISTS _tr_cur_vih_risk_factor;
CREATE TABLE _tr_cur_vih_risk_factor AS SELECT * FROM vih_risk_factor;

DROP TABLE IF EXISTS _tr_cur_vaccination;
CREATE TABLE _tr_cur_vaccination AS SELECT * FROM vaccination;

DROP TABLE IF EXISTS _tr_cur_patient_malaria;
CREATE TABLE _tr_cur_patient_malaria AS SELECT * FROM patient_malaria;

DROP TABLE IF EXISTS _tr_cur_patient_on_art;
CREATE TABLE _tr_cur_patient_on_art AS SELECT * FROM patient_on_art;

DROP TABLE IF EXISTS _tr_cur_patient_on_arv;
CREATE TABLE _tr_cur_patient_on_arv AS SELECT * FROM patient_on_arv;

DROP TABLE IF EXISTS _tr_cur_family_planning;
CREATE TABLE _tr_cur_family_planning AS SELECT * FROM family_planning;


-- =============================================================================
-- Step 4: Restore state before running new version
-- =============================================================================

-- patient: restore all ETL-written columns
UPDATE patient p
INNER JOIN _tr_bak_patient b ON p.patient_id = b.patient_id
SET p.given_name      = b.given_name,
    p.family_name     = b.family_name,
    p.gender          = b.gender,
    p.birthdate       = b.birthdate,
    p.vih_status      = b.vih_status,
    p.arv_status      = b.arv_status,
    p.st_id           = b.st_id,
    p.pc_id           = b.pc_id,
    p.national_id     = b.national_id,
    p.identifier      = b.identifier,
    p.isante_id       = b.isante_id,
    p.location_id     = b.location_id,
    p.site_code       = b.site_code,
    p.last_address    = b.last_address,
    p.place_of_birth  = b.place_of_birth,
    p.telephone       = b.telephone,
    p.mother_name     = b.mother_name,
    p.contact_name    = b.contact_name,
    p.maritalStatus   = b.maritalStatus,
    p.occupation      = b.occupation,
    p.first_visit_date = b.first_visit_date,
    p.last_visit_date  = b.last_visit_date,
    p.next_visit_date  = b.next_visit_date,
    p.date_started_arv = b.date_started_arv,
    p.transferred_in   = b.transferred_in,
    p.date_transferred_in = b.date_transferred_in,
    p.date_started_arv_other_site = b.date_started_arv_other_site;

TRUNCATE TABLE patient_visit;
INSERT INTO patient_visit SELECT * FROM _tr_bak_patient_visit;

TRUNCATE TABLE patient_dispensing;
INSERT INTO patient_dispensing SELECT * FROM _tr_bak_patient_dispensing;

TRUNCATE TABLE patient_prescription;
INSERT INTO patient_prescription SELECT * FROM _tr_bak_patient_prescription;

TRUNCATE TABLE health_qual_patient_visit;
INSERT INTO health_qual_patient_visit SELECT * FROM _tr_bak_health_qual;

TRUNCATE TABLE patient_laboratory;
INSERT INTO patient_laboratory SELECT * FROM _tr_bak_patient_laboratory;

TRUNCATE TABLE patient_tb_diagnosis;
INSERT INTO patient_tb_diagnosis SELECT * FROM _tr_bak_patient_tb_diagnosis;

TRUNCATE TABLE patient_nutrition;
INSERT INTO patient_nutrition SELECT * FROM _tr_bak_patient_nutrition;

TRUNCATE TABLE patient_ob_gyn;
INSERT INTO patient_ob_gyn SELECT * FROM _tr_bak_patient_ob_gyn;

TRUNCATE TABLE patient_imagerie;
INSERT INTO patient_imagerie SELECT * FROM _tr_bak_patient_imagerie;

TRUNCATE TABLE discontinuation_reason;
INSERT INTO discontinuation_reason SELECT * FROM _tr_bak_discontinuation_reason;

TRUNCATE TABLE stopping_reason;
INSERT INTO stopping_reason SELECT * FROM _tr_bak_stopping_reason;

TRUNCATE TABLE patient_pregnancy;
INSERT INTO patient_pregnancy SELECT * FROM _tr_bak_patient_pregnancy;

TRUNCATE TABLE alert;
INSERT INTO alert SELECT * FROM _tr_bak_alert;

TRUNCATE TABLE visit_type;
INSERT INTO visit_type SELECT * FROM _tr_bak_visit_type;

TRUNCATE TABLE patient_delivery;
INSERT INTO patient_delivery SELECT * FROM _tr_bak_patient_delivery;

TRUNCATE TABLE virological_tests;
INSERT INTO virological_tests SELECT * FROM _tr_bak_virological_tests;

TRUNCATE TABLE serological_tests;
INSERT INTO serological_tests SELECT * FROM _tr_bak_serological_tests;

TRUNCATE TABLE patient_pcr;
INSERT INTO patient_pcr SELECT * FROM _tr_bak_patient_pcr;

DELETE FROM pediatric_hiv_visit;
INSERT INTO pediatric_hiv_visit SELECT * FROM _tr_bak_pediatric_hiv_visit;

TRUNCATE TABLE patient_menstruation;
INSERT INTO patient_menstruation SELECT * FROM _tr_bak_patient_menstruation;

TRUNCATE TABLE vih_risk_factor;
INSERT INTO vih_risk_factor SELECT * FROM _tr_bak_vih_risk_factor;

TRUNCATE TABLE vaccination;
INSERT INTO vaccination SELECT * FROM _tr_bak_vaccination;

TRUNCATE TABLE patient_malaria;
INSERT INTO patient_malaria SELECT * FROM _tr_bak_patient_malaria;

DELETE FROM patient_on_art;
INSERT INTO patient_on_art SELECT * FROM _tr_bak_patient_on_art;

TRUNCATE TABLE patient_on_arv;
INSERT INTO patient_on_arv SELECT * FROM _tr_bak_patient_on_arv;

TRUNCATE TABLE family_planning;
INSERT INTO family_planning SELECT * FROM _tr_bak_family_planning;


-- =============================================================================
-- Step 5: Run the NEW version
-- =============================================================================
SET @_tr_new_start = NOW(6);
CALL _test_reports_new();
SET @_tr_new_end = NOW(6);


-- =============================================================================
-- Step 6: Capture results from new version
-- =============================================================================

DROP TABLE IF EXISTS _tr_new_patient;
CREATE TABLE _tr_new_patient AS SELECT * FROM patient;

DROP TABLE IF EXISTS _tr_new_patient_visit;
CREATE TABLE _tr_new_patient_visit AS SELECT * FROM patient_visit;

DROP TABLE IF EXISTS _tr_new_patient_dispensing;
CREATE TABLE _tr_new_patient_dispensing AS SELECT * FROM patient_dispensing;

DROP TABLE IF EXISTS _tr_new_patient_prescription;
CREATE TABLE _tr_new_patient_prescription AS SELECT * FROM patient_prescription;

DROP TABLE IF EXISTS _tr_new_health_qual;
CREATE TABLE _tr_new_health_qual AS SELECT * FROM health_qual_patient_visit;

DROP TABLE IF EXISTS _tr_new_patient_laboratory;
CREATE TABLE _tr_new_patient_laboratory AS SELECT * FROM patient_laboratory;

DROP TABLE IF EXISTS _tr_new_patient_tb_diagnosis;
CREATE TABLE _tr_new_patient_tb_diagnosis AS SELECT * FROM patient_tb_diagnosis;

DROP TABLE IF EXISTS _tr_new_patient_nutrition;
CREATE TABLE _tr_new_patient_nutrition AS SELECT * FROM patient_nutrition;

DROP TABLE IF EXISTS _tr_new_patient_ob_gyn;
CREATE TABLE _tr_new_patient_ob_gyn AS SELECT * FROM patient_ob_gyn;

DROP TABLE IF EXISTS _tr_new_patient_imagerie;
CREATE TABLE _tr_new_patient_imagerie AS SELECT * FROM patient_imagerie;

DROP TABLE IF EXISTS _tr_new_discontinuation_reason;
CREATE TABLE _tr_new_discontinuation_reason AS SELECT * FROM discontinuation_reason;

DROP TABLE IF EXISTS _tr_new_stopping_reason;
CREATE TABLE _tr_new_stopping_reason AS SELECT * FROM stopping_reason;

DROP TABLE IF EXISTS _tr_new_patient_pregnancy;
CREATE TABLE _tr_new_patient_pregnancy AS SELECT * FROM patient_pregnancy;

DROP TABLE IF EXISTS _tr_new_alert;
CREATE TABLE _tr_new_alert AS SELECT * FROM alert;

DROP TABLE IF EXISTS _tr_new_visit_type;
CREATE TABLE _tr_new_visit_type AS SELECT * FROM visit_type;

DROP TABLE IF EXISTS _tr_new_patient_delivery;
CREATE TABLE _tr_new_patient_delivery AS SELECT * FROM patient_delivery;

DROP TABLE IF EXISTS _tr_new_virological_tests;
CREATE TABLE _tr_new_virological_tests AS SELECT * FROM virological_tests;

DROP TABLE IF EXISTS _tr_new_serological_tests;
CREATE TABLE _tr_new_serological_tests AS SELECT * FROM serological_tests;

DROP TABLE IF EXISTS _tr_new_patient_pcr;
CREATE TABLE _tr_new_patient_pcr AS SELECT * FROM patient_pcr;

DROP TABLE IF EXISTS _tr_new_pediatric_hiv_visit;
CREATE TABLE _tr_new_pediatric_hiv_visit AS SELECT * FROM pediatric_hiv_visit;

DROP TABLE IF EXISTS _tr_new_patient_menstruation;
CREATE TABLE _tr_new_patient_menstruation AS SELECT * FROM patient_menstruation;

DROP TABLE IF EXISTS _tr_new_vih_risk_factor;
CREATE TABLE _tr_new_vih_risk_factor AS SELECT * FROM vih_risk_factor;

DROP TABLE IF EXISTS _tr_new_vaccination;
CREATE TABLE _tr_new_vaccination AS SELECT * FROM vaccination;

DROP TABLE IF EXISTS _tr_new_patient_malaria;
CREATE TABLE _tr_new_patient_malaria AS SELECT * FROM patient_malaria;

DROP TABLE IF EXISTS _tr_new_patient_on_art;
CREATE TABLE _tr_new_patient_on_art AS SELECT * FROM patient_on_art;

DROP TABLE IF EXISTS _tr_new_patient_on_arv;
CREATE TABLE _tr_new_patient_on_arv AS SELECT * FROM patient_on_arv;

DROP TABLE IF EXISTS _tr_new_family_planning;
CREATE TABLE _tr_new_family_planning AS SELECT * FROM family_planning;


-- =============================================================================
-- COMPARISON QUERIES
-- =============================================================================

-- ---- patient (demographics) ----

SELECT 'patient: DIFFERENCES (all ETL-written columns)' AS comparison;
SELECT
    COALESCE(c.patient_id, n.patient_id) AS patient_id,
    c.given_name  AS cur_given_name,  n.given_name  AS new_given_name,
    c.st_id       AS cur_st_id,       n.st_id       AS new_st_id,
    c.vih_status  AS cur_vih_status,  n.vih_status  AS new_vih_status,
    c.site_code   AS cur_site_code,   n.site_code   AS new_site_code,
    c.last_address AS cur_last_address, n.last_address AS new_last_address,
    c.place_of_birth AS cur_place_of_birth, n.place_of_birth AS new_place_of_birth,
    c.contact_name AS cur_contact_name, n.contact_name AS new_contact_name,
    c.transferred_in AS cur_transferred_in, n.transferred_in AS new_transferred_in
FROM _tr_cur_patient c
LEFT JOIN _tr_new_patient n ON c.patient_id = n.patient_id
WHERE c.given_name     <> n.given_name
   OR c.family_name    <> n.family_name
   OR c.gender         <> n.gender
   OR IFNULL(c.st_id,'')       <> IFNULL(n.st_id,'')
   OR IFNULL(c.pc_id,'')       <> IFNULL(n.pc_id,'')
   OR IFNULL(c.national_id,'') <> IFNULL(n.national_id,'')
   OR IFNULL(c.identifier,'')  <> IFNULL(n.identifier,'')
   OR IFNULL(c.isante_id,'')   <> IFNULL(n.isante_id,'')
   OR IFNULL(c.site_code,'')   <> IFNULL(n.site_code,'')
   OR IFNULL(c.vih_status,-1)  <> IFNULL(n.vih_status,-1)
   OR IFNULL(c.last_address,'')    <> IFNULL(n.last_address,'')
   OR IFNULL(c.place_of_birth,'')  <> IFNULL(n.place_of_birth,'')
   OR IFNULL(c.telephone,'')       <> IFNULL(n.telephone,'')
   OR IFNULL(c.mother_name,'')     <> IFNULL(n.mother_name,'')
   OR IFNULL(c.contact_name,'')    <> IFNULL(n.contact_name,'')
   OR IFNULL(c.maritalStatus,-1)   <> IFNULL(n.maritalStatus,-1)
   OR IFNULL(c.occupation,-1)      <> IFNULL(n.occupation,-1)
   OR IFNULL(c.location_id,-1)     <> IFNULL(n.location_id,-1)
   OR IFNULL(c.first_visit_date,'1900-01-01')  <> IFNULL(n.first_visit_date,'1900-01-01')
   OR IFNULL(c.last_visit_date,'1900-01-01')   <> IFNULL(n.last_visit_date,'1900-01-01')
   OR IFNULL(c.next_visit_date,'1900-01-01')   <> IFNULL(n.next_visit_date,'1900-01-01')
   OR IFNULL(c.date_started_arv,'1900-01-01')  <> IFNULL(n.date_started_arv,'1900-01-01')
   OR IFNULL(c.transferred_in,-1)  <> IFNULL(n.transferred_in,-1)
   OR IFNULL(c.date_transferred_in,'1900-01-01') <> IFNULL(n.date_transferred_in,'1900-01-01')
   OR IFNULL(c.date_started_arv_other_site,'1900-01-01') <> IFNULL(n.date_started_arv_other_site,'1900-01-01')
   OR n.patient_id IS NULL
UNION
SELECT
    n.patient_id, NULL, n.given_name, NULL, n.st_id,
    NULL, n.vih_status, NULL, n.site_code,
    NULL, n.last_address, NULL, n.place_of_birth,
    NULL, n.contact_name, NULL, n.transferred_in
FROM _tr_new_patient n
LEFT JOIN _tr_cur_patient c ON n.patient_id = c.patient_id
WHERE c.patient_id IS NULL
LIMIT 100;


-- ---- patient_visit ----

SELECT 'patient_visit: in CURRENT only' AS comparison;
SELECT c.patient_id, c.encounter_id, c.visit_date
FROM _tr_cur_patient_visit c
LEFT JOIN _tr_new_patient_visit n
    ON c.patient_id = n.patient_id AND c.encounter_id = n.encounter_id
WHERE n.patient_id IS NULL
LIMIT 50;

SELECT 'patient_visit: in NEW only' AS comparison;
SELECT n.patient_id, n.encounter_id, n.visit_date
FROM _tr_new_patient_visit n
LEFT JOIN _tr_cur_patient_visit c
    ON n.patient_id = c.patient_id AND n.encounter_id = c.encounter_id
WHERE c.patient_id IS NULL
LIMIT 50;


-- ---- patient_dispensing ----

SELECT 'patient_dispensing: in CURRENT only' AS comparison;
SELECT c.patient_id, c.encounter_id, c.drug_id, c.visit_date
FROM _tr_cur_patient_dispensing c
LEFT JOIN _tr_new_patient_dispensing n
    ON c.patient_id = n.patient_id AND c.encounter_id = n.encounter_id
   AND IFNULL(c.drug_id,0) = IFNULL(n.drug_id,0)
WHERE n.patient_id IS NULL
LIMIT 50;

SELECT 'patient_dispensing: in NEW only' AS comparison;
SELECT n.patient_id, n.encounter_id, n.drug_id, n.visit_date
FROM _tr_new_patient_dispensing n
LEFT JOIN _tr_cur_patient_dispensing c
    ON n.patient_id = c.patient_id AND n.encounter_id = c.encounter_id
   AND IFNULL(n.drug_id,0) = IFNULL(c.drug_id,0)
WHERE c.patient_id IS NULL
LIMIT 50;


-- ---- patient_prescription ----

SELECT 'patient_prescription: in CURRENT only' AS comparison;
SELECT c.patient_id, c.drug_id, c.visit_date
FROM _tr_cur_patient_prescription c
LEFT JOIN _tr_new_patient_prescription n
    ON c.patient_id = n.patient_id AND c.drug_id = n.drug_id
   AND c.visit_date = n.visit_date
WHERE n.patient_id IS NULL
LIMIT 50;

SELECT 'patient_prescription: in NEW only' AS comparison;
SELECT n.patient_id, n.drug_id, n.visit_date
FROM _tr_new_patient_prescription n
LEFT JOIN _tr_cur_patient_prescription c
    ON n.patient_id = c.patient_id AND n.drug_id = c.drug_id
   AND n.visit_date = c.visit_date
WHERE c.patient_id IS NULL
LIMIT 50;


-- ---- health_qual_patient_visit ----

SELECT 'health_qual_patient_visit: in CURRENT only' AS comparison;
SELECT c.patient_id, c.encounter_id, c.visit_date
FROM _tr_cur_health_qual c
LEFT JOIN _tr_new_health_qual n
    ON c.patient_id = n.patient_id AND c.encounter_id = n.encounter_id
WHERE n.patient_id IS NULL
LIMIT 50;

SELECT 'health_qual_patient_visit: in NEW only' AS comparison;
SELECT n.patient_id, n.encounter_id, n.visit_date
FROM _tr_new_health_qual n
LEFT JOIN _tr_cur_health_qual c
    ON n.patient_id = c.patient_id AND n.encounter_id = c.encounter_id
WHERE c.patient_id IS NULL
LIMIT 50;


-- ---- patient_laboratory ----

SELECT 'patient_laboratory: in CURRENT only' AS comparison;
SELECT c.patient_id, c.encounter_id, c.test_id, c.visit_date
FROM _tr_cur_patient_laboratory c
LEFT JOIN _tr_new_patient_laboratory n
    ON c.patient_id = n.patient_id AND c.encounter_id = n.encounter_id
   AND c.test_id = n.test_id
WHERE n.patient_id IS NULL
LIMIT 50;

SELECT 'patient_laboratory: in NEW only' AS comparison;
SELECT n.patient_id, n.encounter_id, n.test_id, n.visit_date
FROM _tr_new_patient_laboratory n
LEFT JOIN _tr_cur_patient_laboratory c
    ON n.patient_id = c.patient_id AND n.encounter_id = c.encounter_id
   AND n.test_id = c.test_id
WHERE c.patient_id IS NULL
LIMIT 50;


-- ---- patient_tb_diagnosis ----

SELECT 'patient_tb_diagnosis: in CURRENT only' AS comparison;
SELECT c.patient_id, c.encounter_id, c.visit_date
FROM _tr_cur_patient_tb_diagnosis c
LEFT JOIN _tr_new_patient_tb_diagnosis n
    ON c.patient_id = n.patient_id AND c.encounter_id = n.encounter_id
WHERE n.patient_id IS NULL
LIMIT 50;

SELECT 'patient_tb_diagnosis: in NEW only' AS comparison;
SELECT n.patient_id, n.encounter_id, n.visit_date
FROM _tr_new_patient_tb_diagnosis n
LEFT JOIN _tr_cur_patient_tb_diagnosis c
    ON n.patient_id = c.patient_id AND n.encounter_id = c.encounter_id
WHERE c.patient_id IS NULL
LIMIT 50;


-- ---- patient_nutrition ----

SELECT 'patient_nutrition: in CURRENT only' AS comparison;
SELECT c.patient_id, c.encounter_id, c.visit_date
FROM _tr_cur_patient_nutrition c
LEFT JOIN _tr_new_patient_nutrition n
    ON c.patient_id = n.patient_id AND c.encounter_id = n.encounter_id
WHERE n.patient_id IS NULL
LIMIT 50;

SELECT 'patient_nutrition: in NEW only' AS comparison;
SELECT n.patient_id, n.encounter_id, n.visit_date
FROM _tr_new_patient_nutrition n
LEFT JOIN _tr_cur_patient_nutrition c
    ON n.patient_id = c.patient_id AND n.encounter_id = c.encounter_id
WHERE c.patient_id IS NULL
LIMIT 50;


-- ---- patient_ob_gyn ----

SELECT 'patient_ob_gyn: in CURRENT only' AS comparison;
SELECT c.patient_id, c.encounter_id, c.visit_date
FROM _tr_cur_patient_ob_gyn c
LEFT JOIN _tr_new_patient_ob_gyn n
    ON c.patient_id = n.patient_id AND c.encounter_id = n.encounter_id
WHERE n.patient_id IS NULL
LIMIT 50;

SELECT 'patient_ob_gyn: in NEW only' AS comparison;
SELECT n.patient_id, n.encounter_id, n.visit_date
FROM _tr_new_patient_ob_gyn n
LEFT JOIN _tr_cur_patient_ob_gyn c
    ON n.patient_id = c.patient_id AND n.encounter_id = c.encounter_id
WHERE c.patient_id IS NULL
LIMIT 50;


-- ---- patient_imagerie ----

SELECT 'patient_imagerie: in CURRENT only' AS comparison;
SELECT c.patient_id, c.encounter_id, c.visit_date
FROM _tr_cur_patient_imagerie c
LEFT JOIN _tr_new_patient_imagerie n
    ON c.patient_id = n.patient_id AND c.encounter_id = n.encounter_id
WHERE n.patient_id IS NULL
LIMIT 50;

SELECT 'patient_imagerie: in NEW only' AS comparison;
SELECT n.patient_id, n.encounter_id, n.visit_date
FROM _tr_new_patient_imagerie n
LEFT JOIN _tr_cur_patient_imagerie c
    ON n.patient_id = c.patient_id AND n.encounter_id = c.encounter_id
WHERE c.patient_id IS NULL
LIMIT 50;


-- ---- discontinuation_reason ----

SELECT 'discontinuation_reason: in CURRENT only' AS comparison;
SELECT c.patient_id, c.reason, c.visit_date
FROM _tr_cur_discontinuation_reason c
LEFT JOIN _tr_new_discontinuation_reason n
    ON c.patient_id = n.patient_id AND c.reason = n.reason
WHERE n.patient_id IS NULL
LIMIT 50;

SELECT 'discontinuation_reason: in NEW only' AS comparison;
SELECT n.patient_id, n.reason, n.visit_date
FROM _tr_new_discontinuation_reason n
LEFT JOIN _tr_cur_discontinuation_reason c
    ON n.patient_id = c.patient_id AND n.reason = c.reason
WHERE c.patient_id IS NULL
LIMIT 50;


-- ---- stopping_reason ----

SELECT 'stopping_reason: in CURRENT only' AS comparison;
SELECT c.patient_id, c.reason, c.visit_date
FROM _tr_cur_stopping_reason c
LEFT JOIN _tr_new_stopping_reason n
    ON c.patient_id = n.patient_id AND IFNULL(c.reason,0) = IFNULL(n.reason,0)
WHERE n.patient_id IS NULL
LIMIT 50;

SELECT 'stopping_reason: in NEW only' AS comparison;
SELECT n.patient_id, n.reason, n.visit_date
FROM _tr_new_stopping_reason n
LEFT JOIN _tr_cur_stopping_reason c
    ON n.patient_id = c.patient_id AND IFNULL(n.reason,0) = IFNULL(c.reason,0)
WHERE c.patient_id IS NULL
LIMIT 50;


-- ---- patient_pregnancy ----

SELECT 'patient_pregnancy: in CURRENT only' AS comparison;
SELECT c.patient_id, c.encounter_id, c.start_date
FROM _tr_cur_patient_pregnancy c
LEFT JOIN _tr_new_patient_pregnancy n
    ON c.patient_id = n.patient_id
   AND IFNULL(c.encounter_id,0) = IFNULL(n.encounter_id,0)
   AND c.start_date = n.start_date
WHERE n.patient_id IS NULL
LIMIT 50;

SELECT 'patient_pregnancy: in NEW only' AS comparison;
SELECT n.patient_id, n.encounter_id, n.start_date
FROM _tr_new_patient_pregnancy n
LEFT JOIN _tr_cur_patient_pregnancy c
    ON n.patient_id = c.patient_id
   AND IFNULL(n.encounter_id,0) = IFNULL(c.encounter_id,0)
   AND n.start_date = c.start_date
WHERE c.patient_id IS NULL
LIMIT 50;


-- ---- alert ----

SELECT 'alert: in CURRENT only' AS comparison;
SELECT c.patient_id, c.id_alert, c.encounter_id, c.date_alert
FROM _tr_cur_alert c
LEFT JOIN _tr_new_alert n
    ON c.patient_id = n.patient_id AND c.id_alert = n.id_alert
   AND IFNULL(c.encounter_id,0) = IFNULL(n.encounter_id,0)
WHERE n.patient_id IS NULL
LIMIT 50;

SELECT 'alert: in NEW only' AS comparison;
SELECT n.patient_id, n.id_alert, n.encounter_id, n.date_alert
FROM _tr_new_alert n
LEFT JOIN _tr_cur_alert c
    ON n.patient_id = c.patient_id AND n.id_alert = c.id_alert
   AND IFNULL(n.encounter_id,0) = IFNULL(c.encounter_id,0)
WHERE c.patient_id IS NULL
LIMIT 50;

-- Alert counts by type
SELECT 'ALERT COUNTS BY TYPE' AS comparison;
SELECT
    COALESCE(c.id_alert, n.id_alert) AS alert_type,
    c.current_count,
    n.new_count,
    COALESCE(c.current_count, 0) - COALESCE(n.new_count, 0) AS difference
FROM (
    SELECT id_alert, COUNT(*) AS current_count
    FROM _tr_cur_alert GROUP BY id_alert
) c
LEFT JOIN (
    SELECT id_alert, COUNT(*) AS new_count
    FROM _tr_new_alert GROUP BY id_alert
) n ON c.id_alert = n.id_alert
UNION
SELECT
    n.id_alert, c.current_count, n.new_count,
    COALESCE(c.current_count, 0) - COALESCE(n.new_count, 0)
FROM (
    SELECT id_alert, COUNT(*) AS new_count
    FROM _tr_new_alert GROUP BY id_alert
) n
LEFT JOIN (
    SELECT id_alert, COUNT(*) AS current_count
    FROM _tr_cur_alert GROUP BY id_alert
) c ON n.id_alert = c.id_alert
WHERE c.id_alert IS NULL
ORDER BY alert_type;


-- ---- visit_type ----

SELECT 'visit_type: in CURRENT only' AS comparison;
SELECT c.patient_id, c.encounter_id, c.v_type
FROM _tr_cur_visit_type c
LEFT JOIN _tr_new_visit_type n
    ON c.patient_id = n.patient_id AND c.encounter_id = n.encounter_id
WHERE n.patient_id IS NULL
LIMIT 50;

SELECT 'visit_type: in NEW only' AS comparison;
SELECT n.patient_id, n.encounter_id, n.v_type
FROM _tr_new_visit_type n
LEFT JOIN _tr_cur_visit_type c
    ON n.patient_id = c.patient_id AND n.encounter_id = c.encounter_id
WHERE c.patient_id IS NULL
LIMIT 50;


-- ---- patient_delivery ----

SELECT 'patient_delivery: in CURRENT only' AS comparison;
SELECT c.patient_id, c.encounter_id, c.delivery_location
FROM _tr_cur_patient_delivery c
LEFT JOIN _tr_new_patient_delivery n
    ON c.patient_id = n.patient_id AND c.encounter_id = n.encounter_id
WHERE n.patient_id IS NULL
LIMIT 50;

SELECT 'patient_delivery: in NEW only' AS comparison;
SELECT n.patient_id, n.encounter_id, n.delivery_location
FROM _tr_new_patient_delivery n
LEFT JOIN _tr_cur_patient_delivery c
    ON n.patient_id = c.patient_id AND n.encounter_id = c.encounter_id
WHERE c.patient_id IS NULL
LIMIT 50;


-- ---- virological_tests ----

SELECT 'virological_tests: in CURRENT only' AS comparison;
SELECT c.patient_id, c.encounter_id, c.obs_group_id, c.test_result
FROM _tr_cur_virological_tests c
LEFT JOIN _tr_new_virological_tests n
    ON c.patient_id = n.patient_id AND c.obs_group_id = n.obs_group_id
WHERE n.patient_id IS NULL
LIMIT 50;

SELECT 'virological_tests: in NEW only' AS comparison;
SELECT n.patient_id, n.encounter_id, n.obs_group_id, n.test_result
FROM _tr_new_virological_tests n
LEFT JOIN _tr_cur_virological_tests c
    ON n.patient_id = c.patient_id AND n.obs_group_id = c.obs_group_id
WHERE c.patient_id IS NULL
LIMIT 50;


-- ---- serological_tests ----

SELECT 'serological_tests: in CURRENT only' AS comparison;
SELECT c.patient_id, c.encounter_id, c.obs_group_id, c.test_result
FROM _tr_cur_serological_tests c
LEFT JOIN _tr_new_serological_tests n
    ON c.patient_id = n.patient_id AND c.obs_group_id = n.obs_group_id
WHERE n.patient_id IS NULL
LIMIT 50;

SELECT 'serological_tests: in NEW only' AS comparison;
SELECT n.patient_id, n.encounter_id, n.obs_group_id, n.test_result
FROM _tr_new_serological_tests n
LEFT JOIN _tr_cur_serological_tests c
    ON n.patient_id = c.patient_id AND n.obs_group_id = c.obs_group_id
WHERE c.patient_id IS NULL
LIMIT 50;


-- ---- patient_pcr ----

SELECT 'patient_pcr: in CURRENT only' AS comparison;
SELECT c.patient_id, c.encounter_id, c.pcr_result
FROM _tr_cur_patient_pcr c
LEFT JOIN _tr_new_patient_pcr n
    ON c.patient_id = n.patient_id AND c.encounter_id = n.encounter_id
WHERE n.patient_id IS NULL
LIMIT 50;

SELECT 'patient_pcr: in NEW only' AS comparison;
SELECT n.patient_id, n.encounter_id, n.pcr_result
FROM _tr_new_patient_pcr n
LEFT JOIN _tr_cur_patient_pcr c
    ON n.patient_id = c.patient_id AND n.encounter_id = c.encounter_id
WHERE c.patient_id IS NULL
LIMIT 50;


-- ---- pediatric_hiv_visit ----

SELECT 'pediatric_hiv_visit: in CURRENT only' AS comparison;
SELECT c.patient_id, c.encounter_id, c.ptme, c.actual_vih_status
FROM _tr_cur_pediatric_hiv_visit c
LEFT JOIN _tr_new_pediatric_hiv_visit n
    ON c.patient_id = n.patient_id AND c.encounter_id = n.encounter_id
WHERE n.patient_id IS NULL
LIMIT 50;

SELECT 'pediatric_hiv_visit: in NEW only' AS comparison;
SELECT n.patient_id, n.encounter_id, n.ptme, n.actual_vih_status
FROM _tr_new_pediatric_hiv_visit n
LEFT JOIN _tr_cur_pediatric_hiv_visit c
    ON n.patient_id = c.patient_id AND n.encounter_id = c.encounter_id
WHERE c.patient_id IS NULL
LIMIT 50;


-- ---- patient_menstruation ----

SELECT 'patient_menstruation: in CURRENT only' AS comparison;
SELECT c.patient_id, c.encounter_id, c.ddr
FROM _tr_cur_patient_menstruation c
LEFT JOIN _tr_new_patient_menstruation n
    ON c.patient_id = n.patient_id AND c.encounter_id = n.encounter_id
WHERE n.patient_id IS NULL
LIMIT 50;

SELECT 'patient_menstruation: in NEW only' AS comparison;
SELECT n.patient_id, n.encounter_id, n.ddr
FROM _tr_new_patient_menstruation n
LEFT JOIN _tr_cur_patient_menstruation c
    ON n.patient_id = c.patient_id AND n.encounter_id = c.encounter_id
WHERE c.patient_id IS NULL
LIMIT 50;


-- ---- vih_risk_factor ----

SELECT 'vih_risk_factor: in CURRENT only' AS comparison;
SELECT c.patient_id, c.encounter_id, c.risk_factor
FROM _tr_cur_vih_risk_factor c
LEFT JOIN _tr_new_vih_risk_factor n
    ON c.patient_id = n.patient_id AND c.encounter_id = n.encounter_id
   AND c.risk_factor = n.risk_factor
WHERE n.patient_id IS NULL
LIMIT 50;

SELECT 'vih_risk_factor: in NEW only' AS comparison;
SELECT n.patient_id, n.encounter_id, n.risk_factor
FROM _tr_new_vih_risk_factor n
LEFT JOIN _tr_cur_vih_risk_factor c
    ON n.patient_id = c.patient_id AND n.encounter_id = c.encounter_id
   AND n.risk_factor = c.risk_factor
WHERE c.patient_id IS NULL
LIMIT 50;


-- ---- vaccination ----

SELECT 'vaccination: in CURRENT only' AS comparison;
SELECT c.patient_id, c.encounter_id, c.age_range, c.vaccination_done
FROM _tr_cur_vaccination c
LEFT JOIN _tr_new_vaccination n
    ON c.patient_id = n.patient_id AND c.encounter_id = n.encounter_id
   AND c.age_range <=> n.age_range
WHERE n.patient_id IS NULL
LIMIT 50;

SELECT 'vaccination: in NEW only' AS comparison;
SELECT n.patient_id, n.encounter_id, n.age_range, n.vaccination_done
FROM _tr_new_vaccination n
LEFT JOIN _tr_cur_vaccination c
    ON n.patient_id = c.patient_id AND n.encounter_id = c.encounter_id
   AND n.age_range <=> c.age_range
WHERE c.patient_id IS NULL
LIMIT 50;

-- vaccination_done differences (same row, different result)
SELECT 'vaccination: vaccination_done DIFFERENCES' AS comparison;
SELECT c.patient_id, c.encounter_id, c.age_range,
       c.vaccination_done AS cur_done, n.vaccination_done AS new_done
FROM _tr_cur_vaccination c
INNER JOIN _tr_new_vaccination n
    ON c.patient_id = n.patient_id AND c.encounter_id = n.encounter_id
   AND c.age_range <=> n.age_range
WHERE c.vaccination_done <> n.vaccination_done
LIMIT 50;


-- ---- patient_malaria ----

SELECT 'patient_malaria: in CURRENT only' AS comparison;
SELECT c.patient_id, c.encounter_id, c.visit_date
FROM _tr_cur_patient_malaria c
LEFT JOIN _tr_new_patient_malaria n
    ON c.patient_id = n.patient_id AND c.encounter_id = n.encounter_id
WHERE n.patient_id IS NULL
LIMIT 50;

SELECT 'patient_malaria: in NEW only' AS comparison;
SELECT n.patient_id, n.encounter_id, n.visit_date
FROM _tr_new_patient_malaria n
LEFT JOIN _tr_cur_patient_malaria c
    ON n.patient_id = c.patient_id AND n.encounter_id = c.encounter_id
WHERE c.patient_id IS NULL
LIMIT 50;

-- malaria flag differences
SELECT 'patient_malaria: FLAG DIFFERENCES' AS comparison;
SELECT c.patient_id, c.encounter_id,
       c.fever_for_less_than_2wks     AS cur_fever,      n.fever_for_less_than_2wks     AS new_fever,
       c.suspected_malaria             AS cur_suspected,  n.suspected_malaria             AS new_suspected,
       c.confirmed_malaria             AS cur_confirmed,  n.confirmed_malaria             AS new_confirmed,
       c.microscopic_test              AS cur_micro,      n.microscopic_test              AS new_micro,
       c.rapid_test                    AS cur_rapid,      n.rapid_test                    AS new_rapid
FROM _tr_cur_patient_malaria c
INNER JOIN _tr_new_patient_malaria n
    ON c.patient_id = n.patient_id AND c.encounter_id = n.encounter_id
WHERE c.fever_for_less_than_2wks     <> n.fever_for_less_than_2wks
   OR c.suspected_malaria             <> n.suspected_malaria
   OR c.confirmed_malaria             <> n.confirmed_malaria
   OR c.microscopic_test              <> n.microscopic_test
   OR c.rapid_test                    <> n.rapid_test
LIMIT 50;


-- ---- patient_on_art ----

SELECT 'patient_on_art: in CURRENT only' AS comparison;
SELECT c.patient_id, c.treatment_regime_lines, c.key_population
FROM _tr_cur_patient_on_art c
LEFT JOIN _tr_new_patient_on_art n ON c.patient_id = n.patient_id
WHERE n.patient_id IS NULL
LIMIT 50;

SELECT 'patient_on_art: in NEW only' AS comparison;
SELECT n.patient_id, n.treatment_regime_lines, n.key_population
FROM _tr_new_patient_on_art n
LEFT JOIN _tr_cur_patient_on_art c ON n.patient_id = c.patient_id
WHERE c.patient_id IS NULL
LIMIT 50;

-- patient_on_art column differences
SELECT 'patient_on_art: COLUMN DIFFERENCES' AS comparison;
SELECT c.patient_id,
       c.treatment_regime_lines       AS cur_regime,      n.treatment_regime_lines       AS new_regime,
       c.key_population               AS cur_keypop,      n.key_population               AS new_keypop,
       c.tb_screened                   AS cur_tb_screen,   n.tb_screened                   AS new_tb_screen,
       c.tb_status                    AS cur_tb_status,   n.tb_status                    AS new_tb_status,
       c.breast_feeding               AS cur_bf,          n.breast_feeding               AS new_bf
FROM _tr_cur_patient_on_art c
INNER JOIN _tr_new_patient_on_art n ON c.patient_id = n.patient_id
WHERE IFNULL(c.treatment_regime_lines,'') <> IFNULL(n.treatment_regime_lines,'')
   OR IFNULL(c.key_population,'')         <> IFNULL(n.key_population,'')
   OR IFNULL(c.tb_screened,0)             <> IFNULL(n.tb_screened,0)
   OR IFNULL(c.tb_status,'')             <> IFNULL(n.tb_status,'')
   OR IFNULL(c.breast_feeding,0)          <> IFNULL(n.breast_feeding,0)
   OR IFNULL(c.tested_hiv_postive,0)      <> IFNULL(n.tested_hiv_postive,0)
LIMIT 50;


-- ---- patient_on_arv ----

SELECT 'patient_on_arv: in CURRENT only' AS comparison;
SELECT c.patient_id FROM _tr_cur_patient_on_arv c
LEFT JOIN _tr_new_patient_on_arv n ON c.patient_id = n.patient_id
WHERE n.patient_id IS NULL
LIMIT 50;

SELECT 'patient_on_arv: in NEW only' AS comparison;
SELECT n.patient_id FROM _tr_new_patient_on_arv n
LEFT JOIN _tr_cur_patient_on_arv c ON n.patient_id = c.patient_id
WHERE c.patient_id IS NULL
LIMIT 50;


-- ---- family_planning ----

SELECT 'family_planning: in CURRENT only' AS comparison;
SELECT c.patient_id, c.encounter_id, c.planning
FROM _tr_cur_family_planning c
LEFT JOIN _tr_new_family_planning n
    ON c.patient_id = n.patient_id AND c.encounter_id = n.encounter_id
WHERE n.patient_id IS NULL
LIMIT 50;

SELECT 'family_planning: in NEW only' AS comparison;
SELECT n.patient_id, n.encounter_id, n.planning
FROM _tr_new_family_planning n
LEFT JOIN _tr_cur_family_planning c
    ON n.patient_id = c.patient_id AND n.encounter_id = c.encounter_id
WHERE c.patient_id IS NULL
LIMIT 50;


-- =============================================================================
-- SUMMARY COUNTS
-- =============================================================================

SELECT 'SUMMARY COUNTS' AS comparison;
SELECT 'patient'                    AS table_name, (SELECT COUNT(*) FROM _tr_cur_patient)                AS current_count, (SELECT COUNT(*) FROM _tr_new_patient)                AS new_count
UNION ALL SELECT 'patient_visit',                  (SELECT COUNT(*) FROM _tr_cur_patient_visit),                           (SELECT COUNT(*) FROM _tr_new_patient_visit)
UNION ALL SELECT 'patient_dispensing',             (SELECT COUNT(*) FROM _tr_cur_patient_dispensing),                      (SELECT COUNT(*) FROM _tr_new_patient_dispensing)
UNION ALL SELECT 'patient_prescription',           (SELECT COUNT(*) FROM _tr_cur_patient_prescription),                    (SELECT COUNT(*) FROM _tr_new_patient_prescription)
UNION ALL SELECT 'health_qual_patient_visit',      (SELECT COUNT(*) FROM _tr_cur_health_qual),                             (SELECT COUNT(*) FROM _tr_new_health_qual)
UNION ALL SELECT 'patient_laboratory',             (SELECT COUNT(*) FROM _tr_cur_patient_laboratory),                      (SELECT COUNT(*) FROM _tr_new_patient_laboratory)
UNION ALL SELECT 'patient_tb_diagnosis',           (SELECT COUNT(*) FROM _tr_cur_patient_tb_diagnosis),                    (SELECT COUNT(*) FROM _tr_new_patient_tb_diagnosis)
UNION ALL SELECT 'patient_nutrition',              (SELECT COUNT(*) FROM _tr_cur_patient_nutrition),                       (SELECT COUNT(*) FROM _tr_new_patient_nutrition)
UNION ALL SELECT 'patient_ob_gyn',                 (SELECT COUNT(*) FROM _tr_cur_patient_ob_gyn),                          (SELECT COUNT(*) FROM _tr_new_patient_ob_gyn)
UNION ALL SELECT 'patient_imagerie',               (SELECT COUNT(*) FROM _tr_cur_patient_imagerie),                        (SELECT COUNT(*) FROM _tr_new_patient_imagerie)
UNION ALL SELECT 'discontinuation_reason',         (SELECT COUNT(*) FROM _tr_cur_discontinuation_reason),                  (SELECT COUNT(*) FROM _tr_new_discontinuation_reason)
UNION ALL SELECT 'stopping_reason',                (SELECT COUNT(*) FROM _tr_cur_stopping_reason),                         (SELECT COUNT(*) FROM _tr_new_stopping_reason)
UNION ALL SELECT 'patient_pregnancy',              (SELECT COUNT(*) FROM _tr_cur_patient_pregnancy),                       (SELECT COUNT(*) FROM _tr_new_patient_pregnancy)
UNION ALL SELECT 'alert',                          (SELECT COUNT(*) FROM _tr_cur_alert),                                   (SELECT COUNT(*) FROM _tr_new_alert)
UNION ALL SELECT 'visit_type',                     (SELECT COUNT(*) FROM _tr_cur_visit_type),                              (SELECT COUNT(*) FROM _tr_new_visit_type)
UNION ALL SELECT 'patient_delivery',               (SELECT COUNT(*) FROM _tr_cur_patient_delivery),                        (SELECT COUNT(*) FROM _tr_new_patient_delivery)
UNION ALL SELECT 'virological_tests',              (SELECT COUNT(*) FROM _tr_cur_virological_tests),                       (SELECT COUNT(*) FROM _tr_new_virological_tests)
UNION ALL SELECT 'serological_tests',              (SELECT COUNT(*) FROM _tr_cur_serological_tests),                       (SELECT COUNT(*) FROM _tr_new_serological_tests)
UNION ALL SELECT 'patient_pcr',                    (SELECT COUNT(*) FROM _tr_cur_patient_pcr),                             (SELECT COUNT(*) FROM _tr_new_patient_pcr)
UNION ALL SELECT 'pediatric_hiv_visit',            (SELECT COUNT(*) FROM _tr_cur_pediatric_hiv_visit),                     (SELECT COUNT(*) FROM _tr_new_pediatric_hiv_visit)
UNION ALL SELECT 'patient_menstruation',           (SELECT COUNT(*) FROM _tr_cur_patient_menstruation),                    (SELECT COUNT(*) FROM _tr_new_patient_menstruation)
UNION ALL SELECT 'vih_risk_factor',                (SELECT COUNT(*) FROM _tr_cur_vih_risk_factor),                         (SELECT COUNT(*) FROM _tr_new_vih_risk_factor)
UNION ALL SELECT 'vaccination',                    (SELECT COUNT(*) FROM _tr_cur_vaccination),                             (SELECT COUNT(*) FROM _tr_new_vaccination)
UNION ALL SELECT 'patient_malaria',                (SELECT COUNT(*) FROM _tr_cur_patient_malaria),                         (SELECT COUNT(*) FROM _tr_new_patient_malaria)
UNION ALL SELECT 'patient_on_art',                 (SELECT COUNT(*) FROM _tr_cur_patient_on_art),                          (SELECT COUNT(*) FROM _tr_new_patient_on_art)
UNION ALL SELECT 'patient_on_arv',                 (SELECT COUNT(*) FROM _tr_cur_patient_on_arv),                          (SELECT COUNT(*) FROM _tr_new_patient_on_arv)
UNION ALL SELECT 'family_planning',                (SELECT COUNT(*) FROM _tr_cur_family_planning),                         (SELECT COUNT(*) FROM _tr_new_family_planning);


-- =============================================================================
-- EXECUTION TIMING
-- =============================================================================

SELECT 'EXECUTION TIMING' AS comparison;
SELECT
    TIMESTAMPDIFF(MICROSECOND, @_tr_current_start, @_tr_current_end) / 1000000.0 AS current_seconds,
    TIMESTAMPDIFF(MICROSECOND, @_tr_new_start, @_tr_new_end) / 1000000.0 AS new_seconds,
    TIMESTAMPDIFF(MICROSECOND, @_tr_current_start, @_tr_current_end) / 1000000.0
      - TIMESTAMPDIFF(MICROSECOND, @_tr_new_start, @_tr_new_end) / 1000000.0 AS difference_seconds;


-- =============================================================================
-- CLEANUP (uncomment when done testing)
-- =============================================================================
/*
-- Restore original state
UPDATE patient p
INNER JOIN _tr_bak_patient b ON p.patient_id = b.patient_id
SET p.given_name      = b.given_name,
    p.family_name     = b.family_name,
    p.gender          = b.gender,
    p.birthdate       = b.birthdate,
    p.vih_status      = b.vih_status,
    p.st_id           = b.st_id,
    p.pc_id           = b.pc_id,
    p.national_id     = b.national_id,
    p.identifier      = b.identifier,
    p.isante_id       = b.isante_id,
    p.site_code       = b.site_code,
    p.date_started_arv = b.date_started_arv;

TRUNCATE TABLE patient_visit;
INSERT INTO patient_visit SELECT * FROM _tr_bak_patient_visit;
TRUNCATE TABLE patient_dispensing;
INSERT INTO patient_dispensing SELECT * FROM _tr_bak_patient_dispensing;
TRUNCATE TABLE patient_prescription;
INSERT INTO patient_prescription SELECT * FROM _tr_bak_patient_prescription;
TRUNCATE TABLE health_qual_patient_visit;
INSERT INTO health_qual_patient_visit SELECT * FROM _tr_bak_health_qual;
TRUNCATE TABLE patient_laboratory;
INSERT INTO patient_laboratory SELECT * FROM _tr_bak_patient_laboratory;
TRUNCATE TABLE patient_tb_diagnosis;
INSERT INTO patient_tb_diagnosis SELECT * FROM _tr_bak_patient_tb_diagnosis;
TRUNCATE TABLE patient_nutrition;
INSERT INTO patient_nutrition SELECT * FROM _tr_bak_patient_nutrition;
TRUNCATE TABLE patient_ob_gyn;
INSERT INTO patient_ob_gyn SELECT * FROM _tr_bak_patient_ob_gyn;
TRUNCATE TABLE patient_imagerie;
INSERT INTO patient_imagerie SELECT * FROM _tr_bak_patient_imagerie;
TRUNCATE TABLE discontinuation_reason;
INSERT INTO discontinuation_reason SELECT * FROM _tr_bak_discontinuation_reason;
TRUNCATE TABLE stopping_reason;
INSERT INTO stopping_reason SELECT * FROM _tr_bak_stopping_reason;
TRUNCATE TABLE patient_pregnancy;
INSERT INTO patient_pregnancy SELECT * FROM _tr_bak_patient_pregnancy;
TRUNCATE TABLE alert;
INSERT INTO alert SELECT * FROM _tr_bak_alert;
TRUNCATE TABLE visit_type;
INSERT INTO visit_type SELECT * FROM _tr_bak_visit_type;
TRUNCATE TABLE patient_delivery;
INSERT INTO patient_delivery SELECT * FROM _tr_bak_patient_delivery;
TRUNCATE TABLE virological_tests;
INSERT INTO virological_tests SELECT * FROM _tr_bak_virological_tests;
TRUNCATE TABLE serological_tests;
INSERT INTO serological_tests SELECT * FROM _tr_bak_serological_tests;
TRUNCATE TABLE patient_pcr;
INSERT INTO patient_pcr SELECT * FROM _tr_bak_patient_pcr;
DELETE FROM pediatric_hiv_visit;
INSERT INTO pediatric_hiv_visit SELECT * FROM _tr_bak_pediatric_hiv_visit;
TRUNCATE TABLE patient_menstruation;
INSERT INTO patient_menstruation SELECT * FROM _tr_bak_patient_menstruation;
TRUNCATE TABLE vih_risk_factor;
INSERT INTO vih_risk_factor SELECT * FROM _tr_bak_vih_risk_factor;
TRUNCATE TABLE vaccination;
INSERT INTO vaccination SELECT * FROM _tr_bak_vaccination;
TRUNCATE TABLE patient_malaria;
INSERT INTO patient_malaria SELECT * FROM _tr_bak_patient_malaria;
DELETE FROM patient_on_art;
INSERT INTO patient_on_art SELECT * FROM _tr_bak_patient_on_art;
TRUNCATE TABLE patient_on_arv;
INSERT INTO patient_on_arv SELECT * FROM _tr_bak_patient_on_arv;
TRUNCATE TABLE family_planning;
INSERT INTO family_planning SELECT * FROM _tr_bak_family_planning;

-- Drop all test tables
DROP TABLE IF EXISTS _tr_bak_patient, _tr_bak_patient_visit, _tr_bak_patient_dispensing,
    _tr_bak_patient_prescription, _tr_bak_health_qual, _tr_bak_patient_laboratory,
    _tr_bak_patient_tb_diagnosis, _tr_bak_patient_nutrition, _tr_bak_patient_ob_gyn,
    _tr_bak_patient_imagerie, _tr_bak_discontinuation_reason, _tr_bak_stopping_reason,
    _tr_bak_patient_pregnancy, _tr_bak_alert, _tr_bak_visit_type,
    _tr_bak_patient_delivery, _tr_bak_virological_tests, _tr_bak_serological_tests,
    _tr_bak_patient_pcr, _tr_bak_pediatric_hiv_visit, _tr_bak_patient_menstruation,
    _tr_bak_vih_risk_factor, _tr_bak_vaccination, _tr_bak_patient_malaria,
    _tr_bak_patient_on_art, _tr_bak_patient_on_arv, _tr_bak_family_planning;

DROP TABLE IF EXISTS _tr_cur_patient, _tr_cur_patient_visit, _tr_cur_patient_dispensing,
    _tr_cur_patient_prescription, _tr_cur_health_qual, _tr_cur_patient_laboratory,
    _tr_cur_patient_tb_diagnosis, _tr_cur_patient_nutrition, _tr_cur_patient_ob_gyn,
    _tr_cur_patient_imagerie, _tr_cur_discontinuation_reason, _tr_cur_stopping_reason,
    _tr_cur_patient_pregnancy, _tr_cur_alert, _tr_cur_visit_type,
    _tr_cur_patient_delivery, _tr_cur_virological_tests, _tr_cur_serological_tests,
    _tr_cur_patient_pcr, _tr_cur_pediatric_hiv_visit, _tr_cur_patient_menstruation,
    _tr_cur_vih_risk_factor, _tr_cur_vaccination, _tr_cur_patient_malaria,
    _tr_cur_patient_on_art, _tr_cur_patient_on_arv, _tr_cur_family_planning;

DROP TABLE IF EXISTS _tr_new_patient, _tr_new_patient_visit, _tr_new_patient_dispensing,
    _tr_new_patient_prescription, _tr_new_health_qual, _tr_new_patient_laboratory,
    _tr_new_patient_tb_diagnosis, _tr_new_patient_nutrition, _tr_new_patient_ob_gyn,
    _tr_new_patient_imagerie, _tr_new_discontinuation_reason, _tr_new_stopping_reason,
    _tr_new_patient_pregnancy, _tr_new_alert, _tr_new_visit_type,
    _tr_new_patient_delivery, _tr_new_virological_tests, _tr_new_serological_tests,
    _tr_new_patient_pcr, _tr_new_pediatric_hiv_visit, _tr_new_patient_menstruation,
    _tr_new_vih_risk_factor, _tr_new_vaccination, _tr_new_patient_malaria,
    _tr_new_patient_on_art, _tr_new_patient_on_arv, _tr_new_family_planning;

-- Drop the test procedures
DROP PROCEDURE IF EXISTS _test_reports_current;
DROP PROCEDURE IF EXISTS _test_reports_new;
*/
