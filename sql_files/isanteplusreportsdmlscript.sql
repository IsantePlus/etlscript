USE isanteplus;

-- =============================================================================
-- PRÉAMBULE : Vérifier si la colonne pc_id existe déjà
-- =============================================================================

SET @col_exists := (
    SELECT COUNT(*)
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = 'isanteplus'
      AND TABLE_NAME = 'patient'
      AND COLUMN_NAME = 'pc_id'
);

-- Construire la requête dynamiquement
SET @sql := IF(
    @col_exists = 0,
    'ALTER TABLE isanteplus.patient ADD COLUMN pc_id VARCHAR(50);',
    'SELECT ''Column pc_id already exists'';'
);

PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

-- =============================================================================
-- PHASE 0 : RÉSOLUTION DES UUID EN VARIABLES DE SESSION
-- Lectures rapides sur de petites tables de référence
-- =============================================================================

SET SQL_SAFE_UPDATES = 0;

-- ---- Types de consultation (encounter_type) ----
SET @et_first_hiv_visit := (SELECT encounter_type_id FROM openmrs.encounter_type WHERE uuid = '17536ba6-dd7c-4f58-8014-08c7cb798ac7');
SET @et_followup_hiv_visit := (SELECT encounter_type_id FROM openmrs.encounter_type WHERE uuid = '204ad066-c5c2-4229-9a62-644bc5617ca2');
SET @et_ped_first_hiv_visit := (SELECT encounter_type_id FROM openmrs.encounter_type WHERE uuid = '349ae0b4-65c1-4122-aa06-480f186c8350');
SET @et_ped_followup_hiv_visit := (SELECT encounter_type_id FROM openmrs.encounter_type WHERE uuid = '33491314-c352-42d0-bd5d-a9d0bffc9bf1');
SET @et_lab := (SELECT encounter_type_id FROM openmrs.encounter_type WHERE uuid = 'f037e97b-471e-4898-a07c-b8e169e0ddc4');
SET @et_discontinuation := (SELECT encounter_type_id FROM openmrs.encounter_type WHERE uuid = '9d0113c6-f23a-4461-8428-7e9a7344f2ba');
SET @et_obgyn_initial := (SELECT encounter_type_id FROM openmrs.encounter_type WHERE uuid = '5c312603-25c1-4dbe-be18-1a167eb85f97');
SET @et_obgyn_followup := (SELECT encounter_type_id FROM openmrs.encounter_type WHERE uuid = '49592bec-dd22-4b6c-a97f-4dd2af6f2171');
SET @et_labor_delivery := (SELECT encounter_type_id FROM openmrs.encounter_type WHERE uuid = 'd95b3540-a39f-4d1e-a301-8ee0e03d5eab');
SET @et_adult_initial := (SELECT encounter_type_id FROM openmrs.encounter_type WHERE uuid = '12f4d7c3-e047-4455-a607-47a40fe32460');
SET @et_adult_followup := (SELECT encounter_type_id FROM openmrs.encounter_type WHERE uuid = 'a5600919-4dde-4eb8-a45b-05c204af8284');
SET @et_ped_initial := (SELECT encounter_type_id FROM openmrs.encounter_type WHERE uuid = '709610ff-5e39-4a47-9c27-a60e740b0944');
SET @et_ped_followup := (SELECT encounter_type_id FROM openmrs.encounter_type WHERE uuid = 'fdb5b14f-555f-4282-b4c1-9286addf0aae');
SET @et_imagerie := (SELECT encounter_type_id FROM openmrs.encounter_type WHERE uuid = 'a4cab59f-f0ce-46c3-bd76-416db36ec719');

-- ---- Types d'identifiant patient (patient_identifier_type) ----
SET @pit_st_code := (SELECT patient_identifier_type_id FROM openmrs.patient_identifier_type WHERE uuid = 'd059f6d0-9e42-4760-8de1-8316b48bc5f1');
SET @pit_pc_code := (SELECT patient_identifier_type_id FROM openmrs.patient_identifier_type WHERE uuid = 'b7a154fd-0097-4071-ac09-af11ee7e0310');
SET @pit_national_id := (SELECT patient_identifier_type_id FROM openmrs.patient_identifier_type WHERE uuid = '9fb4533d-4fd5-4276-875b-2ab41597f5dd');
SET @pit_isanteplus_id := (SELECT patient_identifier_type_id FROM openmrs.patient_identifier_type WHERE uuid = '05a29f94-c0ed-11e2-94be-8c13b969e334');
SET @pit_isante_id := (SELECT patient_identifier_type_id FROM openmrs.patient_identifier_type WHERE uuid = '0e0c7cc2-3491-4675-b705-746e372ff346');

-- ---- Types d'attribut de personne (person_attribute_type) ----
SET @pat_birthplace := (SELECT person_attribute_type_id FROM openmrs.person_attribute_type WHERE uuid = '8d8718c2-c2cc-11de-8d13-0010c6dffd0f');
SET @pat_telephone := (SELECT person_attribute_type_id FROM openmrs.person_attribute_type WHERE uuid = '14d4f066-15f5-102d-96e4-000c29c2a5d7');
SET @pat_mother_name := (SELECT person_attribute_type_id FROM openmrs.person_attribute_type WHERE uuid = '8d871d18-c2cc-11de-8d13-0010c6dffd0f');

-- ---- Type d'attribut de lieu (location_attribute_type) ----
SET @lat_isante_site := (SELECT location_attribute_type_id FROM openmrs.location_attribute_type WHERE uuid = '0e52924e-4ebb-40ba-9b83-b198b532653b');

-- ---- Concepts ----
SET @concept_ddp := (SELECT concept_id FROM openmrs.concept WHERE uuid = 'c2aacdc8-156e-4527-8934-a8fb94162419');
SET @concept_date_premiers_soins := (SELECT concept_id FROM openmrs.concept WHERE uuid = 'd9885523-a923-474b-88df-f3294d422c3c');
SET @concept_tb_diag_group := (SELECT concept_id FROM openmrs.concept WHERE uuid = '30d2b9eb-0a2f-4b0a-9ae9-31476ec13ed6');
SET @concept_mdr_tb_diag_group := (SELECT concept_id FROM openmrs.concept WHERE uuid = 'b148cd09-496d-4a97-8cd5-75500f2d684f');
SET @concept_posology_alt := (SELECT concept_id FROM openmrs.concept WHERE uuid = 'ca8bc9c3-7f97-450a-8f33-e98f776b90e1');

-- Groupes de diagnostic de grossesse
SET @concept_preg_grp_1 := (SELECT concept_id FROM openmrs.concept WHERE uuid = '3fea18d4-88f1-40c1-aadc-41dca3449f9d');
SET @concept_preg_grp_2 := (SELECT concept_id FROM openmrs.concept WHERE uuid = '73da2a29-a035-41b5-8891-717ba99a3081');
SET @concept_preg_grp_3 := (SELECT concept_id FROM openmrs.concept WHERE uuid = '361bd482-59a9-4ee8-80f0-e7e39b1d1827');
SET @concept_preg_grp_4 := (SELECT concept_id FROM openmrs.concept WHERE uuid = 'fd7987b1-d551-4451-b8e2-59a998adf1d5');
SET @concept_preg_grp_5 := (SELECT concept_id FROM openmrs.concept WHERE uuid = 'ee6c7fd3-6a2f-4af0-8978-e1c5e06a9a62');
SET @concept_preg_grp_6 := (SELECT concept_id FROM openmrs.concept WHERE uuid = '6e639f6c-1b62-41c4-8cfd-fb76b3205313');
SET @concept_preg_grp_7 := (SELECT concept_id FROM openmrs.concept WHERE uuid = 'f9d52515-6c56-41b3-881a-1b40f355144c');
SET @concept_preg_grp_8 := (SELECT concept_id FROM openmrs.concept WHERE uuid = '1dfb560d-6627-441e-a8e2-d1517b51c8b4');
SET @concept_preg_grp_9 := (SELECT concept_id FROM openmrs.concept WHERE uuid = '756f00e4-b1b6-40cd-b5ab-d5cce8a571fb');
SET @concept_preg_grp_10 := (SELECT concept_id FROM openmrs.concept WHERE uuid = '22be1344-65f9-4310-9be3-1d300e57820b');
SET @concept_preg_grp_11 := (SELECT concept_id FROM openmrs.concept WHERE uuid = 'cb4d6c75-c218-4a26-9046-41e0939e55c4');

-- Groupes de tests virologiques
SET @concept_viro_grp_1 := (SELECT concept_id FROM openmrs.concept WHERE uuid = 'eaa7f684-1473-4f59-acb4-686bada87846');
SET @concept_viro_grp_2 := (SELECT concept_id FROM openmrs.concept WHERE uuid = '9a05c0d5-2c03-4c3a-a810-6bc513ae7ee7');
SET @concept_viro_grp_3 := (SELECT concept_id FROM openmrs.concept WHERE uuid = '535b63e9-0773-4f4e-94af-69ff8f412411');

-- Groupes de tests sérologiques
SET @concept_sero_grp_1 := (SELECT concept_id FROM openmrs.concept WHERE uuid = '28e8ffc8-1b65-484c-baa1-929f0b8901a6');
SET @concept_sero_grp_2 := (SELECT concept_id FROM openmrs.concept WHERE uuid = '6e3aa01c-8a70-42b6-94fe-6ac465b620d9');
SET @concept_sero_grp_3 := (SELECT concept_id FROM openmrs.concept WHERE uuid = '2a66236f-d84b-4cc8-a552-15b12238e7ea');
SET @concept_sero_grp_4 := (SELECT concept_id FROM openmrs.concept WHERE uuid = '121d7ed6-c039-465d-9663-4ab631232ba9');
SET @concept_sero_grp_5 := (SELECT concept_id FROM openmrs.concept WHERE uuid = 'ec6e3a54-3e4b-4647-b9bd-baf0d06a98d2');
SET @concept_sero_grp_6 := (SELECT concept_id FROM openmrs.concept WHERE uuid = '99f7b98e-8900-4898-9772-a88f4783babd');

-- Concepts patient_on_art
SET @concept_breast_feeding := (SELECT concept_id FROM openmrs.concept WHERE uuid = '7e0f24aa-4f8e-42d0-8649-282bc3c867e3');
SET @concept_key_population := (SELECT concept_id FROM openmrs.concept WHERE uuid = 'b2726cc7-df4b-463c-919d-1c7a600fef87');
SET @concept_first_line_regimen := (SELECT concept_id FROM openmrs.concept WHERE uuid = 'dd69cffe-d7b8-4cf1-bc11-3ac302763d48');
SET @concept_second_line_regimen := (SELECT concept_id FROM openmrs.concept WHERE uuid = '77488a7b-957f-4ebc-892a-e53e7c910363');
SET @concept_third_line_regimen := (SELECT concept_id FROM openmrs.concept WHERE uuid = '99d88c3e-00ad-4122-a300-a88ff5c125c9');
SET @concept_genexpert := (SELECT concept_id FROM openmrs.concept WHERE uuid = '4cbdc90a-e007-4a48-af54-5dd204edadd9');
SET @concept_viral_load_type := (SELECT concept_id FROM openmrs.concept WHERE uuid = '6b41328f-48bc-497c-8977-283feaa9cea6');
SET @concept_viral_load_targeted := (SELECT concept_id FROM openmrs.concept WHERE uuid = '5c4fb18a-70f1-4a0b-924c-0b595d7dbb90');
SET @concept_viral_load_routine := (SELECT concept_id FROM openmrs.concept WHERE uuid = '71e6fd5c-1544-4c9d-a452-32fdba8efc82');
SET @concept_tb_bact_pos_1 := (SELECT concept_id FROM openmrs.concept WHERE uuid = '36d6616b-8c7c-4768-9f38-2be4b704fccd');
SET @concept_tb_bact_pos_2 := (SELECT concept_id FROM openmrs.concept WHERE uuid = 'f4ee3bcc-947c-4390-9190-a335c2cd5868');

-- =============================================================================
-- SNAPSHOT : Copie des tables openmrs dans des tables temporaires
-- Réduit la contention de verrouillage sur les tables de production
-- =============================================================================

DROP TEMPORARY TABLE IF EXISTS _tmp_obs;
CREATE TEMPORARY TABLE _tmp_obs AS
SELECT * FROM openmrs.obs;

DROP TEMPORARY TABLE IF EXISTS _tmp_encounter;
CREATE TEMPORARY TABLE _tmp_encounter AS
SELECT * FROM openmrs.encounter;

DROP TEMPORARY TABLE IF EXISTS _tmp_visit;
CREATE TEMPORARY TABLE _tmp_visit AS
SELECT * FROM openmrs.visit;

DROP TEMPORARY TABLE IF EXISTS _tmp_encounter_provider;
CREATE TEMPORARY TABLE _tmp_encounter_provider AS
SELECT * FROM openmrs.encounter_provider;

DROP TEMPORARY TABLE IF EXISTS _tmp_person;
CREATE TEMPORARY TABLE _tmp_person AS
SELECT * FROM openmrs.person;

DROP TEMPORARY TABLE IF EXISTS _tmp_patient;
CREATE TEMPORARY TABLE _tmp_patient AS
SELECT * FROM openmrs.patient;

DROP TEMPORARY TABLE IF EXISTS _tmp_person_attribute;
CREATE TEMPORARY TABLE _tmp_person_attribute AS
SELECT * FROM openmrs.person_attribute;

-- Index sur les tables temporaires pour optimiser les jointures
ALTER TABLE _tmp_obs ADD INDEX idx_obs_encounter_id (encounter_id);
ALTER TABLE _tmp_obs ADD INDEX idx_obs_person_id (person_id);
ALTER TABLE _tmp_obs ADD INDEX idx_obs_concept_id (concept_id);
ALTER TABLE _tmp_obs ADD INDEX idx_obs_obs_group_id (obs_group_id);
ALTER TABLE _tmp_obs ADD INDEX idx_obs_obs_id (obs_id);

DROP TEMPORARY TABLE IF EXISTS _tmp_obs_grp;
CREATE TEMPORARY TABLE _tmp_obs_grp AS
SELECT * FROM openmrs.obs;
ALTER TABLE _tmp_obs_grp ADD INDEX idx_obs_grp_encounter_id (encounter_id);
ALTER TABLE _tmp_obs_grp ADD INDEX idx_obs_grp_person_id (person_id);
ALTER TABLE _tmp_obs_grp ADD INDEX idx_obs_grp_concept_id (concept_id);
ALTER TABLE _tmp_obs_grp ADD INDEX idx_obs_grp_obs_group_id (obs_group_id);
ALTER TABLE _tmp_obs_grp ADD INDEX idx_obs_grp_obs_id (obs_id);

DROP TEMPORARY TABLE IF EXISTS _tmp_obs_sib;
CREATE TEMPORARY TABLE _tmp_obs_sib AS
SELECT * FROM openmrs.obs
WHERE obs_group_id IS NOT NULL;
ALTER TABLE _tmp_obs_sib ADD INDEX idx_obs_sib_encounter_id (encounter_id);
ALTER TABLE _tmp_obs_sib ADD INDEX idx_obs_sib_person_id (person_id);
ALTER TABLE _tmp_obs_sib ADD INDEX idx_obs_sib_concept_id (concept_id);
ALTER TABLE _tmp_obs_sib ADD INDEX idx_obs_sib_obs_group_id (obs_group_id);
ALTER TABLE _tmp_obs_sib ADD INDEX idx_obs_sib_obs_id (obs_id);

ALTER TABLE _tmp_encounter ADD PRIMARY KEY (encounter_id);
ALTER TABLE _tmp_encounter ADD INDEX idx_enc_patient_id (patient_id);
ALTER TABLE _tmp_encounter ADD INDEX idx_enc_visit_id (visit_id);
ALTER TABLE _tmp_encounter ADD INDEX idx_enc_encounter_type (encounter_type);

DROP TEMPORARY TABLE IF EXISTS _tmp_encounter_2;
CREATE TEMPORARY TABLE _tmp_encounter_2 AS
SELECT * FROM openmrs.encounter;
ALTER TABLE _tmp_encounter_2 ADD PRIMARY KEY (encounter_id);
ALTER TABLE _tmp_encounter_2 ADD INDEX idx_enc2_patient_id (patient_id);
ALTER TABLE _tmp_encounter_2 ADD INDEX idx_enc2_visit_id (visit_id);
ALTER TABLE _tmp_encounter_2 ADD INDEX idx_enc2_encounter_type (encounter_type);

ALTER TABLE _tmp_visit ADD INDEX idx_vis_visit_id (visit_id);
ALTER TABLE _tmp_visit ADD INDEX idx_vis_patient_id (patient_id);

DROP TEMPORARY TABLE IF EXISTS _tmp_visit_2;
CREATE TEMPORARY TABLE _tmp_visit_2 AS
SELECT * FROM openmrs.visit;
ALTER TABLE _tmp_visit_2 ADD INDEX idx_vis2_visit_id (visit_id);
ALTER TABLE _tmp_visit_2 ADD INDEX idx_vis2_patient_id (patient_id);


ALTER TABLE _tmp_encounter_provider ADD INDEX idx_ep_encounter_id (encounter_id);
ALTER TABLE _tmp_person ADD INDEX idx_per_person_id (person_id);
ALTER TABLE _tmp_patient ADD INDEX idx_pat_patient_id (patient_id);
ALTER TABLE _tmp_person_attribute ADD INDEX idx_pattr_person_id (person_id);

-- =============================================================================
-- SECTION 1 : Données démographiques des patients (patient)
-- =============================================================================

/* Location table DDL - outside transaction (causes implicit commit) */
CREATE TABLE IF NOT EXISTS location(
  name text,
  location_id INT(11),
  isante_location_id text,
  CONSTRAINT pk_reports_location PRIMARY KEY(location_id)
)ENGINE = InnoDB DEFAULT CHARSET = utf8;

START TRANSACTION;

/* insert data to patient table */
INSERT INTO patient
(
  patient_id,
  given_name,
  family_name,
  gender,
  birthdate,
  creator,
  date_created,
  last_inserted_date,
  last_updated_date,
  voided
)
SELECT pn.person_id,
  pn.given_name,
  pn.family_name,
  pe.gender,
  pe.birthdate,
  pn.creator,
  pn.date_created,
  now() as last_inserted_date,
  now() as last_updated_date,
  pn.voided
FROM openmrs.person_name pn
INNER JOIN _tmp_person pe ON pe.person_id = pn.person_id
INNER JOIN _tmp_patient pa ON pe.person_id = pa.patient_id
ON DUPLICATE KEY UPDATE
  given_name = pn.given_name,
  family_name = pn.family_name,
  gender = pe.gender,
  birthdate = pe.birthdate,
  creator = pn.creator,
  date_created = pn.date_created,
  last_updated_date = now(),
  voided = pn.voided;

/*ST CODE*/
UPDATE patient p
INNER JOIN openmrs.patient_identifier pi ON p.patient_id = pi.patient_id
SET p.st_id = pi.identifier
WHERE pi.identifier_type = @pit_st_code
AND pi.voided = 0;

/*PC CODE*/
UPDATE patient p
INNER JOIN openmrs.patient_identifier pi ON p.patient_id = pi.patient_id
SET p.pc_id = pi.identifier
WHERE pi.identifier_type = @pit_pc_code
AND pi.voided = 0;

/*National ID*/
UPDATE patient p
INNER JOIN openmrs.patient_identifier pi ON p.patient_id = pi.patient_id
SET p.national_id = pi.identifier
WHERE pi.identifier_type = @pit_national_id;

/*iSantePlus_ID*/
UPDATE patient p
INNER JOIN openmrs.patient_identifier pi ON p.patient_id = pi.patient_id
SET p.identifier = pi.identifier
WHERE pi.identifier_type = @pit_isanteplus_id;

/*isante_id*/
UPDATE patient p
INNER JOIN openmrs.patient_identifier pi ON p.patient_id = pi.patient_id
SET p.isante_id = pi.identifier
WHERE pi.identifier_type = @pit_isante_id;

/* update location_id for patients*/
UPDATE patient p
INNER JOIN (
  SELECT DISTINCT pid.patient_id, pid.location_id
  FROM openmrs.patient_identifier pid
  WHERE pid.identifier_type = @pit_isanteplus_id
) pi ON p.patient_id = pi.patient_id
SET p.location_id = pi.location_id
WHERE pi.location_id IS NOT NULL;

/*Adding iSante Site_code to the patient table*/
INSERT INTO location (name, location_id, isante_location_id)
SELECT DISTINCT l.name, l.location_id, la.value_reference
FROM openmrs.location l
INNER JOIN openmrs.location_attribute la ON l.location_id = la.location_id
WHERE la.attribute_type_id = @lat_isante_site
ON DUPLICATE KEY UPDATE
  name = l.name,
  isante_location_id = la.value_reference;

UPDATE isanteplus.patient p
INNER JOIN isanteplus.location l ON p.location_id = l.location_id
SET site_code = l.isante_location_id;

/*update patient with address*/
UPDATE patient p
INNER JOIN openmrs.person_address padd ON p.patient_id = padd.person_id
SET p.last_address=
CASE WHEN ((padd.address1 <> '' AND padd.address1 is not null)
  AND (padd.address2 <> '' AND padd.address2 is not null)
)
  THEN CONCAT(padd.address1,' ',padd.address2)
WHEN ((padd.address1 <> '' AND padd.address1 is not null)
  AND (padd.address2 = '' OR padd.address2 is null)
)
  THEN padd.address1 ELSE padd.address2
END;

/*Update for birthPlace*/
UPDATE patient p
INNER JOIN _tmp_person_attribute pa ON p.patient_id = pa.person_id
SET p.place_of_birth = pa.value
WHERE pa.person_attribute_type_id = @pat_birthplace;

/*Update for telephone*/
UPDATE patient p
INNER JOIN _tmp_person_attribute pa ON p.patient_id = pa.person_id
SET p.telephone = pa.value
WHERE pa.person_attribute_type_id = @pat_telephone;

/*Update for mother's Name*/
UPDATE patient p
INNER JOIN _tmp_person_attribute pa ON p.patient_id = pa.person_id
SET p.mother_name = pa.value
WHERE pa.person_attribute_type_id = @pat_mother_name;

/*Update for Civil Status */
DROP TABLE IF EXISTS patient_obs_temp;
CREATE TEMPORARY TABLE patient_obs_temp
SELECT person_id, MAX(obs_datetime) AS obsDt, value_coded
FROM _tmp_obs WHERE concept_id = 1054
GROUP BY person_id;

UPDATE patient p
INNER JOIN patient_obs_temp po ON p.patient_id = po.person_id
SET p.maritalStatus = po.value_coded;

/*Update for Occupation */
DROP TABLE IF EXISTS patient_obs_temp;
CREATE TEMPORARY TABLE patient_obs_temp
SELECT person_id, MAX(obs_datetime) AS obsDt, value_coded
FROM _tmp_obs WHERE concept_id = 1542
GROUP BY person_id;

UPDATE patient p
INNER JOIN patient_obs_temp po ON p.patient_id = po.person_id
SET p.occupation = po.value_coded;

/*Update for Contact Name*/
UPDATE patient p
INNER JOIN _tmp_obs o ON p.patient_id = o.person_id
INNER JOIN _tmp_obs_grp ob ON o.person_id = ob.person_id
    AND o.obs_group_id = ob.obs_id
SET p.contact_name = o.value_text
WHERE o.concept_id = 163258
AND ob.concept_id = 165210
AND (o.value_text is not null AND o.value_text <> '');

/* update patient with vih_status when patient has a HIV form*/
UPDATE patient p
INNER JOIN _tmp_encounter en ON p.patient_id = en.patient_id
SET p.vih_status = 1
WHERE en.encounter_type IN (@et_first_hiv_visit, @et_followup_hiv_visit, @et_ped_first_hiv_visit, @et_ped_followup_hiv_visit)
AND en.voided = 0;

/*Update vih_status WHEN patient has a laboratory form WITH HIV test positive*/
UPDATE patient p
INNER JOIN _tmp_encounter en ON p.patient_id = en.patient_id
INNER JOIN _tmp_obs o ON en.encounter_id = o.encounter_id
    AND en.patient_id = o.person_id
SET p.vih_status = 1
WHERE en.encounter_type = @et_lab
AND o.concept_id IN (1040,1042)
AND o.value_coded = 703
AND en.voided = 0
AND o.voided = 0;

/*Update patient table for having first visit date */
UPDATE patient p
INNER JOIN _tmp_visit vi ON p.patient_id = vi.patient_id
INNER JOIN (
  SELECT v.patient_id, MIN(v.date_started) as date_started
  FROM _tmp_visit_2 v
  GROUP BY v.patient_id
) B ON vi.patient_id = B.patient_id
    AND vi.date_started = B.date_started
SET p.first_visit_date = vi.date_started
WHERE vi.voided = 0;

/*Update patient table for having last visit date */
UPDATE patient p
INNER JOIN _tmp_visit vi ON p.patient_id = vi.patient_id
INNER JOIN (
  SELECT v.patient_id, MAX(v.date_started) as date_started
  FROM _tmp_visit_2 v
  GROUP BY v.patient_id
) B ON vi.patient_id = B.patient_id
    AND vi.date_started = B.date_started
SET p.last_visit_date = vi.date_started
WHERE vi.voided = 0;

/*Update next_visit_date on table patient*/
DROP TABLE IF EXISTS patient_obs_temp;
CREATE TEMPORARY TABLE patient_obs_temp
SELECT person_id, MAX(value_datetime) AS obsDt, value_coded
FROM _tmp_obs WHERE concept_id IN(5096,162549) AND voided = 0
AND value_datetime IS NOT NULL
GROUP BY person_id;

UPDATE patient p
INNER JOIN patient_obs_temp po ON p.patient_id = po.person_id
SET p.next_visit_date = DATE(po.obsDt);

/*Update for date_started_arv area in patient table */
DROP TABLE IF EXISTS patient_obs_temp;
CREATE TEMPORARY TABLE patient_obs_temp
SELECT o.person_id, MIN(o.obs_datetime) AS obsDt, o.value_coded
FROM _tmp_obs_grp ob
  JOIN _tmp_obs o ON ob.obs_id = o.obs_group_id
  JOIN _tmp_obs_sib ob2 ON o.obs_group_id = ob2.obs_group_id
  JOIN isanteplus.arv_drugs darv ON o.value_coded = darv.drug_id
WHERE
  ob.concept_id = 163711
AND o.concept_id = 1282
AND ob2.concept_id IN (1276, 1444, 159368, 1443)
AND o.voided = 0
GROUP BY
  o.person_id;

UPDATE patient p
INNER JOIN patient_obs_temp po ON p.patient_id = po.person_id
SET p.date_started_arv = po.obsDt;

DROP TABLE IF EXISTS patient_obs_temp;

UPDATE patient p
INNER JOIN _tmp_obs o ON p.patient_id = o.person_id
SET p.transferred_in = 1
WHERE o.concept_id = 159936
AND o.value_coded = 5622;

/*Date des premiers soins dans cet établissement*/
UPDATE patient p
INNER JOIN _tmp_obs o ON p.patient_id = o.person_id
SET p.date_transferred_in = o.value_datetime
WHERE o.concept_id = @concept_date_premiers_soins
AND o.value_datetime IS NOT NULL;

/*Date début des ARV dans l'établissement de référence*/
UPDATE patient p
INNER JOIN _tmp_obs o ON p.patient_id = o.person_id
SET p.date_started_arv_other_site = o.value_datetime
WHERE o.concept_id = 159599
AND o.value_datetime IS NOT NULL;

COMMIT;

-- =============================================================================
-- SECTION 2 : Visites des patients (patient_visit)
-- =============================================================================
START TRANSACTION;

INSERT INTO patient_visit
  (visit_date,visit_id,encounter_id,location_id,
  patient_id,start_date,stop_date,creator,
  encounter_type,form_id,next_visit_date,
  last_insert_date, last_updated_date, voided
)
SELECT v.date_started AS visit_date,
  v.visit_id,e.encounter_id,v.location_id,
  v.patient_id,v.date_started,v.date_stopped,
  v.creator,e.encounter_type,e.form_id,o.value_datetime,
  NOW() AS last_inserted_date, NOW() AS last_updated_date, v.voided
FROM _tmp_visit v
INNER JOIN _tmp_encounter e ON v.visit_id = e.visit_id
    AND v.patient_id = e.patient_id
INNER JOIN _tmp_obs o ON o.person_id = e.patient_id
    AND o.encounter_id = e.encounter_id
WHERE o.concept_id = '5096'
AND o.voided = 0
ON DUPLICATE KEY UPDATE
  next_visit_date = o.value_datetime,
  last_updated_date = NOW(),
  voided = v.voided;

COMMIT;

-- =============================================================================
-- SECTION 3 : Dispensation (patient_dispensing, patient_on_arv)
-- =============================================================================
START TRANSACTION;

/*Insert for patient_id,encounter_id, drug_id areas*/
INSERT INTO patient_dispensing
(
  patient_id,
  encounter_id,
  location_id,
  obs_id,
  obs_group_id,
  drug_id,
  dispensation_date,
  last_updated_date,
  voided
)
SELECT DISTINCT ob.person_id,
  ob.encounter_id,ob.location_id,ob.obs_id, ob.obs_group_id,
  ob.value_coded,ob2.obs_datetime, now(), ob.voided
FROM _tmp_obs ob
INNER JOIN _tmp_obs_grp ob1 ON ob.person_id = ob1.person_id
    AND ob.encounter_id = ob1.encounter_id
    AND ob.obs_group_id = ob1.obs_id
INNER JOIN _tmp_obs_sib ob2 ON ob1.obs_id = ob2.obs_group_id
WHERE ob1.concept_id = 163711
AND ob.concept_id = 1282
AND ob2.concept_id IN(1444,159368,1443,1276)
ON DUPLICATE KEY UPDATE
  obs_id = ob.obs_id,
  obs_group_id = ob.obs_group_id,
  dispensation_date = ob2.obs_datetime,
  last_updated_date = NOW(),
  voided = ob.voided;

/*update dispensation_date for table patient_dispensing */
UPDATE patient_dispensing patdisp
INNER JOIN _tmp_obs ob ON patdisp.encounter_id = ob.encounter_id
INNER JOIN _tmp_obs_grp o ON ob.obs_group_id = o.obs_group_id
    AND patdisp.drug_id = o.value_coded
SET patdisp.dispensation_date = DATE(ob.obs_datetime)
WHERE o.concept_id = 1282
AND ob.concept_id = 1276
AND ob.voided = 0;

/*update next_dispensation_date for table patient_dispensing*/
UPDATE patient_dispensing patdisp
INNER JOIN _tmp_obs ob ON patdisp.encounter_id = ob.encounter_id
SET patdisp.next_dispensation_date = DATE(ob.value_datetime)
WHERE ob.concept_id = 162549
AND ob.voided = 0;

/*update provider for patient_dispensing*/
UPDATE patient_dispensing padisp
INNER JOIN _tmp_encounter_provider enp ON padisp.encounter_id = enp.encounter_id
SET padisp.provider_id = enp.provider_id
WHERE enp.voided = 0;

/*Update dose_day, pill_amount for patient_dispensing*/
UPDATE isanteplus.patient_dispensing patdisp
INNER JOIN _tmp_obs ob ON patdisp.encounter_id = ob.encounter_id
INNER JOIN _tmp_obs_grp ob1 ON ob.encounter_id = ob1.encounter_id
    AND ob.obs_group_id = ob1.obs_id
SET patdisp.dose_day = ob.value_numeric
WHERE ob1.concept_id = 163711
AND ob.concept_id = 159368
AND ob.voided = 0;

/*Update pill_amount for patient_dispensing*/
UPDATE isanteplus.patient_dispensing patdisp
INNER JOIN _tmp_obs ob ON patdisp.encounter_id = ob.encounter_id
INNER JOIN _tmp_obs_grp ob1 ON ob.encounter_id = ob1.encounter_id
    AND ob.obs_group_id = ob1.obs_id
SET patdisp.pills_amount = ob.value_numeric
WHERE ob1.concept_id = 163711
AND ob.concept_id = 1443
AND ob.voided = 0;

/*update visit_id, visit_date for table patient_dispensing*/
UPDATE patient_dispensing patdisp
INNER JOIN _tmp_encounter en ON patdisp.encounter_id = en.encounter_id
INNER JOIN _tmp_visit vi ON en.visit_id = vi.visit_id
SET patdisp.visit_id = vi.visit_id, patdisp.visit_date = vi.date_started;

/*update dispensation_location Dispensation communautaire=1755 for table patient_dispensing*/
UPDATE patient_dispensing patdisp
INNER JOIN _tmp_obs ob ON patdisp.encounter_id = ob.encounter_id
SET patdisp.dispensation_location = 1755
WHERE ob.concept_id = 1755
AND ob.value_coded = 1065
AND ob.voided = 0;

/*UPDATE ddp field for patient on DDP*/
UPDATE patient_dispensing patdisp
INNER JOIN _tmp_obs ob ON patdisp.encounter_id = ob.encounter_id
SET patdisp.ddp = 1065
WHERE ob.concept_id = @concept_ddp
AND ob.value_coded = 1065
AND ob.voided = 0;

/* Update on patient_dispensing where the drug is a ARV drug */
UPDATE patient_dispensing pdis
INNER JOIN arv_drugs ad ON pdis.drug_id = ad.drug_id
SET pdis.arv_drug = 1065;

/*update rx_or_prophy for table patient_dispensing*/
UPDATE isanteplus.patient_dispensing pdisp
INNER JOIN _tmp_obs ob2 ON pdisp.encounter_id = ob2.encounter_id
    AND pdisp.patient_id = ob2.person_id
    AND pdisp.location_id = ob2.location_id
INNER JOIN _tmp_obs_grp ob1 ON ob1.obs_id = ob2.obs_group_id
INNER JOIN _tmp_obs_sib ob3 ON ob1.obs_id = ob3.obs_group_id
    AND pdisp.drug_id = ob3.value_coded
SET pdisp.rx_or_prophy = ob2.value_coded
WHERE ob1.concept_id = 1442
AND ob2.concept_id = 160742
AND ob3.concept_id = 1282
AND ob2.voided = 0;

/*Update voided for drug removing*/
UPDATE isanteplus.patient_dispensing pp
INNER JOIN (
  select pap.obs_group_id, count(ob.obs_group_id)
  FROM _tmp_obs ob
  INNER JOIN isanteplus.patient_prescription pap
  ON pap.encounter_id = ob.encounter_id
  AND ob.obs_group_id = pap.obs_group_id
  WHERE ob.voided = 0
  GROUP BY 1
  HAVING count(ob.obs_group_id) <= 1
) B ON pp.obs_group_id = B.obs_group_id
SET pp.voided = 1
WHERE pp.voided <> 1;

/*INSERTION for patient on ARV*/
INSERT INTO patient_on_arv(patient_id,visit_id,visit_date, last_updated_date)
SELECT DISTINCT pdisp.patient_id, pdisp.visit_id,MIN(DATE(pdisp.visit_date)),now()
FROM patient_dispensing pdisp
WHERE pdisp.arv_drug = 1065
AND (pdisp.rx_or_prophy = 138405 OR pdisp.rx_or_prophy is null)
AND pdisp.voided <> 1
GROUP BY pdisp.patient_id
ON DUPLICATE KEY UPDATE
  visit_id = visit_id,
  visit_date = visit_date,
  last_updated_date = now();

COMMIT;

-- =============================================================================
-- SECTION 4 : Prescription (patient_prescription)
-- =============================================================================
START TRANSACTION;

/*Insert for patient_id,encounter_id, drug_id areas*/
INSERT INTO patient_prescription(
  patient_id,
  encounter_id,
  location_id,
  obs_id,
  obs_group_id,
  drug_id,
  dispense,
  last_updated_date,
  voided
)
SELECT DISTINCT ob.person_id,
  ob.encounter_id,ob.location_id,ob.obs_id, ob.obs_group_id,ob.value_coded,
  IF(ob1.concept_id = 163711, 1065, 1066), now(), ob.voided
FROM _tmp_obs ob
INNER JOIN _tmp_obs_grp ob1 ON ob.person_id = ob1.person_id
    AND ob.encounter_id = ob1.encounter_id
    AND ob.obs_group_id = ob1.obs_id
INNER JOIN _tmp_obs_sib ob2 ON ob1.obs_id = ob2.obs_group_id
WHERE (ob1.concept_id = 1442 OR ob1.concept_id = 163711)
AND ob.concept_id = 1282
AND ob2.concept_id IN(160742,1276,1444,159368,1443)
ON DUPLICATE KEY UPDATE
  encounter_id = ob.encounter_id,
  obs_id = ob.obs_id,
  obs_group_id = ob.obs_group_id,
  last_updated_date = now(),
  voided = ob.voided;

/*Insert for dispensing drugs*/
INSERT INTO patient_prescription(
  patient_id,
  encounter_id,
  location_id,
  obs_id,
  obs_group_id,
  drug_id,
  dispensation_date,
  dispense,
  last_updated_date,
  voided
)
SELECT DISTINCT ob.person_id,
  ob.encounter_id,ob.location_id,ob.obs_id,ob.obs_group_id,
  ob.value_coded,DATE(ob2.obs_datetime), 1065, now(), ob.voided
FROM _tmp_obs ob
INNER JOIN _tmp_obs_grp ob1 ON ob.person_id = ob1.person_id
    AND ob.encounter_id = ob1.encounter_id
    AND ob.obs_group_id = ob1.obs_id
INNER JOIN _tmp_obs_sib ob2 ON ob1.obs_id = ob2.obs_group_id
WHERE ob1.concept_id = 163711
AND ob.concept_id = 1282
AND ob2.concept_id IN(1276,1444,159368,1443)
ON DUPLICATE KEY UPDATE
  obs_id = ob.obs_id,
  obs_group_id = ob.obs_group_id,
  dispensation_date = ob2.obs_datetime,
  dispense = 1065,
  last_updated_date = now(),
  voided = ob.voided;

/* Update on patient_prescription where the drug is a ARV drug */
UPDATE patient_prescription ppres
INNER JOIN arv_drugs ad ON ppres.drug_id = ad.drug_id
SET ppres.arv_drug = 1065;

/*update provider for patient_prescription*/
UPDATE patient_prescription pp
INNER JOIN _tmp_encounter_provider enp ON pp.encounter_id = enp.encounter_id
SET pp.provider_id = enp.provider_id;

/*update visit_id, visit_date for table patient_prescription*/
UPDATE patient_prescription patp
INNER JOIN _tmp_encounter en ON patp.encounter_id = en.encounter_id
INNER JOIN _tmp_visit vi ON en.visit_id = vi.visit_id
SET patp.visit_id = vi.visit_id, patp.visit_date = vi.date_started;

/*update next_dispensation_date for table patient_prescription*/
UPDATE patient_prescription pp
INNER JOIN _tmp_obs ob ON pp.encounter_id = ob.encounter_id
SET pp.next_dispensation_date = DATE(ob.value_datetime)
WHERE ob.concept_id = 162549
AND ob.voided = 0;

/*update dispensation_location Dispensation communautaire=1755 for table patient_prescription*/
UPDATE patient_prescription pp
INNER JOIN _tmp_obs ob ON pp.encounter_id = ob.encounter_id
SET pp.dispensation_location = 1755
WHERE ob.concept_id = 1755
AND ob.value_coded = 1065
AND ob.voided = 0;

/*update rx_or_prophy for table patient_prescription*/
UPDATE isanteplus.patient_prescription pp
INNER JOIN _tmp_obs ob2 ON pp.encounter_id = ob2.encounter_id
INNER JOIN _tmp_obs_grp ob1 ON ob1.obs_id = ob2.obs_group_id
INNER JOIN _tmp_obs_sib ob3 ON ob1.obs_id = ob3.obs_group_id
    AND pp.drug_id = ob3.value_coded
SET pp.rx_or_prophy = ob2.value_coded
WHERE ob1.concept_id = 1442
AND ob2.concept_id = 160742
AND ob3.concept_id = 1282
AND ob2.voided = 0;

/*update posology_day for table patient_prescription*/
UPDATE isanteplus.patient_prescription pp
INNER JOIN _tmp_obs ob2 ON pp.encounter_id = ob2.encounter_id
INNER JOIN _tmp_obs_grp ob1 ON ob1.obs_id = ob2.obs_group_id
INNER JOIN _tmp_obs_sib ob3 ON ob1.obs_id = ob3.obs_group_id
    AND pp.drug_id = ob3.value_coded
SET pp.posology = ob2.value_text
WHERE ob1.concept_id = 1442
AND ob2.concept_id = 1444
AND ob3.concept_id = 1282
AND ob2.voided = 0;

/*Update for posology_alt */
UPDATE isanteplus.patient_prescription pp
INNER JOIN _tmp_obs ob2 ON pp.encounter_id = ob2.encounter_id
INNER JOIN _tmp_obs_grp ob1 ON ob1.obs_id = ob2.obs_group_id
INNER JOIN _tmp_obs_sib ob3 ON ob1.obs_id = ob3.obs_group_id
    AND pp.drug_id = ob3.value_coded
SET pp.posology_alt = ob2.value_text
WHERE ob1.concept_id = 1442
AND ob2.concept_id = @concept_posology_alt
AND ob3.concept_id = 1282
AND ob2.voided = 0;

/*update posology_alt_disp for table patient_prescription*/
UPDATE isanteplus.patient_prescription pp
INNER JOIN _tmp_obs ob2 ON pp.encounter_id = ob2.encounter_id
INNER JOIN _tmp_obs_grp ob1 ON ob1.obs_id = ob2.obs_group_id
INNER JOIN _tmp_obs_sib ob3 ON ob1.obs_id = ob3.obs_group_id
    AND pp.drug_id = ob3.value_coded
SET pp.posology_alt_disp = ob2.value_text
WHERE ob1.concept_id = 163711
AND ob2.concept_id = 1444
AND ob3.concept_id = 1282
AND ob2.voided = 0;

/*update number_day for table patient_prescription*/
UPDATE isanteplus.patient_prescription pp
INNER JOIN _tmp_obs ob2 ON pp.encounter_id = ob2.encounter_id
INNER JOIN _tmp_obs_grp ob1 ON ob1.obs_id = ob2.obs_group_id
INNER JOIN _tmp_obs_sib ob3 ON ob1.obs_id = ob3.obs_group_id
    AND pp.drug_id = ob3.value_coded
SET pp.number_day = ob2.value_numeric
WHERE (ob1.concept_id = 1442 OR ob1.concept_id = 163711)
AND ob2.concept_id = 159368
AND ob3.concept_id = 1282
AND ob2.voided = 0;

/*Update number_day_dispense for patient_prescription*/
UPDATE isanteplus.patient_prescription patdisp
INNER JOIN _tmp_obs ob ON patdisp.encounter_id = ob.encounter_id
INNER JOIN _tmp_obs_grp ob1 ON ob.encounter_id = ob1.encounter_id
    AND ob.obs_group_id = ob1.obs_id
SET patdisp.number_day_dispense = ob.value_numeric
WHERE ob1.concept_id = 163711
AND ob.concept_id = 159368
AND ob.voided = 0;

/*Update pills_amount_dispense for patient_prescription*/
UPDATE isanteplus.patient_prescription patdisp
INNER JOIN _tmp_obs ob ON patdisp.encounter_id = ob.encounter_id
INNER JOIN _tmp_obs_grp ob1 ON ob.encounter_id = ob1.encounter_id
    AND ob.obs_group_id = ob1.obs_id
SET patdisp.pills_amount_dispense = ob.value_numeric
WHERE ob1.concept_id = 163711
AND ob.concept_id = 1443
AND ob.voided = 0;

/*Update for having dispensation_date of the drug*/
UPDATE isanteplus.patient_prescription pp
INNER JOIN _tmp_obs ob2 ON pp.drug_id = ob2.value_coded
INNER JOIN _tmp_obs_grp ob1 ON ob1.obs_id = ob2.obs_group_id
INNER JOIN _tmp_obs_sib ob3 ON pp.encounter_id = ob3.encounter_id
    AND ob1.obs_id = ob3.obs_group_id
SET pp.dispensation_date = DATE(ob3.obs_datetime)
WHERE ob1.concept_id = 163711
AND ob2.concept_id = 1282
AND ob3.concept_id = 1276
AND ob3.voided = 0;

UPDATE isanteplus.patient_prescription pp
INNER JOIN (
  select pap.obs_group_id, count(ob.obs_group_id)
  FROM _tmp_obs ob
  INNER JOIN isanteplus.patient_prescription pap
  ON pap.encounter_id = ob.encounter_id
  AND ob.obs_group_id = pap.obs_group_id
  WHERE ob.voided = 0
  GROUP BY 1
  HAVING count(ob.obs_group_id) <= 1
) B ON pp.obs_group_id = B.obs_group_id
SET pp.voided = 1
WHERE pp.voided <> 1;

COMMIT;

-- =============================================================================
-- SECTION 5 : Qualité des soins (health_qual_patient_visit)
-- =============================================================================
START TRANSACTION;

/* Insert data to health_qual_patient_visit table */
INSERT INTO health_qual_patient_visit (visit_date, visit_id, encounter_id, location_id, patient_id, encounter_type, last_insert_date, last_updated_date, voided)
SELECT v.date_started AS visit_date, v.visit_id, e.encounter_id,v.location_id, v.patient_id, e.encounter_type, NOW() AS last_insert_date, NOW() AS last_updated_date, v.voided
FROM _tmp_visit v
INNER JOIN _tmp_encounter e ON v.visit_id = e.visit_id
    AND v.patient_id = e.patient_id
INNER JOIN _tmp_obs o ON o.person_id = e.patient_id
    AND o.encounter_id = e.encounter_id
WHERE o.voided = 0
ON DUPLICATE KEY UPDATE
  encounter_id = e.encounter_id,
  last_updated_date = NOW(),
  voided = v.voided;

/*Update health_qual_patient_visit table for having bmi*/
UPDATE isanteplus.health_qual_patient_visit pv
INNER JOIN (
  SELECT hs.visit_id, ws.weight , hs.height, ( ws.weight / (hs.height*hs.height/10000) ) AS 'patient_bmi'
  FROM (
    SELECT pv.visit_id, o.value_numeric AS 'height'
    FROM isanteplus.health_qual_patient_visit pv
    INNER JOIN _tmp_obs o
    ON o.person_id = pv.patient_id
    INNER JOIN _tmp_encounter e
    ON pv.visit_id = e.visit_id
    AND e.encounter_id = o.encounter_id
    AND e.encounter_id = pv.encounter_id
    WHERE o.concept_id = 5090
    AND o.voided = 0
  ) AS hs
  JOIN (
    SELECT pv.visit_id, o.value_numeric AS 'weight'
    FROM isanteplus.health_qual_patient_visit pv
    INNER JOIN _tmp_obs_grp o
    ON o.person_id = pv.patient_id
    INNER JOIN _tmp_encounter_2 e
    ON pv.visit_id = e.visit_id
    AND e.encounter_id = o.encounter_id
    AND e.encounter_id = pv.encounter_id
    WHERE o.concept_id = 5089
    AND o.voided = 0
  ) AS ws
  ON hs.visit_id = ws.visit_id
) AS bmi ON pv.visit_id = bmi.visit_id
SET pv.patient_bmi = bmi.patient_bmi;

/*Update patient_visit table for having family method planning indicator.*/
UPDATE isanteplus.health_qual_patient_visit pv
INNER JOIN (
  SELECT pv.visit_id, o.value_coded
  FROM isanteplus.health_qual_patient_visit pv
  INNER JOIN _tmp_obs o
  ON o.person_id = pv.patient_id
  INNER JOIN _tmp_encounter e
  ON pv.visit_id = e.visit_id
  AND e.encounter_id = o.encounter_id
  AND e.encounter_id = pv.encounter_id
  WHERE o.concept_id = 374
  AND o.voided = 0
) AS family_planning ON family_planning.visit_id = pv.visit_id
SET pv.family_planning_method_used = TRUE
WHERE value_coded IS NOT NULL;

/*Update health_qual_patient_visit table for adherence evaluation.*/
UPDATE isanteplus.health_qual_patient_visit pv
INNER JOIN (
  SELECT pv.visit_id, o.value_numeric
  FROM isanteplus.health_qual_patient_visit pv
  INNER JOIN _tmp_obs o
  ON o.person_id = pv.patient_id
  INNER JOIN _tmp_encounter e
  ON pv.visit_id = e.visit_id
  AND e.encounter_id = o.encounter_id
  WHERE o.concept_id = 163710
  AND o.voided = 0
) AS adherence ON adherence.visit_id = pv.visit_id
SET pv.adherence_evaluation = adherence.value_numeric
WHERE value_numeric IS NOT NULL;

/*Update health_qual_patient_visit table for evaluation of TB flag.*/
UPDATE isanteplus.health_qual_patient_visit pv
INNER JOIN (
  SELECT pv.visit_id, o.value_coded
  FROM isanteplus.health_qual_patient_visit pv
  INNER JOIN _tmp_obs o
  ON o.person_id = pv.patient_id
  INNER JOIN _tmp_encounter e
  ON pv.visit_id = e.visit_id
  AND e.encounter_id = o.encounter_id
  AND e.encounter_id = pv.encounter_id
  WHERE (o.concept_id IN (160265, 1659, 1110, 163283, 162320, 163284, 1633, 1389, 163951, 159431, 1113, 159798, 159398))
  AND o.voided = 0
) AS evaluation_of_tb ON evaluation_of_tb.visit_id = pv.visit_id
SET pv.evaluated_of_tb = TRUE
WHERE value_coded IS NOT NULL;

/*update for nutritional_assessment_status*/
UPDATE isanteplus.health_qual_patient_visit hqpv
INNER JOIN (
  SELECT pv.encounter_id, o.concept_id
  FROM isanteplus.health_qual_patient_visit pv
  INNER JOIN _tmp_obs o
  ON o.person_id = pv.patient_id
  INNER JOIN _tmp_encounter e
  ON pv.visit_id = e.visit_id
  AND e.encounter_id = o.encounter_id
  AND e.encounter_id = pv.encounter_id
  WHERE (
    (o.concept_id = 5089 AND o.concept_id = 5090)
    OR o.concept_id = 5314
    OR o.concept_id = 1343
  )
  AND o.voided = 0
) AS visits ON visits.encounter_id = hqpv.encounter_id
SET hqpv.nutritional_assessment_completed = TRUE;

/*update for is_active_tb*/
UPDATE isanteplus.health_qual_patient_visit hqpv
INNER JOIN (
  SELECT pv.encounter_id
  FROM isanteplus.health_qual_patient_visit pv
  INNER JOIN _tmp_obs o
  ON o.person_id = pv.patient_id
  INNER JOIN _tmp_encounter e
  ON pv.visit_id = e.visit_id
  AND e.encounter_id = o.encounter_id
  AND e.encounter_id = pv.encounter_id
  WHERE ((o.concept_id = 160592 AND o.value_coded = 113489) OR (o.concept_id = 160749 AND o.value_coded = 1065))
  AND o.voided = 0
) v ON v.encounter_id = hqpv.encounter_id
SET hqpv.is_active_tb = TRUE;

/*Update health_qual_patient_visit table for age patient at the visit.*/
UPDATE isanteplus.health_qual_patient_visit pv
INNER JOIN _tmp_person pe ON pe.person_id = pv.patient_id
SET pv.age_in_years = TIMESTAMPDIFF(YEAR, pe.birthdate, pv.visit_date);

COMMIT;

-- =============================================================================
-- SECTION 6 : Laboratoire (patient_laboratory)
-- =============================================================================
START TRANSACTION;

/*Insertion for patient_laboratory*/
INSERT INTO patient_laboratory(
  patient_id,
  encounter_id,
  location_id,
  test_id,
  last_updated_date,
  voided
)
SELECT DISTINCT ob.person_id,
  ob.encounter_id,ob.location_id,ob.value_coded, NOW(), ob.voided
FROM _tmp_obs ob
INNER JOIN _tmp_encounter enc ON ob.encounter_id = enc.encounter_id
WHERE enc.encounter_type = @et_lab
AND ob.concept_id = 1271
ON DUPLICATE KEY UPDATE
  encounter_id = ob.encounter_id,
  last_updated_date = NOW(),
  voided = ob.voided;

/*Insertion CD4 for patient_laboratory*/
INSERT INTO patient_laboratory(
  patient_id,
  encounter_id,
  location_id,
  test_id,
  last_updated_date,
  voided
)
SELECT DISTINCT ob.person_id,
  ob.encounter_id,ob.location_id,ob.concept_id, NOW(), ob.voided
FROM _tmp_obs ob
INNER JOIN _tmp_encounter enc ON ob.encounter_id = enc.encounter_id
WHERE enc.encounter_type IN (@et_first_hiv_visit, @et_ped_first_hiv_visit)
AND ob.concept_id in (1941,163544)
ON DUPLICATE KEY UPDATE
  encounter_id = ob.encounter_id,
  last_updated_date = NOW(),
  voided = ob.voided;

/*update provider for patient_laboratory*/
UPDATE patient_laboratory lab
INNER JOIN _tmp_encounter_provider enp ON lab.encounter_id = enp.encounter_id
SET lab.provider_id = enp.provider_id
WHERE enp.voided = 0;

/*update visit_id, visit_date for table patient_laboratory*/
UPDATE patient_laboratory lab
INNER JOIN _tmp_encounter en ON lab.encounter_id = en.encounter_id
INNER JOIN _tmp_visit vi ON en.visit_id = vi.visit_id
SET lab.visit_id = vi.visit_id, lab.visit_date = vi.date_started, lab.creation_date = vi.date_created
WHERE vi.voided = 0;

/*update test_done,date_test_done,comment_test_done for patient_laboratory*/
UPDATE patient_laboratory plab
INNER JOIN _tmp_obs ob ON plab.test_id = ob.concept_id
    AND plab.encounter_id = ob.encounter_id
SET plab.test_done = 1,
plab.test_result = CASE WHEN ob.value_coded IS NOT NULL
    THEN ob.value_coded
WHEN ob.value_numeric IS NOT NULL
  THEN ob.value_numeric
WHEN ob.value_text IS NOT NULL THEN ob.value_text
END,
plab.date_test_done = ob.obs_datetime,
plab.comment_test_done = ob.comments
WHERE ob.voided = 0
AND (ob.value_coded IS NOT NULL OR ob.value_numeric IS NOT NULL
  OR ob.value_text IS NOT NULL
)
  ;

/*update order_destination for patient_laboratory*/
UPDATE patient_laboratory plab
INNER JOIN _tmp_obs ob ON plab.encounter_id = ob.encounter_id
SET plab.order_destination = ob.value_text
WHERE ob.concept_id = 160632
AND ob.voided = 0;

/*update test_name for patient_laboratory*/
UPDATE patient_laboratory plab
INNER JOIN openmrs.concept_name cn ON plab.test_id = cn.concept_id
SET plab.test_name = cn.name
WHERE cn.locale = "fr"
AND cn.voided = 0;

COMMIT;

-- =============================================================================
-- SECTION 7 : Diagnostic TB (patient_tb_diagnosis)
-- =============================================================================
START TRANSACTION;

/*Insert when Tuberculose AND MDR TB areas are checked*/
INSERT INTO patient_tb_diagnosis
(
  patient_id,
  encounter_id,
  location_id,
  last_updated_date,
  voided
)
SELECT DISTINCT ob.person_id,
  ob.encounter_id,ob.location_id, now(), ob.voided
FROM _tmp_obs ob
INNER JOIN _tmp_obs_grp ob1 ON ob.person_id = ob1.person_id
    AND ob.encounter_id = ob1.encounter_id
    AND ob.obs_group_id = ob1.obs_id
WHERE ob1.concept_id IN (@concept_tb_diag_group, @concept_mdr_tb_diag_group)
AND ((ob.concept_id = 1284 AND ob.value_coded = 112141)
  OR (ob.concept_id = 1284 AND ob.value_coded = 159345)
)
ON DUPLICATE KEY UPDATE
  encounter_id = ob.encounter_id,
  last_updated_date = now(),
  voided = ob.voided;

/*Insert when Nouveau diagnostic Or suivi in the tuberculose menu are checked*/
INSERT INTO patient_tb_diagnosis
(
  patient_id,
  encounter_id,
  location_id,
  last_updated_date,
  voided
)
SELECT DISTINCT ob.person_id,ob.encounter_id,ob.location_id, now(), ob.voided
FROM _tmp_obs ob
WHERE ob.concept_id = 1659
AND (ob.value_coded = 160567 OR ob.value_coded = 1662 OR ob.value_coded = 1663)
ON DUPLICATE KEY UPDATE
  encounter_id = ob.encounter_id,
  last_updated_date = now(),
  voided = ob.voided;

/*Insert when the area Toux >= 2 semaines is checked*/
INSERT INTO patient_tb_diagnosis
(
  patient_id,
  encounter_id,
  location_id,
  last_updated_date,
  voided
)
SELECT DISTINCT ob.person_id,ob.encounter_id,ob.location_id, NOW(), ob.voided
FROM _tmp_obs ob
WHERE ob.concept_id = 159614
AND ob.value_coded = 159799
ON DUPLICATE KEY UPDATE
  encounter_id = ob.encounter_id,
  last_updated_date = NOW(),
  voided = ob.voided;

/*Insert when one of the status tb is checked on the resultat du traitement(tb) menu*/
INSERT INTO patient_tb_diagnosis
(
  patient_id,
  encounter_id,
  location_id,
  last_updated_date,
  voided
)
SELECT DISTINCT ob.person_id,ob.encounter_id,ob.location_id, NOW(), ob.voided
FROM _tmp_obs ob
WHERE ob.concept_id = 159786
AND (ob.value_coded = 159791 OR ob.value_coded = 160035
  OR ob.value_coded = 159874
  OR ob.value_coded = 160031
  OR ob.value_coded = 160034
)
ON DUPLICATE KEY UPDATE
  encounter_id = ob.encounter_id,
  last_updated_date = NOW(),
  voided = ob.voided;

/*Insert when the HIV patient has a TB diagnosis*/
INSERT INTO patient_tb_diagnosis
(
  patient_id,
  encounter_id,
  location_id,
  last_updated_date,
  voided
)
SELECT DISTINCT ob.person_id,ob.encounter_id,ob.location_id, NOW(), ob.voided
FROM _tmp_obs ob
WHERE (ob.concept_id = 6042 OR ob.concept_id = 6097)
AND (ob.value_coded = 159355 OR ob.value_coded = 42
  OR ob.value_coded = 118890
  OR ob.value_coded = 5042
)
ON DUPLICATE KEY UPDATE
  encounter_id = ob.encounter_id,
  last_updated_date = NOW(),
  voided = ob.voided;

/*update for visit_id AND visit_date*/
UPDATE patient_tb_diagnosis pat
INNER JOIN _tmp_encounter en ON pat.encounter_id = en.encounter_id
INNER JOIN _tmp_visit vi ON en.visit_id = vi.visit_id
SET pat.visit_id = vi.visit_id, pat.visit_date = vi.date_started
WHERE vi.voided = 0;

/*update provider*/
UPDATE patient_tb_diagnosis pat
INNER JOIN _tmp_encounter_provider enp ON pat.encounter_id = enp.encounter_id
SET pat.provider_id = enp.provider_id
WHERE enp.voided = 0;

/*Update tb_diag*/
UPDATE patient_tb_diagnosis pat
INNER JOIN _tmp_obs ob ON pat.encounter_id = ob.encounter_id
INNER JOIN _tmp_obs_grp ob1 ON ob.obs_group_id = ob1.obs_id
SET pat.tb_diag = 1
WHERE ob1.concept_id = @concept_tb_diag_group
AND (ob.concept_id = 1284 AND ob.value_coded = 112141)
AND ob.voided = 0;

/*Update mdr_tb_diag*/
UPDATE patient_tb_diagnosis pat
INNER JOIN _tmp_obs ob ON pat.encounter_id = ob.encounter_id
INNER JOIN _tmp_obs_grp ob1 ON ob.obs_group_id = ob1.obs_id
SET pat.mdr_tb_diag = 1
WHERE ob1.concept_id = @concept_mdr_tb_diag_group
AND (ob.concept_id = 1284 AND ob.value_coded = 159345)
AND ob.voided = 0;

/*update for TB type (pulmonaire, multirésistante, extrapulmonaire ou disséminée)*/
UPDATE patient_tb_diagnosis pat
INNER JOIN (
  SELECT ob.encounter_id,
  MAX(CASE WHEN ob.value_coded = 42 THEN 1 END) AS tb_pulmonaire,
  MAX(CASE WHEN ob.value_coded = 159355 THEN 1 END) AS tb_multiresistante,
  MAX(CASE WHEN ob.value_coded IN (118890, 5042) THEN 1 END) AS tb_extrapul_ou_diss
  FROM _tmp_obs ob
  WHERE ob.concept_id IN (6042, 6097)
  AND ob.value_coded IN (42, 159355, 118890, 5042)
  AND ob.voided = 0
  GROUP BY ob.encounter_id
) agg ON pat.encounter_id = agg.encounter_id
SET pat.tb_pulmonaire = agg.tb_pulmonaire,
pat.tb_multiresistante = agg.tb_multiresistante,
pat.tb_extrapul_ou_diss = agg.tb_extrapul_ou_diss;

/*update tb_new_diag AND tb_follow_up_diag*/
UPDATE patient_tb_diagnosis pat
INNER JOIN (
  SELECT ob.encounter_id,
  MAX(CASE WHEN ob.value_coded = 160567 THEN 1 END) AS tb_new_diag,
  MAX(CASE WHEN ob.value_coded = 1662 THEN 1 END) AS tb_follow_up_diag
  FROM _tmp_obs ob
  WHERE ob.concept_id = 1659
  AND ob.value_coded IN (160567, 1662)
  AND ob.voided = 0
  GROUP BY ob.encounter_id
) agg ON pat.encounter_id = agg.encounter_id
SET pat.tb_new_diag = agg.tb_new_diag,
pat.tb_follow_up_diag = agg.tb_follow_up_diag;

/*update cough_for_2wks_or_more*/
UPDATE patient_tb_diagnosis pat
INNER JOIN _tmp_obs ob ON pat.encounter_id = ob.encounter_id
SET pat.cough_for_2wks_or_more = 1
WHERE (ob.concept_id = 159614 AND ob.value_coded = 159799)
AND ob.voided = 0;

/*update tb_treatment_start_date*/
UPDATE patient_tb_diagnosis pat
INNER JOIN _tmp_obs ob ON pat.encounter_id = ob.encounter_id
SET pat.tb_treatment_start_date = ob.value_datetime
WHERE ob.concept_id = 1113
AND ob.voided = 0;

/*update for status_tb_treatment*/
UPDATE patient_tb_diagnosis pat
INNER JOIN _tmp_obs ob ON pat.encounter_id = ob.encounter_id
SET pat.status_tb_treatment=
CASE WHEN ob.value_coded = 159791 THEN 1 -- Cured
WHEN ob.value_coded = 160035 THEN 2 -- Completed Treatment
WHEN ob.value_coded = 159874 THEN 4 -- Treatment failure
WHEN ob.value_coded = 5240 OR ob.value_coded = 160031 THEN 8 -- Defaulted
WHEN ob.value_coded = 160034 THEN 5 -- Died
WHEN ob.value_coded = 159492 THEN 16 -- Transfered out
END
WHERE ob.concept_id = 159786
AND ob.voided = 0;

/*Update for Actif and Gueri for TB diagnosis for HIV patient*/
UPDATE patient_tb_diagnosis pat
INNER JOIN _tmp_obs ob ON pat.encounter_id = ob.encounter_id
INNER JOIN (
  SELECT o.person_id, o.encounter_id, COUNT(o.encounter_id) AS nb
  FROM _tmp_obs_grp o
  WHERE o.concept_id = 6042
  AND o.value_coded IN (42,159355,118890)
  GROUP BY 1
) a ON ob.encounter_id = a.encounter_id
    AND ob.person_id = a.person_id
SET pat.status_tb_treatment =
CASE WHEN (ob.concept_id = 6097 AND a.nb = 0)  THEN 1
WHEN (ob.concept_id = 6042 AND a.nb > 0)  THEN 6
END
WHERE ob.value_coded IN (42,159355,118890,5042)
AND ob.voided = 0;

/*Guéri*/
UPDATE isanteplus.patient_tb_diagnosis pat
INNER JOIN _tmp_obs ob ON pat.encounter_id = ob.encounter_id
    AND pat.patient_id = ob.person_id
SET pat.status_tb_treatment = 1
WHERE ob.concept_id = 6097
AND ob.value_coded IN (42,159355,118890,5042)
AND ob.voided = 0;

/*Actif*/
UPDATE isanteplus.patient_tb_diagnosis pat
INNER JOIN _tmp_obs ob ON pat.encounter_id = ob.encounter_id
    AND pat.patient_id = ob.person_id
SET pat.status_tb_treatment = 6
WHERE ob.concept_id = 6042
AND ob.value_coded IN (42,159355,118890)
AND ob.voided = 0;

/*Update for traitement TB COMPLETE AND Actuellement sous traitement*/
UPDATE patient_tb_diagnosis pat
INNER JOIN _tmp_obs ob ON pat.encounter_id = ob.encounter_id
SET pat.status_tb_treatment=
CASE WHEN ob.value_coded = 1663 THEN 2
WHEN ob.value_coded = 1662 THEN 6
ELSE NULL
END
WHERE ob.concept_id = 1659
AND ob.voided = 0;

/*update tb_treatment_stop_date*/
UPDATE patient_tb_diagnosis pat
INNER JOIN _tmp_obs ob ON pat.encounter_id = ob.encounter_id
SET pat.tb_treatment_stop_date = ob.value_datetime
WHERE ob.concept_id = 159431
AND ob.voided = 0;

/* Update encounter type id*/
UPDATE patient_tb_diagnosis pat
INNER JOIN _tmp_encounter enc ON pat.encounter_id = enc.encounter_id
SET pat.encounter_type_id = enc.encounter_type
WHERE enc.voided = 0;

/* Age at visit in years and Age at Visit in Months*/
UPDATE patient_tb_diagnosis pat
INNER JOIN patient p ON pat.patient_id = p.patient_id
SET pat.age_at_visit_years = TIMESTAMPDIFF(YEAR, p.birthdate, pat.visit_date),
pat.age_at_visit_months = TIMESTAMPDIFF(MONTH, p.birthdate, pat.visit_date);

/* Started TB Treatment*/
UPDATE patient_tb_diagnosis pat
INNER JOIN _tmp_obs o ON pat.encounter_id = o.encounter_id
SET pat.tb_started_treatment = 1
WHERE o.concept_id = 1113
AND o.value_datetime IS NOT NULL
AND o.voided = 0;

/* Dyspnea + tb_started_treatment (concept_id=159614, value_coded=122496)*/
UPDATE patient_tb_diagnosis pat
INNER JOIN _tmp_obs o ON pat.encounter_id = o.encounter_id
SET pat.tb_started_treatment = 1,
pat.dyspnea = 1
WHERE o.concept_id = 159614
AND o.value_coded = 122496
AND o.voided = 0;

/*Diagnosis based on sputum / Xray*/
UPDATE patient_tb_diagnosis pat
INNER JOIN (
  SELECT o.encounter_id,
  MAX(CASE WHEN o.value_coded = 307 THEN 1 END) AS tb_diag_sputum,
  MAX(CASE WHEN o.value_coded = 12 THEN 1 END) AS tb_diag_xray
  FROM _tmp_obs o
  WHERE o.concept_id = 163752
  AND o.value_coded IN (307, 12)
  AND o.voided = 0
  GROUP BY o.encounter_id
) agg ON pat.encounter_id = agg.encounter_id
SET pat.tb_diag_sputum = agg.tb_diag_sputum,
pat.tb_diag_xray = agg.tb_diag_xray;

/*Sputum Results at Month 0/2/3/5 and End*/
UPDATE patient_tb_diagnosis pat
INNER JOIN (
  SELECT ob1.encounter_id,
  MAX(CASE WHEN ob.concept_id = 166136 THEN (CASE WHEN ob1.value_coded = 703 THEN 1 WHEN ob1.value_coded = 664 THEN 2 END) END) AS tb_test_result_mon_0,
  MAX(CASE WHEN ob.concept_id = 166134 THEN (CASE WHEN ob1.value_coded = 703 THEN 1 WHEN ob1.value_coded = 664 THEN 2 END) END) AS tb_test_result_mon_2,
  MAX(CASE WHEN ob.concept_id = 165978 THEN (CASE WHEN ob1.value_coded = 703 THEN 1 WHEN ob1.value_coded = 664 THEN 2 END) END) AS tb_test_result_mon_3,
  MAX(CASE WHEN ob.concept_id = 165999 THEN (CASE WHEN ob1.value_coded = 703 THEN 1 WHEN ob1.value_coded = 664 THEN 2 END) END) AS tb_test_result_mon_5,
  MAX(CASE WHEN ob.concept_id = 165804 THEN (CASE WHEN ob1.value_coded = 703 THEN 1 WHEN ob1.value_coded = 664 THEN 2 END) END) AS tb_test_result_end
  FROM _tmp_obs ob1
  INNER JOIN _tmp_obs_grp ob
  ON ob.obs_id = ob1.obs_group_id
  WHERE ob.concept_id IN (166136, 166134, 165978, 165999, 165804)
  AND ob1.concept_id = 307
  GROUP BY ob1.encounter_id
) agg ON pat.encounter_id = agg.encounter_id
SET pat.tb_test_result_mon_0 = agg.tb_test_result_mon_0,
pat.tb_test_result_mon_2 = agg.tb_test_result_mon_2,
pat.tb_test_result_mon_3 = agg.tb_test_result_mon_3,
pat.tb_test_result_mon_5 = agg.tb_test_result_mon_5,
pat.tb_test_result_end = agg.tb_test_result_end;

/*TB Classification (Pulmonary, Extra-Pulmonary, Meningitis, Genital, Pleural, Miliary, Gangliponic, Intestinal, Other)*/
UPDATE patient_tb_diagnosis pat
INNER JOIN (
  SELECT o.encounter_id,
  MAX(CASE WHEN o.value_coded = 42 THEN 1 END) AS tb_class_pulmonary,
  MAX(CASE WHEN o.value_coded = 5042 THEN 1 END) AS tb_class_extrapulmonary,
  MAX(CASE WHEN o.value_coded = 111967 THEN 1 END) AS tb_extra_meningitis,
  MAX(CASE WHEN o.value_coded = 159167 THEN 1 END) AS tb_extra_genital,
  MAX(CASE WHEN o.value_coded = 111946 THEN 1 END) AS tb_extra_pleural,
  MAX(CASE WHEN o.value_coded = 115753 THEN 1 END) AS tb_extra_miliary,
  MAX(CASE WHEN o.value_coded = 111873 THEN 1 END) AS tb_extra_gangliponic,
  MAX(CASE WHEN o.value_coded = 161355 THEN 1 END) AS tb_extra_intestinal,
  MAX(CASE WHEN o.value_coded = 5622 THEN 1 END) AS tb_extra_other
  FROM _tmp_obs o
  WHERE o.concept_id = 160040
  AND o.value_coded IN (42, 5042, 111967, 159167, 111946, 115753, 111873, 161355, 5622)
  AND o.voided = 0
  GROUP BY o.encounter_id
) agg ON pat.encounter_id = agg.encounter_id
SET pat.tb_class_pulmonary = agg.tb_class_pulmonary,
pat.tb_class_extrapulmonary = agg.tb_class_extrapulmonary,
pat.tb_extra_meningitis = agg.tb_extra_meningitis,
pat.tb_extra_genital = agg.tb_extra_genital,
pat.tb_extra_pleural = agg.tb_extra_pleural,
pat.tb_extra_miliary = agg.tb_extra_miliary,
pat.tb_extra_gangliponic = agg.tb_extra_gangliponic,
pat.tb_extra_intestinal = agg.tb_extra_intestinal,
pat.tb_extra_other = agg.tb_extra_other;

/*Any TB Medication Prescribed*/
UPDATE patient_tb_diagnosis pat
INNER JOIN _tmp_obs o ON pat.encounter_id = o.encounter_id
SET pat.tb_medication_provided = 1
WHERE o.concept_id = 1111
AND o.value_coded IN (75948, 160093, 160096, 160095, 160092, 84360, 163753, 160094, 82900)
AND o.voided = 0;

/*HIV Test result*/
UPDATE patient_tb_diagnosis pat
INNER JOIN _tmp_obs o ON pat.encounter_id = o.encounter_id
SET pat.tb_hiv_test_result = (
  CASE WHEN(o.value_coded = 703) THEN 4 -- Positive
  WHEN (o.value_coded = 664) THEN 2 -- Negative
  END
)
WHERE o.concept_id = 1169
AND o.value_coded IN (1402, 664, 703)
AND o.voided = 0;

/*Cotrimoxazole prophylaxis*/
UPDATE patient_tb_diagnosis pat
INNER JOIN _tmp_obs o ON pat.encounter_id = o.encounter_id
SET pat.tb_prophy_cotrimoxazole = 1
WHERE o.concept_id = 1109
AND o.value_coded = 105281
AND o.voided = 0;

/*On ARVs*/
UPDATE patient_tb_diagnosis pat
INNER JOIN _tmp_obs o ON pat.encounter_id = o.encounter_id
SET pat.on_arv = (CASE WHEN o.value_coded = 160119 THEN 1 WHEN 1461 THEN 2 ELSE NULL END)
WHERE o.concept_id = 160117
AND o.value_coded IN (160119, 1461)
AND o.voided = 0;

COMMIT;

-- =============================================================================
-- SECTION 8 : Surveillance nutritionnelle (patient_nutrition)
-- =============================================================================
START TRANSACTION;

INSERT INTO patient_nutrition
(
  patient_id,
  encounter_type_id,
  encounter_id,
  location_id,
  last_updated_date,
  visit_id,
  visit_date,
  voided
)
SELECT DISTINCT
  enc.patient_id,
  enc.encounter_type,
  enc.encounter_id,
  enc.location_id,
  NOW(),
  enc.visit_id,
  CAST(enc.encounter_datetime AS DATE),
  enc.voided
FROM _tmp_encounter enc
WHERE enc.encounter_type IN (@et_adult_initial, @et_adult_followup, @et_ped_initial, @et_ped_followup)
ON DUPLICATE KEY UPDATE
  encounter_id = enc.encounter_id,
  visit_date = CAST(enc.encounter_datetime AS DATE),
  last_updated_date = NOW(),
  voided = enc.voided;

/*Age At Visit in Years and Months*/
UPDATE isanteplus.patient_nutrition pat
INNER JOIN _tmp_obs o ON pat.encounter_id = o.encounter_id
INNER JOIN isanteplus.patient p ON pat.patient_id = p.patient_id
SET pat.age_at_visit_years = TIMESTAMPDIFF(YEAR,p.birthdate,pat.visit_date),
pat.age_at_visit_months = TIMESTAMPDIFF(MONTH,p.birthdate,pat.visit_date)
WHERE o.voided = 0;

/*Weight*/
UPDATE isanteplus.patient_nutrition pat
INNER JOIN _tmp_obs o ON pat.encounter_id = o.encounter_id
SET pat.weight = o.value_numeric
WHERE o.concept_id = 5089
AND o.voided = 0;

/*Height*/
UPDATE isanteplus.patient_nutrition pat
INNER JOIN _tmp_obs o ON pat.encounter_id = o.encounter_id
SET pat.height = o.value_numeric
WHERE o.concept_id = 5090
AND o.voided = 0;

/*BMI*/
UPDATE isanteplus.patient_nutrition pat
SET pat.bmi = ROUND((pat.weight/(pat.height/100*pat.height/100)),1)
WHERE pat.age_at_visit_years>=20
AND pat.voided = 0;

/*Edema*/
UPDATE isanteplus.patient_nutrition pat
INNER JOIN _tmp_obs o ON pat.encounter_id = o.encounter_id
SET pat.edema = (CASE WHEN o.concept_id = 159614 AND o.value_coded = 460 THEN 1 ELSE 0 END)
WHERE o.voided = 0;

/*Weight for height*/
UPDATE isanteplus.patient_nutrition pat
INNER JOIN _tmp_obs o ON pat.encounter_id = o.encounter_id
SET pat.weight_for_height = (
  CASE
  WHEN o.value_coded = 1115 THEN 1 -- Normal
  WHEN o.value_coded = 164131 THEN 2 -- SAM
  WHEN o.value_coded = 123815 THEN 2 -- MAM
  END
)
WHERE o.concept_id = 163515
AND o.value_coded IN (1115, 164131, 123815)
AND o.voided = 0;

COMMIT;

-- =============================================================================
-- SECTION 9 : OB/GYN (patient_ob_gyn)
-- =============================================================================
START TRANSACTION;

INSERT INTO patient_ob_gyn
(
  patient_id,
  encounter_type_id,
  encounter_id,
  location_id,
  last_updated_date,
  visit_id,
  visit_date,
  voided
)
SELECT DISTINCT
  enc.patient_id,
  enc.encounter_type,
  enc.encounter_id,
  enc.location_id,
  NOW(),
  enc.visit_id,
  CAST(enc.encounter_datetime AS DATE),
  enc.voided
FROM _tmp_encounter enc
WHERE enc.encounter_type IN (@et_obgyn_initial, @et_obgyn_followup)
ON DUPLICATE KEY UPDATE
  encounter_id = enc.encounter_id,
  visit_date = CAST(enc.encounter_datetime AS DATE),
  last_updated_date = NOW(),
  voided = enc.voided;

/*MUAC*/
UPDATE isanteplus.patient_ob_gyn pat
INNER JOIN _tmp_obs o ON pat.encounter_id = o.encounter_id
SET pat.muac = o.value_numeric
WHERE o.concept_id = 1343
AND o.voided = 0;

/*Pregnant*/
UPDATE isanteplus.patient_ob_gyn pat
INNER JOIN _tmp_obs o ON pat.encounter_id = o.encounter_id
SET pat.pregnant = 1
WHERE o.concept_id = 160288
AND o.value_coded = 1622
AND o.voided = 0;

/*Next Visit Date*/
UPDATE isanteplus.patient_ob_gyn pat
INNER JOIN _tmp_obs o ON pat.encounter_id = o.encounter_id
SET pat.next_visit_date = o.value_datetime
WHERE o.concept_id = 5096
AND o.voided = 0;

/*Edd*/
UPDATE isanteplus.patient_ob_gyn pat
INNER JOIN _tmp_obs o ON pat.encounter_id = o.encounter_id
SET pat.edd = o.value_datetime
WHERE o.concept_id = 5596
AND o.voided = 0;

/*Birth Plan*/
UPDATE isanteplus.patient_ob_gyn pat
INNER JOIN _tmp_obs o ON pat.encounter_id = o.encounter_id
SET pat.birth_plan = 1
WHERE o.concept_id IN (163764, 161007, 160112, 163765, 163766)
AND o.value_coded = 1065
AND o.voided = 0;

/*High Risk*/
UPDATE isanteplus.patient_ob_gyn pat
INNER JOIN _tmp_obs o ON pat.encounter_id = o.encounter_id
SET pat.high_risk = 1
WHERE o.concept_id = 160079
AND o.value_coded IN (1107, 145777, 148834, 119476, 460, 1053, 163119, 163120)
AND o.voided = 0;

/*Gestation Greater Than 12Wks*/
UPDATE isanteplus.patient_ob_gyn pat
INNER JOIN _tmp_obs o ON pat.encounter_id = o.encounter_id
SET pat.gestation_greater_than_12_wks = 1
WHERE o.concept_id = 1438
AND o.value_numeric>=12
AND o.voided = 0;

/*Iron Supplement, Folic Acid Supplement, Prescribed Iron, Prescribed Folic Acid (concept_id=1282)*/
UPDATE isanteplus.patient_ob_gyn pat
INNER JOIN (
  SELECT o.encounter_id,
  MAX(CASE WHEN o.value_coded = 5843 THEN 1 END) AS iron_supplement,
  MAX(CASE WHEN o.value_coded = 76613 THEN 1 END) AS folic_acid_supplement,
  MAX(CASE WHEN o.value_coded = 78218 THEN 1 END) AS prescribed_iron,
  MAX(CASE WHEN o.value_coded = 76613 THEN 1 END) AS prescribed_folic_acid
  FROM _tmp_obs o
  WHERE o.concept_id = 1282
  AND o.value_coded IN (5843, 76613, 78218)
  AND o.voided = 0
  GROUP BY o.encounter_id
) agg ON pat.encounter_id = agg.encounter_id
SET pat.iron_supplement = agg.iron_supplement,
pat.folic_acid_supplement = agg.folic_acid_supplement,
pat.prescribed_iron = agg.prescribed_iron,
pat.prescribed_folic_acid = agg.prescribed_folic_acid;

/*Tetanus Toxoid Vaccine*/
UPDATE isanteplus.patient_ob_gyn pat
INNER JOIN _tmp_obs o ON pat.encounter_id = o.encounter_id
SET pat.tetanus_toxoid_vaccine = 1
WHERE o.concept_id = 984
AND o.value_coded = 84879
AND o.voided = 0;

/*Iron Defiency Anemia*/
UPDATE isanteplus.patient_ob_gyn pat
INNER JOIN _tmp_obs o ON pat.encounter_id = o.encounter_id
SET pat.iron_defiency_anemia = 1
WHERE o.concept_id = 160079
AND o.value_coded = 148834
AND o.voided = 0;

/*Elevated Blood Pressure*/
UPDATE isanteplus.patient_ob_gyn pat
INNER JOIN _tmp_obs o ON pat.encounter_id = o.encounter_id
SET pat.elevated_blood_pressure = 1
WHERE (o.concept_id = 5085 AND o.value_numeric>=120 AND o.value_numeric<=129) -- bp systolic
AND (o.concept_id = 5086 AND o.value_numeric<80) -- bp diastolic
AND o.voided = 0;

COMMIT;

-- =============================================================================
-- SECTION 10 : Imagerie + Discontinuation (patient_imagerie, discontinuation_reason, stopping_reason)
-- =============================================================================
TRUNCATE TABLE discontinuation_reason;
TRUNCATE TABLE stopping_reason;
START TRANSACTION;

/*Insertion for patient_imagerie */
INSERT INTO patient_imagerie (patient_id,location_id,visit_id,encounter_id,visit_date, voided)
SELECT DISTINCT ob.person_id,ob.location_id,vi.visit_id, ob.encounter_id,vi.date_started, vi.voided
FROM _tmp_obs ob
INNER JOIN _tmp_encounter en ON ob.encounter_id = en.encounter_id
INNER JOIN _tmp_visit vi ON en.visit_id = vi.visit_id
WHERE en.encounter_type = @et_imagerie
AND (ob.concept_id = 12 OR ob.concept_id = 309 OR ob.concept_id = 307)
ON DUPLICATE KEY UPDATE
  visit_date = vi.date_started,
  voided = vi.voided;

/*update radiographie_pul of table patient_imagerie*/
UPDATE isanteplus.patient_imagerie patim
INNER JOIN _tmp_obs ob ON patim.encounter_id = ob.encounter_id
SET patim.radiographie_pul = ob.value_coded
WHERE ob.concept_id = 12
AND ob.voided = 0;

/*update radiographie_autre of table patient_imagerie*/
UPDATE isanteplus.patient_imagerie patim
INNER JOIN _tmp_obs ob ON patim.encounter_id = ob.encounter_id
SET patim.radiographie_autre = ob.value_coded
WHERE ob.concept_id = 309
AND ob.voided = 0;

/*update crachat_barr of table patient_imagerie*/
UPDATE isanteplus.patient_imagerie patim
INNER JOIN _tmp_obs ob ON patim.encounter_id = ob.encounter_id
SET patim.crachat_barr = ob.value_coded
WHERE ob.concept_id = 307
AND ob.voided = 0;

/*Part of patient Status*/
INSERT INTO discontinuation_reason(patient_id,visit_id,visit_date,reason,reason_name)
SELECT v.patient_id,v.visit_id, MAX(v.date_started),ob.value_coded,
CASE WHEN(ob.value_coded = 5240) THEN 'Perdu de vue'
WHEN (ob.value_coded = 159492) THEN 'Transfert'
WHEN (ob.value_coded = 159) THEN 'Décès'
WHEN (ob.value_coded = 1667) THEN 'Discontinuations'
WHEN (ob.value_coded = 1067) THEN 'Inconnue'
END
FROM _tmp_visit v
INNER JOIN _tmp_encounter enc ON v.visit_id = enc.visit_id
INNER JOIN _tmp_obs ob ON enc.encounter_id = ob.encounter_id
WHERE enc.encounter_type = @et_discontinuation
AND ob.concept_id = 161555
AND ob.voided = 0
AND enc.voided <> 1
GROUP BY v.patient_id, ob.value_coded;

/*INSERT for stopping_reason*/
INSERT INTO stopping_reason(patient_id,visit_id,visit_date,reason,reason_name,other_reason)
SELECT v.patient_id,v.visit_id,
  MAX(v.date_started),ob.value_coded,
CASE WHEN(ob.value_coded = 1754) THEN 'ARVs non-disponibles'
WHEN (ob.value_coded = 160415) THEN 'Patient a déménagé'
WHEN (ob.value_coded = 115198) THEN 'Adhérence inadéquate'
WHEN (ob.value_coded = 159737) THEN 'Préférence du patient'
WHEN (ob.value_coded = 5622) THEN 'Autre raison, préciser'
END, ob.comments
FROM _tmp_visit v
INNER JOIN _tmp_encounter enc ON v.visit_id = enc.visit_id
INNER JOIN _tmp_obs ob ON enc.encounter_id = ob.encounter_id
WHERE enc.encounter_type = @et_discontinuation
AND ob.concept_id = 1667
AND ob.value_coded IN(1754,160415,115198,159737,5622)
AND ob.voided = 0
GROUP BY v.patient_id, ob.value_coded;

/*Delete FROM discontinuation_reason*/
DELETE FROM discontinuation_reason
WHERE visit_id NOT IN(SELECT str.visit_id FROM stopping_reason str
  WHERE str.reason = 115198
  OR str.reason = 159737
)
AND reason = 1667;

COMMIT;

-- =============================================================================
-- SECTION 11 : Grossesse (patient_pregnancy)
-- =============================================================================
START TRANSACTION;

/*Patient_pregnancy insertion*/
INSERT INTO patient_pregnancy (patient_id,encounter_id,start_date,last_updated_date, voided)
SELECT DISTINCT ob.person_id,ob.encounter_id,DATE(ob.obs_datetime) AS start_date, now(), ob.voided
FROM _tmp_obs ob
INNER JOIN _tmp_obs_grp ob1 ON ob.obs_group_id = ob1.obs_id
WHERE ob1.concept_id IN (@concept_preg_grp_1, @concept_preg_grp_2, @concept_preg_grp_3,
  @concept_preg_grp_4, @concept_preg_grp_5, @concept_preg_grp_6, @concept_preg_grp_7,
  @concept_preg_grp_8, @concept_preg_grp_9, @concept_preg_grp_10, @concept_preg_grp_11
)
AND ob.concept_id = 1284
AND ob.value_coded IN (46,129251,132678,47,163751,1449,118245,129211,141631,158489,490,118744)
ON DUPLICATE KEY UPDATE
  start_date = start_date,
  last_updated_date = now(),
  voided = ob.voided;

/*Patient_pregnancy insertion for area Femme enceinte (Grossesse)*/
INSERT INTO patient_pregnancy (patient_id,encounter_id,start_date,last_updated_date, voided)
SELECT ob.person_id,ob.encounter_id,DATE(ob.obs_datetime) AS start_date, NOW(), ob.voided
FROM _tmp_obs ob
WHERE ob.concept_id = 162225
AND ob.value_coded = 1434
ON DUPLICATE KEY UPDATE
  start_date = start_date,
  last_updated_date = NOW(),
  voided = ob.voided;

/*Patient_pregnancy insertion for area Conseils sur l'allaitement maternel*/
INSERT INTO patient_pregnancy (patient_id,encounter_id,start_date,last_updated_date, voided)
SELECT ob.person_id,ob.encounter_id,DATE(ob.obs_datetime) AS start_date, NOW(), ob.voided
FROM _tmp_obs ob
WHERE ob.concept_id = 1592
AND ob.value_coded IN (1910,162186,5486,5576,163106,1622)
ON DUPLICATE KEY UPDATE
  start_date = start_date,
  last_updated_date = NOW(),
  voided = ob.voided;

/*Insertion in patient_pregnancy table where prenatale is checked in the OBGYN form*/
INSERT INTO patient_pregnancy(patient_id,encounter_id,start_date, last_updated_date, voided)
SELECT DISTINCT ob.person_id,ob.encounter_id,DATE(enc.encounter_datetime) AS start_date,NOW(), ob.voided
FROM _tmp_obs ob
INNER JOIN _tmp_encounter enc ON ob.encounter_id = enc.encounter_id
WHERE enc.encounter_type IN (@et_obgyn_initial, @et_obgyn_followup)
AND ob.concept_id = 160288
AND ob.value_coded = 1622
ON DUPLICATE KEY UPDATE
  start_date = start_date,
  last_updated_date = NOW(),
  voided = ob.voided;

/*Insertion in patient_pregnancy table where DPA is filled*/
INSERT INTO patient_pregnancy(patient_id,encounter_id,start_date,last_updated_date, voided)
SELECT DISTINCT ob.person_id,ob.encounter_id,DATE(enc.encounter_datetime) AS start_date, NOW(), ob.voided
FROM _tmp_obs ob
INNER JOIN _tmp_encounter enc ON ob.encounter_id = enc.encounter_id
WHERE enc.encounter_type IN (@et_obgyn_initial, @et_obgyn_followup)
AND ob.concept_id = 5596
AND (ob.value_datetime <> "" AND ob.value_datetime IS NOT NULL)
ON DUPLICATE KEY UPDATE
  start_date = start_date,
  last_updated_date = NOW(),
  voided = ob.voided;

/*Patient_pregnancy insertion for areas B-HCG(positif),Test de Grossesse(positif) */
INSERT INTO patient_pregnancy(patient_id,encounter_id,start_date,last_updated_date, voided)
SELECT ob.person_id,ob.encounter_id,DATE(ob.obs_datetime) AS start_date, NOW(), ob.voided
FROM _tmp_obs ob
WHERE (ob.concept_id = 1945 OR ob.concept_id = 45)
AND ob.value_coded = 703
ON DUPLICATE KEY UPDATE
  start_date = start_date,
  last_updated_date = NOW(),
  voided = ob.voided;

/*INSERTION in patient_pregnancy for planning ARV*/
INSERT INTO patient_pregnancy(patient_id,encounter_id,start_date,last_updated_date, voided)
SELECT ob.person_id,ob.encounter_id,DATE(ob.obs_datetime) AS start_date, NOW(), ob.voided
FROM _tmp_obs ob
WHERE ob.concept_id IN (163764,161007,163765,163766)
AND ob.value_coded = 1065
ON DUPLICATE KEY UPDATE
  start_date = start_date,
  last_updated_date = NOW(),
  voided = ob.voided;

/*Insertion in patient_pregnancy for area Changement dans la fréquence mouvements foetaux*/
INSERT INTO patient_pregnancy(patient_id,encounter_id,start_date,last_updated_date, voided)
SELECT ob.person_id,ob.encounter_id,DATE(ob.obs_datetime) AS start_date, NOW(), ob.voided
FROM _tmp_obs ob
WHERE ob.concept_id = 159614
AND ob.value_coded IN (113377,159937)
ON DUPLICATE KEY UPDATE
  start_date = start_date,
  last_updated_date = NOW(),
  voided = ob.voided;

/*Insertion in patient_pregnancy table where a form travail et accouchement is filled*/
INSERT INTO patient_pregnancy(patient_id,encounter_id,start_date, end_date, last_updated_date, voided)
SELECT DISTINCT ob.person_id,ob.encounter_id,(DATE(enc.encounter_datetime)- INTERVAL 9 MONTH) AS start_date,
  DATE(enc.encounter_datetime) AS end_date, NOW(), ob.voided
FROM _tmp_obs ob
INNER JOIN _tmp_encounter enc ON ob.encounter_id = enc.encounter_id
WHERE enc.encounter_type = @et_labor_delivery
ON DUPLICATE KEY UPDATE
  start_date = start_date,
  end_date = end_date,
  last_updated_date = NOW(),
  voided = ob.voided;

/*Insertion in patient_pregnancy table where DPA/Lieu is filled*/
INSERT INTO patient_pregnancy(patient_id,encounter_id,start_date,last_updated_date, voided)
SELECT DISTINCT ob.person_id,ob.encounter_id,DATE(enc.encounter_datetime) AS start_date, NOW(), ob.voided
FROM _tmp_obs ob
INNER JOIN _tmp_encounter enc ON ob.encounter_id = enc.encounter_id
WHERE enc.encounter_type IN (@et_obgyn_initial, @et_obgyn_followup)
AND ob.concept_id IN(7957,159758)
AND ob.value_coded = 1589
AND (ob.comments IS NOT NULL AND ob.comments <> "")
ON DUPLICATE KEY UPDATE
  start_date = start_date,
  last_updated_date = NOW(),
  voided = ob.voided;

/*Insertion in patient_pregnancy table where Semaine de Gestation / Rythme cardiaque / Hauteur utérine*/
INSERT INTO patient_pregnancy(patient_id,encounter_id,start_date,last_updated_date, voided)
SELECT DISTINCT ob.person_id,ob.encounter_id,DATE(enc.encounter_datetime) AS start_date, NOW(), ob.voided
FROM _tmp_obs ob
INNER JOIN _tmp_encounter enc ON ob.encounter_id = enc.encounter_id
WHERE enc.encounter_type IN (@et_obgyn_initial, @et_obgyn_followup)
AND ob.concept_id IN(1438,1440,1439)
AND ob.value_numeric > 0
ON DUPLICATE KEY UPDATE
  start_date = start_date,
  last_updated_date = NOW(),
  voided = ob.voided;

/*Patient_pregnancy - Insertion for Position*/
INSERT INTO patient_pregnancy(patient_id,encounter_id,start_date,last_updated_date, voided)
SELECT DISTINCT ob.person_id,ob.encounter_id,DATE(enc.encounter_datetime) AS start_date, NOW(), ob.voided
FROM _tmp_obs ob
INNER JOIN _tmp_encounter enc ON ob.encounter_id = enc.encounter_id
WHERE enc.encounter_type IN (@et_obgyn_initial, @et_obgyn_followup)
AND ob.concept_id = 163749
AND ob.value_coded IN (5141,5139)
ON DUPLICATE KEY UPDATE
  start_date = start_date,
  last_updated_date = NOW(),
  voided = ob.voided;

/*Patient_pregnancy - Insertion for Présentation*/
INSERT INTO patient_pregnancy(patient_id,encounter_id,start_date,last_updated_date, voided)
SELECT DISTINCT ob.person_id,ob.encounter_id,DATE(enc.encounter_datetime) AS start_date, NOW(), ob.voided
FROM _tmp_obs ob
INNER JOIN _tmp_encounter enc ON ob.encounter_id = enc.encounter_id
WHERE enc.encounter_type IN (@et_obgyn_initial, @et_obgyn_followup)
AND ob.concept_id = 160090
AND ob.value_coded IN (160001,139814,112259)
ON DUPLICATE KEY UPDATE
  start_date = start_date,
  last_updated_date = NOW(),
  voided = ob.voided;

/*Patient_pregnancy - Insertion for Position (concept 163750)*/
INSERT INTO patient_pregnancy(patient_id,encounter_id,start_date,last_updated_date, voided)
SELECT DISTINCT ob.person_id,ob.encounter_id,DATE(enc.encounter_datetime) AS start_date, NOW(), ob.voided
FROM _tmp_obs ob
INNER JOIN _tmp_encounter enc ON ob.encounter_id = enc.encounter_id
WHERE enc.encounter_type IN (@et_obgyn_initial, @et_obgyn_followup)
AND ob.concept_id = 163750
AND ob.value_coded IN (163748,163747)
ON DUPLICATE KEY UPDATE
  start_date = start_date,
  last_updated_date = NOW(),
  voided = ob.voided;

/* Patient_pregnancy updated date_stop for area DPA*/
UPDATE patient_pregnancy ppr
INNER JOIN _tmp_obs ob ON ppr.patient_id = ob.person_id
    AND ppr.start_date < DATE(ob.value_datetime)
SET end_date = DATE(ob.value_datetime)
WHERE ob.concept_id = 5596
AND ob.voided = 0
AND ppr.end_date IS NULL;

/*Patient_pregnancy updated end_date for La date d'une fiche de travail et d'accouchement*/
UPDATE patient_pregnancy ppr
INNER JOIN _tmp_encounter enc ON ppr.patient_id = enc.patient_id
    AND ppr.start_date < DATE(enc.encounter_datetime)
SET end_date = DATE(enc.encounter_datetime)
WHERE ppr.end_date is null
AND enc.encounter_type = @et_labor_delivery
AND enc.voided = 0;

/*Patient_pregnancy updated for DDR – 3 mois + 7 jours=1427 */
UPDATE patient_pregnancy ppr
INNER JOIN _tmp_obs ob ON ppr.patient_id = ob.person_id
INNER JOIN _tmp_encounter enc ON ob.person_id = enc.patient_id
    AND ppr.start_date <= DATE(enc.encounter_datetime)
SET end_date = DATE(ob.value_datetime) - INTERVAL 3 MONTH + INTERVAL 7 DAY + INTERVAL 1 YEAR
WHERE ob.concept_id = 1427
AND ob.voided = 0
AND ppr.end_date IS NULL;

/*update patient_pregnancy (Add 9 Months on the start_date for finding the end_date) */
UPDATE patient_pregnancy ppr
SET ppr.end_date = ppr.start_date + INTERVAL 9 MONTH
WHERE (TIMESTAMPDIFF(MONTH,ppr.start_date,DATE(NOW()))>=9)
AND ppr.end_date IS NULL;

COMMIT;

-- =============================================================================
-- SECTION 12 : Alertes (alert)
-- =============================================================================
TRUNCATE TABLE alert;
START TRANSACTION;

/*Insertion for Nombre de patient sous ARV depuis 6 mois sans un résultat de charge virale*/
INSERT INTO alert(patient_id,id_alert,encounter_id,date_alert)
SELECT DISTINCT B.patient_id,1,B.encounter_id, B.visit_date
FROM isanteplus.patient p
INNER JOIN (
  SELECT pdis.patient_id, MAX(pdis.encounter_id) AS encounter_id, MIN(DATE(pdis.visit_date)) AS visit_date
  FROM isanteplus.patient_dispensing pdis
  WHERE pdis.arv_drug = 1065
  GROUP BY 1
) B ON p.patient_id = B.patient_id
    AND p.date_started_arv = B.visit_date
WHERE (TIMESTAMPDIFF(MONTH,DATE(p.date_started_arv),DATE(NOW())) >= 6)
AND NOT EXISTS (SELECT 1 FROM isanteplus.patient_laboratory pl
  WHERE pl.patient_id = p.patient_id
  AND pl.test_id IN(856, 1305)
  AND pl.test_done = 1
  AND pl.voided <> 1
  AND ((pl.test_result IS NOT NULL) OR (pl.test_result <> ''))
)
AND NOT EXISTS (SELECT 1 FROM _tmp_encounter enc
  WHERE enc.patient_id = p.patient_id
  AND enc.encounter_type = @et_discontinuation
  AND EXISTS (SELECT 1 FROM isanteplus.discontinuation_reason dr
    WHERE dr.patient_id = enc.patient_id
  )
)
AND p.vih_status = 1;

/*Insertion for Nombre de femmes enceintes, sous ARV depuis 4 mois sans un résultat de charge virale*/
INSERT INTO alert(patient_id,id_alert,encounter_id,date_alert)
SELECT DISTINCT B.patient_id,2,B.encounter_id, B.visit_date
FROM isanteplus.patient p
INNER JOIN (
  SELECT pdis.patient_id, MAX(pdis.encounter_id) AS encounter_id, MIN(DATE(pdis.visit_date)) AS visit_date
  FROM isanteplus.patient_dispensing pdis
  WHERE pdis.arv_drug = 1065
  GROUP BY 1
) B ON p.patient_id = B.patient_id
    AND p.date_started_arv = B.visit_date
INNER JOIN isanteplus.patient_pregnancy pp ON p.patient_id = pp.patient_id
WHERE (TIMESTAMPDIFF(MONTH,DATE(p.date_started_arv),DATE(NOW())) >= 4)
AND NOT EXISTS (SELECT 1 FROM isanteplus.patient_laboratory pl
  WHERE pl.patient_id = p.patient_id
  AND pl.test_id IN(856, 1305)
  AND pl.test_done = 1
  AND pl.voided <> 1
  AND ((pl.test_result IS NOT NULL) OR (pl.test_result <> ''))
)
AND NOT EXISTS (SELECT 1 FROM _tmp_encounter enc
  WHERE enc.patient_id = p.patient_id
  AND enc.encounter_type = @et_discontinuation
  AND EXISTS (SELECT 1 FROM isanteplus.discontinuation_reason dr
    WHERE dr.patient_id = enc.patient_id
  )
)
AND p.vih_status = 1;

/*Insertion for Nombre de patients ayant leur dernière charge virale remontant à au moins 12 mois*/
INSERT INTO alert(patient_id,id_alert,encounter_id,date_alert)
SELECT DISTINCT plab.patient_id,3,plab.encounter_id, IFNULL(DATE(plab.date_test_done),DATE(plab.visit_date))
FROM isanteplus.patient p
INNER JOIN isanteplus.patient_laboratory plab ON p.patient_id = plab.patient_id
INNER JOIN (
  SELECT pl.patient_id, MAX(IFNULL(DATE(date_test_done),DATE(pl.visit_date))) AS visit_date
  FROM isanteplus.patient_laboratory pl
  WHERE pl.test_id IN(856, 1305)
  AND pl.test_done = 1
  AND pl.voided <> 1
  AND ((pl.test_result IS NOT NULL) OR (pl.test_result <> ''))
  GROUP BY 1
) C ON plab.patient_id = C.patient_id
    AND IFNULL(DATE(plab.date_test_done),DATE(plab.visit_date)) = C.visit_date
INNER JOIN isanteplus.patient_on_arv parv ON p.patient_id = parv.patient_id
WHERE (TIMESTAMPDIFF(MONTH,DATE(C.visit_date),DATE(NOW())) >= 12)
AND NOT EXISTS (SELECT 1 FROM _tmp_encounter enc
  WHERE enc.patient_id = p.patient_id
  AND enc.encounter_type = @et_discontinuation
  AND EXISTS (SELECT 1 FROM isanteplus.discontinuation_reason dr
    WHERE dr.patient_id = enc.patient_id
  )
)
AND p.vih_status = 1;

/*Insertion for charge virale > 1000 copies/ml remontant à au moins 3 mois*/
INSERT INTO alert(patient_id,id_alert,encounter_id,date_alert)
SELECT DISTINCT plab.patient_id,4,plab.encounter_id, IFNULL(DATE(plab.date_test_done),DATE(plab.visit_date))
FROM isanteplus.patient p
INNER JOIN isanteplus.patient_laboratory plab ON p.patient_id = plab.patient_id
INNER JOIN (
  SELECT pl.patient_id, MAX(IFNULL(DATE(date_test_done),DATE(pl.visit_date))) AS visit_date
  FROM isanteplus.patient_laboratory pl
  WHERE pl.test_id IN(856, 1305)
  AND pl.test_done = 1
  AND pl.voided <> 1
  AND ((pl.test_result IS NOT NULL) OR (pl.test_result <> ''))
  GROUP BY 1
) C ON plab.patient_id = C.patient_id
    AND IFNULL(DATE(plab.date_test_done),DATE(plab.visit_date)) = C.visit_date
INNER JOIN isanteplus.patient_on_arv parv ON p.patient_id = parv.patient_id
WHERE (TIMESTAMPDIFF(MONTH,DATE(C.visit_date),DATE(NOW())) > 3)
AND plab.test_result > 1000
AND NOT EXISTS (SELECT 1 FROM _tmp_encounter enc
  WHERE enc.patient_id = p.patient_id
  AND enc.encounter_type = @et_discontinuation
  AND EXISTS (SELECT 1 FROM isanteplus.discontinuation_reason dr
    WHERE dr.patient_id = enc.patient_id
  )
)
AND p.vih_status = 1;

/*patient avec une dernière charge viral >1000 copies/ml*/
INSERT INTO alert(patient_id,id_alert,encounter_id,date_alert)
SELECT DISTINCT plab.patient_id,5,plab.encounter_id, IFNULL(DATE(plab.date_test_done),DATE(plab.visit_date))
FROM isanteplus.patient p
INNER JOIN isanteplus.patient_laboratory plab ON p.patient_id = plab.patient_id
INNER JOIN (
  SELECT pl.patient_id, MAX(IFNULL(DATE(date_test_done),DATE(pl.visit_date))) AS visit_date
  FROM isanteplus.patient_laboratory pl
  WHERE pl.test_id IN(856, 1305)
  AND pl.test_done = 1
  AND pl.voided <> 1
  AND ((pl.test_result IS NOT NULL) OR (pl.test_result <> ''))
  GROUP BY 1
) C ON plab.patient_id = C.patient_id
    AND IFNULL(DATE(plab.date_test_done),DATE(plab.visit_date)) = C.visit_date
INNER JOIN isanteplus.patient_on_arv parv ON p.patient_id = parv.patient_id
WHERE plab.test_result > 1000
AND NOT EXISTS (SELECT 1 FROM _tmp_encounter enc
  WHERE enc.patient_id = p.patient_id
  AND enc.encounter_type = @et_discontinuation
  AND EXISTS (SELECT 1 FROM isanteplus.discontinuation_reason dr
    WHERE dr.patient_id = enc.patient_id
  )
)
AND p.vih_status = 1;

/*Tout patient dont la prochaine date de dispensation arrive dans les 30 prochains jours*/
INSERT INTO alert(patient_id,id_alert,encounter_id,date_alert)
SELECT DISTINCT pdisp.patient_id,7,pdisp.encounter_id, DATE(pdisp.visit_date)
FROM isanteplus.patient p
INNER JOIN isanteplus.patient_dispensing pdisp ON p.patient_id = pdisp.patient_id
INNER JOIN (
  SELECT pd.patient_id, MAX(pd.next_dispensation_date) AS next_dispensation_date
  FROM isanteplus.patient_dispensing pd
  WHERE pd.arv_drug = 1065
  AND (pd.rx_or_prophy <> 163768 OR pd.rx_or_prophy IS NULL)
  AND pd.voided <> 1
  GROUP BY 1
) B ON pdisp.patient_id = B.patient_id
    AND pdisp.next_dispensation_date = B.next_dispensation_date
WHERE DATEDIFF(pdisp.next_dispensation_date,NOW()) BETWEEN 0
AND 30
AND NOT EXISTS (SELECT 1 FROM _tmp_encounter enc
  WHERE enc.patient_id = p.patient_id
  AND enc.encounter_type = @et_discontinuation
  AND EXISTS (SELECT 1 FROM isanteplus.discontinuation_reason dr
    WHERE dr.patient_id = enc.patient_id
  )
)
  ;

/*Tout patient dont la prochaine date de dispensation se situe dans le passe*/
INSERT INTO alert(patient_id,id_alert,encounter_id,date_alert)
SELECT DISTINCT pdisp.patient_id,7,pdisp.encounter_id, DATE(pdisp.visit_date)
FROM isanteplus.patient p
INNER JOIN isanteplus.patient_dispensing pdisp ON p.patient_id = pdisp.patient_id
INNER JOIN (
  SELECT pd.patient_id, MAX(pd.next_dispensation_date) AS next_dispensation_date
  FROM isanteplus.patient_dispensing pd
  WHERE pd.arv_drug = 1065
  AND (pd.rx_or_prophy <> 163768 OR pd.rx_or_prophy IS NULL)
  AND pd.voided <> 1
  GROUP BY 1
) B ON pdisp.patient_id = B.patient_id
    AND pdisp.next_dispensation_date = B.next_dispensation_date
WHERE DATEDIFF(B.next_dispensation_date,NOW()) < 0
AND NOT EXISTS (SELECT 1 FROM _tmp_encounter enc
  WHERE enc.patient_id = p.patient_id
  AND enc.encounter_type = @et_discontinuation
)
  ;

/*patients sous ARV depuis 5 mois sans un résultat de charge virale*/
INSERT INTO alert(patient_id,id_alert,encounter_id,date_alert)
SELECT DISTINCT B.patient_id,8,B.encounter_id, B.visit_date
FROM isanteplus.patient p
INNER JOIN (
  SELECT pdis.patient_id, MAX(pdis.encounter_id) AS encounter_id, MIN(DATE(pdis.visit_date)) AS visit_date
  FROM isanteplus.patient_dispensing pdis
  WHERE pdis.arv_drug = 1065
  GROUP BY 1
) B ON p.patient_id = B.patient_id
    AND p.date_started_arv = B.visit_date
WHERE (TIMESTAMPDIFF(MONTH,DATE(p.date_started_arv),DATE(NOW())) = 5)
AND NOT EXISTS (SELECT 1 FROM isanteplus.patient_laboratory pl
  WHERE pl.patient_id = p.patient_id
  AND pl.test_id IN(856, 1305)
  AND pl.test_done = 1
  AND pl.voided <> 1
  AND ((pl.test_result IS NOT NULL) OR (pl.test_result <> ''))
)
AND NOT EXISTS (SELECT 1 FROM _tmp_encounter enc
  WHERE enc.patient_id = p.patient_id
  AND enc.encounter_type = @et_discontinuation
)
AND p.vih_status = 1;

COMMIT;

-- =============================================================================
-- SECTION 13 : Type de visite, Accouchement, Tests virologiques,
--              VIH pédiatrique, Menstruation, Facteurs de risque, Vaccination
-- =============================================================================
START TRANSACTION;

/*Insertion for visit_type*/
INSERT INTO visit_type(patient_id,encounter_id,location_id,
  visit_id,concept_id,v_type,encounter_date, last_updated_date, voided
)
SELECT ob.person_id, ob.encounter_id,ob.location_id, enc.visit_id,
  ob.concept_id,ob.value_coded, DATE(enc.encounter_datetime), NOW(), ob.voided
FROM _tmp_obs ob
INNER JOIN _tmp_encounter enc ON ob.encounter_id = enc.encounter_id
WHERE ob.concept_id = 160288
AND ob.value_coded IN (160456,1622,1623,5483)
ON DUPLICATE KEY UPDATE
  encounter_id = ob.encounter_id,
  last_updated_date = NOW(),
  voided = ob.voided;

/* Insertion for table patient_delivery */
INSERT INTO patient_delivery
(
  patient_id,
  encounter_id,
  location_id,
  delivery_location,
  encounter_date,
  last_updated_date,
  voided
)
SELECT DISTINCT ob.person_id,ob.encounter_id,
  ob.location_id,ob.value_coded, DATE(enc.encounter_datetime), NOW(), ob.voided
FROM _tmp_obs ob
INNER JOIN _tmp_encounter enc ON ob.encounter_id = enc.encounter_id
WHERE enc.encounter_type = @et_labor_delivery
AND ob.concept_id = 1572
AND ob.value_coded IN(163266,1501,1502,5622)
ON DUPLICATE KEY UPDATE
  delivery_location = ob.value_coded,
  last_updated_date = NOW(),
  voided = ob.voided;

UPDATE patient_delivery pdel
INNER JOIN _tmp_obs ob ON pdel.encounter_id = ob.encounter_id
    AND pdel.location_id = ob.location_id
SET pdel.delivery_date = ob.value_datetime
WHERE ob.concept_id = 5599
AND ob.voided = 0;

/*insertion of virological tests (PCR) in the table virological_tests*/
INSERT INTO virological_tests
(
  patient_id,
  encounter_id,
  location_id,
  concept_group,
  obs_group_id,
  test_id,
  answer_concept_id,
  last_updated_date,
  voided
)
SELECT DISTINCT ob.person_id,ob.encounter_id,
  ob.location_id,ob1.concept_id,ob.obs_group_id,ob.concept_id, ob.value_coded, now(), ob.voided
FROM _tmp_obs ob
INNER JOIN _tmp_obs_grp ob1 ON ob.person_id = ob1.person_id
    AND ob.encounter_id = ob1.encounter_id
    AND ob.obs_group_id = ob1.obs_id
WHERE ob1.concept_id IN (@concept_viro_grp_1, @concept_viro_grp_2, @concept_viro_grp_3)
AND ob.concept_id = 162087
AND ob.value_coded = 1030
ON DUPLICATE KEY UPDATE
  encounter_id = ob.encounter_id,
  last_updated_date = now(),
  voided = ob.voided;

/*Update for area test_result for PCR*/
UPDATE virological_tests vtests
INNER JOIN _tmp_obs ob ON vtests.obs_group_id = ob.obs_group_id
    AND vtests.encounter_id = ob.encounter_id
    AND vtests.location_id = ob.location_id
SET vtests.test_result = ob.value_coded
WHERE ob.concept_id = 1030
AND ob.voided = 0;

/*Update for area age for PCR*/
UPDATE virological_tests vtests
INNER JOIN _tmp_obs ob ON vtests.obs_group_id = ob.obs_group_id
    AND vtests.encounter_id = ob.encounter_id
    AND vtests.location_id = ob.location_id
SET vtests.age = ob.value_numeric
WHERE ob.concept_id = 163540
AND ob.voided = 0;

/*Update for age_unit for PCR*/
UPDATE virological_tests vtests
INNER JOIN _tmp_obs ob ON vtests.obs_group_id = ob.obs_group_id
    AND vtests.encounter_id = ob.encounter_id
    AND vtests.location_id = ob.location_id
SET vtests.age_unit = ob.value_coded
WHERE ob.concept_id = 163541
AND ob.voided = 0;

/*Update encounter date for virological_tests*/
UPDATE virological_tests vtests
INNER JOIN _tmp_encounter enc ON vtests.location_id = enc.location_id
    AND vtests.encounter_id = enc.encounter_id
SET vtests.encounter_date = DATE(enc.encounter_datetime)
WHERE enc.voided = 0;

/*Update to fill test_date area*/
UPDATE virological_tests vtests
INNER JOIN patient p ON vtests.patient_id = p.patient_id
SET vtests.test_date =
CASE WHEN(vtests.age_unit = 1072 AND (ADDDATE(DATE(p.birthdate), INTERVAL vtests.age DAY) < DATE(NOW())))
  THEN ADDDATE(DATE(p.birthdate), INTERVAL vtests.age DAY)
WHEN(vtests.age_unit = 1074
  AND (ADDDATE(DATE(p.birthdate), INTERVAL vtests.age MONTH) < DATE(NOW()))
)
  THEN ADDDATE(DATE(p.birthdate), INTERVAL vtests.age MONTH)
ELSE
  vtests.encounter_date
END
WHERE vtests.test_id = 162087
AND answer_concept_id = 1030;

/*Insertion for pediatric_hiv_visit */
INSERT INTO pediatric_hiv_visit
(
  patient_id,
  encounter_id,
  location_id,
  encounter_date,
  voided
)
SELECT DISTINCT ob.person_id,ob.encounter_id,
  ob.location_id,DATE(enc.encounter_datetime), ob.voided
FROM _tmp_obs ob
INNER JOIN _tmp_encounter enc ON ob.encounter_id = enc.encounter_id
WHERE enc.encounter_type IN (@et_ped_first_hiv_visit, @et_ped_followup_hiv_visit)
AND ob.concept_id IN(163776,5665,1401)
ON DUPLICATE KEY UPDATE
  encounter_id = ob.encounter_id,
  voided = ob.voided;

/*update for ptme*/
UPDATE pediatric_hiv_visit pv
INNER JOIN _tmp_obs ob ON pv.encounter_id = ob.encounter_id
    AND pv.location_id = ob.location_id
SET pv.ptme = ob.value_coded
WHERE ob.concept_id = 163776
AND ob.voided = 0;

/*update for prophylaxie72h*/
UPDATE pediatric_hiv_visit pv
INNER JOIN _tmp_obs ob ON pv.encounter_id = ob.encounter_id
    AND pv.location_id = ob.location_id
SET pv.prophylaxie72h = ob.value_coded
WHERE ob.concept_id = 5665
AND ob.voided = 0;

/*update for actual_vih_status*/
UPDATE pediatric_hiv_visit pv
INNER JOIN _tmp_obs ob ON pv.encounter_id = ob.encounter_id
    AND pv.location_id = ob.location_id
SET pv.actual_vih_status = ob.value_coded
WHERE ob.concept_id = 1401
AND ob.voided = 0;

/*Insertion for patient_menstruation*/
INSERT INTO patient_menstruation
(
  patient_id,
  encounter_id,
  location_id,
  encounter_date,
  last_updated_date,
  voided
)
SELECT DISTINCT ob.person_id,ob.encounter_id,
  ob.location_id,DATE(enc.encounter_datetime), NOW(), ob.voided
FROM _tmp_obs ob
INNER JOIN _tmp_encounter enc ON ob.encounter_id = enc.encounter_id
WHERE enc.encounter_type IN (@et_obgyn_initial, @et_obgyn_followup)
AND ob.concept_id IN(163732,160597,1427)
ON DUPLICATE KEY UPDATE
  encounter_id = ob.encounter_id,
  last_updated_date = NOW(),
  voided = ob.voided;

/*Update table patient_menstruation for DDR value date*/
UPDATE patient_menstruation pm
INNER JOIN _tmp_obs ob ON pm.encounter_id = ob.encounter_id
    AND pm.location_id = ob.location_id
SET pm.ddr = DATE(ob.value_datetime)
WHERE ob.concept_id = 1427
AND ob.voided = 0;

/*Insertion for risks factor*/
INSERT INTO vih_risk_factor
(
  patient_id,
  encounter_id,
  location_id,
  risk_factor,
  encounter_date,
  last_updated_date,
  voided
)
SELECT DISTINCT ob.person_id,ob.encounter_id,
  ob.location_id,ob.value_coded,
  DATE(enc.encounter_datetime), NOW(), ob.voided
FROM _tmp_obs ob
INNER JOIN _tmp_encounter enc ON ob.encounter_id = enc.encounter_id
WHERE enc.encounter_type IN (@et_first_hiv_visit, @et_ped_first_hiv_visit)
AND ob.concept_id IN(1061,160581)
AND ob.value_coded IN (163290,163291,105,1063,163273,163274,163289,163275,5567,159218)
ON DUPLICATE KEY UPDATE
  encounter_id = ob.encounter_id,
  last_updated_date = NOW(),
  voided = ob.voided;

/*Insertion for risks factor for other risks*/
INSERT INTO vih_risk_factor
(
  patient_id,
  encounter_id,
  location_id,
  risk_factor,
  encounter_date,
  last_updated_date,
  voided
)
SELECT DISTINCT ob.person_id,ob.encounter_id,
  ob.location_id,ob.concept_id,
  DATE(enc.encounter_datetime), NOW(), ob.voided
FROM _tmp_obs ob
INNER JOIN _tmp_encounter enc ON ob.encounter_id = enc.encounter_id
WHERE enc.encounter_type IN (@et_first_hiv_visit, @et_ped_first_hiv_visit)
AND ob.concept_id IN(123160,156660,163276,163278,160579,160580)
AND ob.value_coded = 1065
ON DUPLICATE KEY UPDATE
  encounter_id = ob.encounter_id,
  last_updated_date = NOW(),
  voided = ob.voided;

/*Insertion for vaccination*/
INSERT INTO vaccination(
  patient_id,
  encounter_id,
  encounter_date,
  location_id,
  voided
)
SELECT DISTINCT ob.person_id, ob.encounter_id, enc.encounter_datetime, ob.location_id, ob.voided
FROM _tmp_obs ob
INNER JOIN _tmp_encounter enc ON ob.encounter_id = enc.encounter_id
WHERE ob.concept_id = 984
ON DUPLICATE KEY UPDATE
  encounter_id = ob.encounter_id,
  voided = ob.voided;

COMMIT;

/* Vaccination temp table (DDL - outside transaction) */
DROP TABLE IF EXISTS `temp_vaccination`;
CREATE TABLE temp_vaccination (
  person_id INT(11),
  value_coded INT(11),
  dose INT(11),
  obs_group_id INT(11),
  obs_datetime DATETIME,
  encounter_id INT(11)
);

START TRANSACTION;

/*Set age range (day)*/
UPDATE isanteplus.vaccination v
INNER JOIN isanteplus.patient p ON v.patient_id = p.patient_id
SET v.age_range=
CASE
WHEN (
  TIMESTAMPDIFF(DAY, p.birthdate, v.encounter_date) BETWEEN 0
  AND 45
) THEN 45
WHEN TIMESTAMPDIFF(DAY, p.birthdate, v.encounter_date) BETWEEN 46 AND 75
  THEN 75
WHEN TIMESTAMPDIFF(DAY, p.birthdate, v.encounter_date) BETWEEN 76 AND 105
  THEN 105
WHEN TIMESTAMPDIFF(DAY, p.birthdate, v.encounter_date) BETWEEN 106 AND 270
  THEN 270
ELSE NULL
END;

/*Query for receive vaccination dates*/
INSERT INTO temp_vaccination (person_id, value_coded, dose, obs_group_id, obs_datetime, encounter_id)
SELECT ob.person_id, ob.value_coded, ob2.value_numeric, ob.obs_group_id, ob.obs_datetime, ob.encounter_id
FROM _tmp_obs ob
INNER JOIN _tmp_obs_grp ob2 ON ob2.obs_group_id = ob.obs_group_id
WHERE ob2.concept_id = 1418
AND ob.concept_id = 984
AND ob.voided = 0;

/*Update vaccination table for children 0-45 days old*/
UPDATE isanteplus.vaccination v
SET v.vaccination_done = TRUE
WHERE v.age_range = 45
AND (
  ( -- Scenario A 0-45
    3 = (SELECT COUNT(tv.person_id) FROM temp_vaccination tv WHERE tv.encounter_id = v.encounter_id AND tv.dose = 1 AND tv.value_coded IN (783, 1423, 83531))
  )
  OR ( -- Scenario B 0-45
    5 = (SELECT COUNT(tv.person_id) FROM temp_vaccination tv WHERE tv.encounter_id = v.encounter_id AND tv.dose = 1 AND tv.value_coded IN (781, 782, 783, 5261, 83531))
  )
);

/*Update vaccination table for children 46-75 days old*/
UPDATE isanteplus.vaccination v
SET v.vaccination_done = TRUE
WHERE v.age_range = 75
AND (
  ( -- Scenario A 46-75
    3 = (SELECT COUNT(tv.person_id) FROM temp_vaccination tv WHERE tv.encounter_id = v.encounter_id AND tv.dose = 1 AND tv.value_coded IN (783, 1423, 83531))
    AND 3 = (SELECT COUNT(tv.person_id) FROM temp_vaccination tv WHERE tv.encounter_id = v.encounter_id AND tv.dose = 2 AND tv.value_coded IN (783, 1423, 83531))
  )
  OR ( -- Scenario B 46-75
    5 = (SELECT  COUNT(tv.person_id) FROM temp_vaccination tv WHERE tv.encounter_id = v.encounter_id AND tv.dose = 1 AND tv.value_coded IN (781, 782, 783, 5261, 83531))
    AND 5 = (SELECT  COUNT(tv.person_id) FROM temp_vaccination tv WHERE tv.encounter_id = v.encounter_id AND tv.dose = 2 AND tv.value_coded IN (781, 782, 783, 5261, 83531))
  )
);

/*Update vaccination table for children 76-105 days old*/
UPDATE isanteplus.vaccination v
SET v.vaccination_done = TRUE
WHERE v.age_range = 105
AND (
  ( -- Scenario A 76-105
    3 = (SELECT COUNT(tv.person_id) FROM temp_vaccination tv WHERE tv.encounter_id = v.encounter_id AND tv.dose = 1 AND tv.value_coded IN (783, 1423, 83531))
    AND 3 = (SELECT COUNT(tv.person_id) FROM temp_vaccination tv WHERE tv.encounter_id = v.encounter_id AND tv.dose = 2 AND tv.value_coded IN (783, 1423, 83531))
    AND 2 = (SELECT COUNT(tv.person_id) FROM temp_vaccination tv WHERE tv.encounter_id = v.encounter_id AND tv.dose = 2 AND tv.value_coded IN (783, 1423))
  )
  OR ( -- Scenario B 76-105
    5 = (SELECT  COUNT(tv.person_id) FROM temp_vaccination tv WHERE tv.encounter_id = v.encounter_id AND tv.dose = 1 AND tv.value_coded IN (781, 782, 783, 5261, 83531))
    AND 5 = (SELECT  COUNT(tv.person_id) FROM temp_vaccination tv WHERE tv.encounter_id = v.encounter_id AND tv.dose = 2 AND tv.value_coded IN (781, 782, 783, 5261, 83531))
    AND 4 = (SELECT  COUNT(tv.person_id) FROM temp_vaccination tv WHERE tv.encounter_id = v.encounter_id AND tv.dose = 3 AND tv.value_coded IN (781, 782, 783, 5261))
  )
);

/*Update vaccination table for children 106-270 days old*/
UPDATE isanteplus.vaccination v
SET v.vaccination_done = TRUE
WHERE v.age_range = 270
AND (
  ( -- Scenario A 106-270
    3 = (SELECT  COUNT(tv.person_id) FROM temp_vaccination tv WHERE tv.encounter_id = v.encounter_id AND tv.dose = 1 AND tv.value_coded IN (783, 1423, 83531))
    AND (
      159701 IN (SELECT tv.value_coded FROM temp_vaccination tv WHERE tv.encounter_id = v.encounter_id AND tv.dose = 1)
      OR 162586 IN (SELECT tv.value_coded FROM temp_vaccination tv WHERE tv.encounter_id = v.encounter_id AND tv.dose = 1)
    )
    AND 3 = (SELECT  COUNT(tv.person_id) FROM temp_vaccination tv WHERE tv.encounter_id = v.encounter_id AND tv.dose = 2 AND tv.value_coded IN (783, 1423, 83531))
    AND ((
        159701 IN (SELECT tv.value_coded FROM temp_vaccination tv WHERE tv.encounter_id = v.encounter_id AND tv.dose = 2)
        AND 159701 IN (SELECT tv.value_coded FROM temp_vaccination tv WHERE tv.encounter_id = v.encounter_id AND tv.dose = 1)
      ) OR (
        162586 IN (SELECT tv.value_coded FROM temp_vaccination tv WHERE tv.encounter_id = v.encounter_id AND tv.dose = 1)
      )
    )
    AND 2 = (SELECT  COUNT(tv.person_id) FROM temp_vaccination tv WHERE tv.encounter_id = v.encounter_id AND tv.dose = 3 AND tv.value_coded IN (783, 1423))
  )
  OR ( -- Scenario B 106-270
    5 = (SELECT  COUNT(tv.person_id) FROM temp_vaccination tv WHERE tv.encounter_id = v.encounter_id AND tv.dose = 1 AND tv.value_coded IN (781, 782, 783, 5261, 83531))
    AND (
      159701 IN (SELECT tv.value_coded FROM temp_vaccination tv WHERE tv.encounter_id = v.encounter_id AND tv.dose = 1)
      OR 162586 IN (SELECT tv.value_coded FROM temp_vaccination tv WHERE tv.encounter_id = v.encounter_id AND tv.dose = 1)
    )
    AND 5 = (SELECT  COUNT(tv.person_id) FROM temp_vaccination tv WHERE tv.encounter_id = v.encounter_id AND tv.dose = 2 AND tv.value_coded IN (781, 782, 783, 5261, 83531))
    AND ((
        159701 IN (SELECT tv.value_coded FROM temp_vaccination tv WHERE tv.encounter_id = v.encounter_id AND tv.dose = 2)
        AND 159701 IN (SELECT tv.value_coded FROM temp_vaccination tv WHERE tv.encounter_id = v.encounter_id AND tv.dose = 1)
      ) OR (
        162586 IN (SELECT tv.value_coded FROM temp_vaccination tv WHERE tv.encounter_id = v.encounter_id AND tv.dose = 1)
      )
    )
    AND 4 = (SELECT  COUNT(tv.person_id) FROM temp_vaccination tv WHERE tv.encounter_id = v.encounter_id AND tv.dose = 3 AND tv.value_coded IN (781, 782, 783, 5261))
  )
);

COMMIT;

DROP TABLE IF EXISTS `temp_vaccination`;

-- =============================================================================
-- SECTION 14 : Tests sérologiques, PCR, Paludisme
-- =============================================================================
TRUNCATE TABLE patient_pcr;
START TRANSACTION;

/*Part of serological tests*/
INSERT into serological_tests
(
  patient_id,
  encounter_id,
  location_id,
  concept_group,
  obs_group_id,
  test_id,
  answer_concept_id,
  last_updated_date,
  voided
)
SELECT DISTINCT ob.person_id,ob.encounter_id,
  ob.location_id,ob1.concept_id,ob.obs_group_id,ob.concept_id, ob.value_coded, now(), ob.voided
FROM _tmp_obs ob
INNER JOIN _tmp_obs_grp ob1 ON ob.person_id = ob1.person_id
    AND ob.encounter_id = ob1.encounter_id
    AND ob.obs_group_id = ob1.obs_id
WHERE ob1.concept_id IN (@concept_sero_grp_1,
  @concept_sero_grp_2,
  @concept_sero_grp_3,
  @concept_sero_grp_4,
  @concept_sero_grp_5,
  @concept_sero_grp_6
)
/*AND ob1.concept_id=1361*/
AND ob.concept_id = 162087
AND ob.value_coded IN(163722,1042)
ON DUPLICATE KEY UPDATE
  encounter_id = ob.encounter_id,
  last_updated_date = now(),
  voided = ob.voided;

/*Update for area test_result for tests serologiques*/
UPDATE serological_tests stests
INNER JOIN _tmp_obs ob ON stests.obs_group_id = ob.obs_group_id
    AND stests.encounter_id = ob.encounter_id
    AND stests.location_id = ob.location_id
SET stests.test_result = ob.value_coded
WHERE ob.concept_id = 163722
AND ob.voided = 0;

/*Update for area age for tests serologiques*/
UPDATE serological_tests stests
INNER JOIN _tmp_obs ob ON stests.obs_group_id = ob.obs_group_id
    AND stests.encounter_id = ob.encounter_id
    AND stests.location_id = ob.location_id
SET stests.age = ob.value_numeric
WHERE ob.concept_id = 163540
AND ob.voided = 0;

/*Update for age_unit for tests serologiques*/
UPDATE serological_tests stests
INNER JOIN _tmp_obs ob ON stests.obs_group_id = ob.obs_group_id
    AND stests.encounter_id = ob.encounter_id
    AND stests.location_id = ob.location_id
SET stests.age_unit = ob.value_coded
WHERE ob.concept_id = 163541
AND ob.voided = 0;

/*Update encounter date for serological_tests*/
UPDATE serological_tests stests
INNER JOIN _tmp_encounter enc ON stests.location_id = enc.location_id
    AND stests.encounter_id = enc.encounter_id
SET stests.encounter_date = DATE(enc.encounter_datetime);
/*End serological tests*/

/*Update to fill test_date area*/
UPDATE serological_tests stests
INNER JOIN patient p ON stests.patient_id = p.patient_id
SET stests.test_date =
CASE WHEN(stests.age_unit = 1072 AND (ADDDATE(DATE(p.birthdate), INTERVAL stests.age DAY) < DATE(NOW())))
  THEN ADDDATE(DATE(p.birthdate), INTERVAL stests.age DAY)
WHEN(stests.age_unit = 1074
  AND (ADDDATE(DATE(p.birthdate), INTERVAL stests.age MONTH) < DATE(NOW()))
)
  THEN ADDDATE(DATE(p.birthdate), INTERVAL stests.age MONTH)
ELSE
  stests.encounter_date
END
WHERE stests.test_id = 162087
AND answer_concept_id IN(163722,1042);

/*END of virological_tests table*/
/*Insert pcr on patient_pcr*/
INSERT INTO patient_pcr(patient_id,encounter_id,location_id,visit_date,pcr_result, test_date)
SELECT DISTINCT pl.patient_id,pl.encounter_id,pl.location_id,pl.visit_date,pl.test_result,pl.date_test_done
FROM isanteplus.patient_laboratory pl
WHERE pl.test_id = 844
AND pl.test_done = 1
AND pl.test_result IN(1301,1302,1300,1304);

INSERT INTO patient_pcr(patient_id,encounter_id,location_id,visit_date,pcr_result, test_date)
SELECT DISTINCT vt.patient_id,vt.encounter_id, vt.location_id,
  vt.encounter_date,vt.test_result,vt.test_date
FROM isanteplus.virological_tests vt
WHERE vt.test_id = 162087
AND vt.answer_concept_id = 1030
AND vt.test_result IN (664,703,1138);

INSERT INTO isanteplus.patient_malaria (patient_id, encounter_type_id, encounter_id, location_id, last_updated_date, visit_id, visit_date, voided)
SELECT DISTINCT
  enc.patient_id,
  enct.encounter_type_id,
  enc.encounter_id,
  enc.location_id,
  NOW(),
  enc.visit_id,
  CAST(enc.encounter_datetime AS DATE),
  enc.voided
FROM _tmp_encounter enc, openmrs.encounter_type enct
WHERE enc.encounter_type = enct.encounter_type_id
AND enct.uuid IN (
  '12f4d7c3-e047-4455-a607-47a40fe32460', -- Soins de santé primaire--premiére consultation (Adult intital consultation)
  'a5600919-4dde-4eb8-a45b-05c204af8284', -- Soins de santé primaire--consultation (Adult followp consultation)
  '709610ff-5e39-4a47-9c27-a60e740b0944', -- Soins de santé primaire--premiére con. p (Paeditric initial consultation)
  'fdb5b14f-555f-4282-b4c1-9286addf0aae', -- Soins de santé primaire--con. pédiatrique (Paediatric followup consultation)
  '49592bec-dd22-4b6c-a97f-4dd2af6f2171', -- Ob/gyn Suivi
  '5c312603-25c1-4dbe-be18-1a167eb85f97', -- Saisie Première ob/gyn
  '17536ba6-dd7c-4f58-8014-08c7cb798ac7', -- Saisie Première
  '204ad066-c5c2-4229-9a62-644bc5617ca2', -- Suivi Visite
  '349ae0b4-65c1-4122-aa06-480f186c8350', -- Saisie Première pédiatrique
  'f037e97b-471e-4898-a07c-b8e169e0ddc4' -- Analyses de Lab.
)
ON DUPLICATE KEY UPDATE
  encounter_id = enc.encounter_id,
  visit_date = CAST(enc.encounter_datetime AS DATE),
  last_updated_date = NOW(),
  voided = enc.voided;

/*Fever < 2 weeks*/
UPDATE isanteplus.patient_malaria pat
INNER JOIN _tmp_obs o ON pat.encounter_id = o.encounter_id
SET pat.fever_for_less_than_2wks = 1
WHERE o.concept_id = 159614
AND o.value_coded = 163740
AND o.voided = 0;

/*Suspected Malaria*/
UPDATE isanteplus.patient_malaria pat
INNER JOIN _tmp_obs o ON pat.encounter_id = o.encounter_id
SET pat.suspected_malaria = 1
WHERE o.concept_id = 6042 OR o.concept_id = 6097
AND o.value_coded = 116128
AND o.voided = 0;

/*Confirmed Malaria*/
UPDATE isanteplus.patient_malaria pat
INNER JOIN _tmp_obs o ON pat.encounter_id = o.encounter_id
SET pat.confirmed_malaria = 1
WHERE (o.concept_id = 6042 OR o.concept_id = 6097)
AND o.value_coded = 160148
AND o.voided = 0;

/*Treated with chloroquine / primaquine / quinine (concept_id=1282)*/
UPDATE isanteplus.patient_malaria pat
INNER JOIN (
  SELECT o.encounter_id,
  MAX(CASE WHEN o.value_coded = 73300 THEN 1 END) AS treated_with_chloroquine,
  MAX(CASE WHEN o.value_coded = 82521 THEN 1 END) AS treated_with_primaquine,
  MAX(CASE WHEN o.value_coded = 83023 THEN 1 END) AS treated_with_quinine
  FROM _tmp_obs o
  WHERE o.concept_id = 1282
  AND o.value_coded IN (73300, 82521, 83023)
  AND o.voided = 0
  GROUP BY o.encounter_id
) agg ON pat.encounter_id = agg.encounter_id
SET pat.treated_with_chloroquine = agg.treated_with_chloroquine,
pat.treated_with_primaquine = agg.treated_with_primaquine,
pat.treated_with_quinine = agg.treated_with_quinine;

/*Test orders: microscopic test / rapid test (concept_id=1271)*/
UPDATE isanteplus.patient_malaria pat
INNER JOIN (
  SELECT o.encounter_id,
  MAX(CASE WHEN o.value_coded = 1366 THEN 1 END) AS microscopic_test,
  MAX(CASE WHEN o.value_coded = 1643 THEN 1 END) AS rapid_test
  FROM _tmp_obs o
  WHERE o.concept_id = 1271
  AND o.value_coded IN (1366, 1643)
  AND o.voided = 0
  GROUP BY o.encounter_id
) agg ON pat.encounter_id = agg.encounter_id
SET pat.microscopic_test = agg.microscopic_test,
pat.rapid_test = agg.rapid_test;

/*Microscopic test results: positive / negative (concept_id=1366)*/
UPDATE isanteplus.patient_malaria pat
INNER JOIN (
  SELECT o.encounter_id,
  MAX(CASE WHEN o.value_coded IN (1365, 1364, 1362, 1363) THEN 1 END) AS positive_microscopic_test_result,
  MAX(CASE WHEN o.value_coded = 664 THEN 1 END) AS negative_microscopic_test_result
  FROM _tmp_obs o
  WHERE o.concept_id = 1366
  AND o.value_coded IN (1365, 1364, 1362, 1363, 664)
  AND o.voided = 0
  GROUP BY o.encounter_id
) agg ON pat.encounter_id = agg.encounter_id
SET pat.positive_microscopic_test_result = agg.positive_microscopic_test_result,
pat.negative_microscopic_test_result = agg.negative_microscopic_test_result;

/*Plasmodium results + rapid test result (concept_id=1643)*/
UPDATE isanteplus.patient_malaria pat
INNER JOIN (
  SELECT o.encounter_id,
  MAX(CASE WHEN o.value_coded = 161246 THEN 1 END) AS positive_plasmodium_falciparum_test_result,
  MAX(CASE WHEN o.value_coded = 161248 THEN 1 END) AS mixed_positive_test_result,
  MAX(CASE WHEN o.value_coded = 161247 THEN 1 END) AS positive_plasmodium_vivax_test_result,
  MAX(CASE WHEN o.value_coded = 703 THEN 1 END) AS positve_rapid_test_result
  FROM _tmp_obs o
  WHERE o.concept_id = 1643
  AND o.value_coded IN (161246, 161248, 161247, 703)
  AND o.voided = 0
  GROUP BY o.encounter_id
) agg ON pat.encounter_id = agg.encounter_id
SET pat.positive_plasmodium_falciparum_test_result = agg.positive_plasmodium_falciparum_test_result,
pat.mixed_positive_test_result = agg.mixed_positive_test_result,
pat.positive_plasmodium_vivax_test_result = agg.positive_plasmodium_vivax_test_result,
pat.positve_rapid_test_result = agg.positve_rapid_test_result;

/*Severe Malaria*/
UPDATE isanteplus.patient_malaria pat
INNER JOIN _tmp_obs o ON pat.encounter_id = o.encounter_id
SET pat.severe_malaria = 1
WHERE o.concept_id = 6042
AND o.value_coded = 160155
AND o.voided = 0;

/*Hospitalized*/
UPDATE isanteplus.patient_malaria pat
INNER JOIN _tmp_obs o ON pat.encounter_id = o.encounter_id
SET pat.hospitallized = 1
WHERE o.concept_id = 1272
AND o.value_coded = 5485
AND o.voided = 0;

/*Confirmed Malaria with pregnancy*/
UPDATE isanteplus.patient_malaria pat
INNER JOIN _tmp_obs o ON pat.encounter_id = o.encounter_id
SET pat.confirmed_malaria_preganancy = 1
WHERE o.concept_id = 160168
AND o.value_coded = 160152
AND o.voided = 0;

COMMIT;

-- =============================================================================
-- SECTION 15 : Patient sous ARV (patient_on_art), Populations clés,
--              Planning familial, Charge virale, Régime
-- =============================================================================
START TRANSACTION;

/*Starting insertion for patient_on_art table*/
INSERT INTO isanteplus.patient_on_art(patient_id)
SELECT DISTINCT pa.patient_id
FROM isanteplus.patient_on_arv pa
ON DUPLICATE KEY UPDATE
  patient_id = pa.patient_id;


/*Insertion lab VHI+ for patient_on_art table*/
INSERT INTO isanteplus.patient_on_art(patient_id)
SELECT  ob.person_id
FROM _tmp_obs ob
WHERE ob.concept_id = 1271
AND ob.value_coded IN (1040, 1042)
ON DUPLICATE KEY UPDATE
  patient_id = ob.person_id;

/*Update lab VHI+ for patient_on_art table*/
UPDATE isanteplus.patient_on_art pa
INNER JOIN _tmp_obs ob ON pa.patient_id = ob.person_id
SET pa.tested_hiv_postive = 1, pa.date_tested_hiv_postive = DATE(ob.obs_datetime)
WHERE ob.concept_id = 1040
AND ob.value_coded = 703
AND ob.voided = 0;

/*Insertion visit VHI+ for patient_on_art table*/
INSERT INTO isanteplus.patient_on_art(patient_id, tested_hiv_postive, date_tested_hiv_postive)
SELECT ob.person_id, 1, DATE(ob.value_datetime)
FROM _tmp_obs ob
WHERE ob.concept_id = 160082
AND ob.voided = 0
ON DUPLICATE KEY UPDATE
  patient_id = ob.person_id;


UPDATE isanteplus.patient_on_art par
INNER JOIN _tmp_obs o ON par.patient_id = o.person_id
SET par.date_completed_preventive_tb_treatment = DATE (o.value_datetime)
WHERE o.concept_id = 163284
AND o.voided = 0;

UPDATE isanteplus.patient_on_art par
INNER JOIN _tmp_obs o ON par.patient_id = o.person_id
SET par.date_completed_preventive_tb_treatment = DATE (o.value_datetime)
WHERE o.concept_id = 509166326
AND o.voided = 0;

UPDATE isanteplus.patient_on_art par
INNER JOIN _tmp_encounter e ON e.patient_id = par.patient_id
SET par.first_vist_date = DATE(e.encounter_datetime)
WHERE e.encounter_type IN (@et_followup_hiv_visit, @et_ped_followup_hiv_visit)
AND e.voided = 0;


UPDATE isanteplus.patient_on_art pat
INNER JOIN (
  SELECT e.patient_id, MAX(e.encounter_datetime) as encounter_datetime
  FROM _tmp_encounter e
  WHERE e.encounter_type IN (@et_first_hiv_visit,
    @et_ped_first_hiv_visit
  )
  AND e.voided = 0
  GROUP BY 1
) B ON pat.patient_id = B.patient_id
SET pat.last_folowup_vist_date = B.encounter_datetime;


UPDATE isanteplus.patient_on_art pat
INNER JOIN (
  SELECT e.patient_id, MAX(e.encounter_datetime) as encounter_datetime
  FROM _tmp_encounter e
  WHERE e.encounter_type IN (@et_first_hiv_visit,
    @et_ped_first_hiv_visit
  )
  AND e.voided = 0
  AND e.encounter_datetime NOT IN (SELECT MAX(e.encounter_datetime)
    FROM _tmp_encounter_2 e
    WHERE e.encounter_type IN (@et_first_hiv_visit, @et_ped_first_hiv_visit)
    AND e.voided = 0
  )
  GROUP BY 1
) B ON pat.patient_id = B.patient_id
SET pat.second_last_folowup_vist_date = B.encounter_datetime;


UPDATE isanteplus.patient_on_art pt
INNER JOIN _tmp_obs o ON o.person_id = pt.patient_id
INNER JOIN _tmp_encounter e ON o.encounter_id = e.encounter_id
SET pt.date_started_arv_for_transfered = DATE(o.obs_datetime)
WHERE o.concept_id = 159599
AND e.encounter_type = @et_first_hiv_visit
AND o.voided = 0;

UPDATE isanteplus.patient_on_art pt
INNER JOIN _tmp_obs o ON o.person_id = pt.patient_id
SET pt.screened_cervical_cancer = (CASE WHEN o.value_coded = 151185 THEN 1 ELSE 0 END)  ,
pt.date_screened_cervical_cancer = DATE(o.obs_datetime)
WHERE o.obs_group_id = 160714
AND o.concept_id = 1651
AND o.value_coded = 151185
AND o.value_coded = 0
AND o.voided = 0;

UPDATE isanteplus.patient_on_art pt
INNER JOIN _tmp_obs o ON o.person_id = pt.patient_id
SET pt.cervical_cancer_status = (
  CASE WHEN o.value_coded = 1115 THEN 'NEGATIVE'
  WHEN o.value_coded = 1116 THEN 'POSTIVE'
  WHEN o.value_coded = 1117 THEN 'UNKNOWN' END
),
pt.date_started_cervical_cancer_status = DATE(o.obs_datetime)
WHERE o.concept_id = 160704
AND o.voided = 0;

UPDATE isanteplus.patient_on_art pt
INNER JOIN _tmp_obs o ON o.person_id = pt.patient_id
SET pt.cervical_cancer_treatment = (
  CASE WHEN o.value_coded = 162812 THEN 'CRYOTHERAPY'
  WHEN o.value_coded = 162810 THEN 'LEEP'
  WHEN o.value_coded = 163408 THEN 'THERMOCOAGULATION' END
),
pt.date_cervical_cancer_treatment = DATE(o.obs_datetime)
WHERE o.concept_id = 1651
AND o.voided = 0;

UPDATE isanteplus.patient_on_art pt
INNER JOIN _tmp_obs o ON o.person_id = pt.patient_id
SET pt.date_started_breast_feeding = DATE(o.obs_datetime)
WHERE o.concept_id = @concept_breast_feeding
AND o.voided = 0;

UPDATE isanteplus.patient_on_art pt
INNER JOIN _tmp_obs o ON o.person_id = pt.patient_id
SET pt.key_population = (
  CASE WHEN o.value_coded = 160578 THEN 'MSM'
  WHEN o.value_coded = 160579 THEN 'SEX PROFESSIONAL'
  WHEN o.value_coded = 162277 THEN 'CAPTIVE'
  WHEN o.value_coded = 124275  THEN 'TRANSGENDER'
  WHEN o.value_coded = 105 THEN 'DRUG USER' END
)
WHERE o.concept_id = @concept_key_population
AND o.voided = 0;

UPDATE isanteplus.patient_on_art pt
INNER JOIN _tmp_obs o ON o.person_id = pt.patient_id
SET pt.reason_non_enrollment = (
  CASE WHEN o.value_coded = 127750 THEN 'VOLUNTARY'
  WHEN o.value_coded in (160432,159) THEN 'DIED'
  WHEN o.value_coded in (160036,159492) THEN 'REFERRED'
  WHEN o.value_coded = 162591 THEN 'MEDICAL'
  WHEN o.value_coded = 155891 THEN 'DENIAL'
  WHEN o.value_coded = 5622  THEN 'OTHER' END
),
pt.date_non_enrollment = DATE(o.obs_datetime)
WHERE o.concept_id in (1667,161555)
AND o.voided = 0;

UPDATE isanteplus.patient_on_art pt
INNER JOIN _tmp_obs o ON o.person_id = pt.patient_id
SET pt.breast_feeding = (CASE WHEN o.value_coded = 1065 THEN 1 ELSE 0 END),
pt.date_breast_feeding = DATE(o.obs_datetime)
WHERE o.concept_id = 5632
AND o.voided = 0;

/*Treatment regime lines: highest priority wins (THIRD > SECOND > FIRST)*/
UPDATE isanteplus.patient_on_art pat
INNER JOIN (
  SELECT o.person_id,
  CASE
  WHEN MAX(CASE WHEN o.value_coded = @concept_third_line_regimen THEN 3 END) IS NOT NULL THEN 'THIRD_LINE'
  WHEN MAX(CASE WHEN o.value_coded = @concept_second_line_regimen THEN 2 END) IS NOT NULL THEN 'SECOND_LINE'
  ELSE 'FIRST_LINE'
  END AS treatment_regime_lines,
  MAX(DATE(o.obs_datetime)) AS date_started_regime_treatment
  FROM _tmp_obs o
  WHERE o.concept_id = 164432
  AND o.value_coded IN (@concept_first_line_regimen, @concept_second_line_regimen, @concept_third_line_regimen)
  AND o.voided = 0
  GROUP BY o.person_id
) agg ON pat.patient_id = agg.person_id
SET pat.treatment_regime_lines = agg.treatment_regime_lines,
pat.date_started_regime_treatment = agg.date_started_regime_treatment;

UPDATE isanteplus.patient_on_art pat
INNER JOIN _tmp_obs o ON o.person_id = pat.patient_id
SET pat.date_full_6_months_of_inh_has_px = DATE (o.value_datetime)
WHERE o.concept_id = 163284
AND o.voided = 0;

UPDATE isanteplus.patient_on_art pat
INNER JOIN _tmp_obs o ON o.person_id = pat.patient_id
SET pat.tb_screened = 1 ,
pat.date_tb_screened = DATE (o.obs_datetime)
WHERE o.concept_id = 1659
AND o.value_coded IN  (142177,1660);

UPDATE isanteplus.patient_on_art pat
INNER JOIN _tmp_obs o ON o.person_id = pat.patient_id
SET pat.tb_status = (
  CASE WHEN o.value_coded = 142177 THEN 'POSTIVE'
  WHEN o.value_coded = 1660 THEN 'NEGATIVE' END
)
WHERE o.concept_id = 1659;

UPDATE isanteplus.patient_on_art pat
INNER JOIN _tmp_obs o ON o.person_id = pat.patient_id
SET pat.date_enrolled_on_tb_treatment = DATE (o.value_datetime)
WHERE o.concept_id = 1113
AND o.voided = 0;


UPDATE isanteplus.patient_on_art pat
INNER JOIN (
  SELECT e.patient_id, MIN(e.encounter_datetime) as min_encounter_date
  FROM _tmp_encounter e
  WHERE e.encounter_type IN (@et_followup_hiv_visit,
    @et_ped_followup_hiv_visit,
    @et_first_hiv_visit,
    @et_ped_first_hiv_visit
  )
  GROUP BY 1
) B ON pat.patient_id = B.patient_id
SET pat.date_tested_hiv_postive = B.min_encounter_date;

UPDATE isanteplus.patient_on_art pat
INNER JOIN _tmp_obs o ON pat.patient_id = o.person_id
SET  pat.tb_genexpert_test = 1 ,
pat.date_sample_sent_for_diagnositic_tb = DATE (o.obs_datetime),
pat.tb_bacteriological_test_status = (CASE WHEN o.value_coded = 1301 THEN 'POSTIVE' ELSE NULL END)
WHERE o.concept_id = @concept_genexpert
AND o.voided = 0;

UPDATE isanteplus.patient_on_art pat
INNER JOIN _tmp_obs o ON pat.patient_id = o.person_id
SET  pat.tb_crachat_test = 1 ,
pat.date_sample_sent_for_diagnositic_tb = DATE (o.obs_datetime) ,
pat.tb_bacteriological_test_status = (CASE WHEN o.value_coded IN (1362,1363,1364) THEN 'POSTIVE' ELSE NULL END)
WHERE o.concept_id = 307
AND o.voided = 0;


UPDATE isanteplus.patient_on_art pat
INNER JOIN _tmp_obs o ON pat.patient_id = o.person_id
SET  pat.tb_other_test = 1 ,
pat.date_sample_sent_for_diagnositic_tb = DATE (o.obs_datetime)
WHERE o.concept_id  IN (159984 ,159982 )
AND o.voided = 0;

UPDATE isanteplus.patient_on_art pat
INNER JOIN _tmp_obs o ON pat.patient_id = o.person_id
SET  pat.tb_bacteriological_test_status = 'POSTIVE'
WHERE o.concept_id = 159982
AND o.value_coded IN (@concept_tb_bact_pos_1, @concept_tb_bact_pos_2)
AND o.voided = 0;

UPDATE isanteplus.patient_on_art pat
INNER JOIN _tmp_obs o ON pat.patient_id = o.person_id
SET  pat.tb_bacteriological_test_status = 'POSTIVE'
WHERE o.concept_id = 159984
AND o.value_coded IN (162204, 162203)
AND o.voided = 0;


UPDATE isanteplus.patient_on_art pat
INNER JOIN _tmp_obs o ON pat.patient_id = o.person_id
SET  pat.viral_load_targeted = 1
WHERE o.concept_id = @concept_viral_load_type
AND o.value_coded = @concept_viral_load_targeted
AND o.voided = 0;

/*Family planning: accepted method, using method, dates (concept_id=374)*/
UPDATE isanteplus.patient_on_art pat
INNER JOIN _tmp_obs o ON pat.patient_id = o.person_id
SET pat.accepted_family_planning_method = (
  CASE WHEN o.value_coded = 780 THEN 'PILLS'
  WHEN o.value_coded = 190 THEN 'CONDOM'
  WHEN o.value_coded = 1359 THEN 'IMPLANTS'
  WHEN o.value_coded = 5279 THEN 'INJECT'
  WHEN o.value_coded = 163759 THEN 'NECKLACE' END
),
pat.using_family_planning_method = (
  CASE WHEN o.value_coded = 780 THEN 'PILLS'
  WHEN o.value_coded = 190 THEN 'CONDOM'
  WHEN o.value_coded = 1359 THEN 'IMPLANTS'
  WHEN o.value_coded = 5279 THEN 'INJECT'
  WHEN o.value_coded = 163759 THEN 'NECKLACE' END
),
pat.date_using_family_planning_method = DATE(o.obs_datetime)
WHERE o.concept_id = 374
AND o.voided = 0;

/*Family planning: earliest acceptance date (concept_id=374)*/
UPDATE isanteplus.patient_on_art pat
INNER JOIN (
  SELECT o.person_id, MIN(o.obs_datetime) AS min_obs_datetime
  FROM _tmp_obs o
  WHERE o.concept_id = 374
  AND o.voided = 0
  GROUP BY o.person_id
) fp ON pat.patient_id = fp.person_id
SET pat.date_accepted_family_planning_method = fp.min_obs_datetime;

UPDATE isanteplus.patient_on_art pat
INNER JOIN _tmp_obs o ON o.person_id = pat.patient_id
SET pat.migrated = (CASE WHEN o.value_coded = 160415 THEN 1 ELSE 0 END )
WHERE o.concept_id = 161555;

/* Insertion for key_populations table */
INSERT INTO key_populations
(
  patient_id,
  encounter_id,
  location_id,
  key_population,
  encounter_date,
  voided,
  last_updated_date
)
SELECT DISTINCT o.person_id,o.encounter_id,
  o.location_id,o.value_coded, o.value_datetime, o.voided, now()
FROM _tmp_obs o
WHERE o.concept_id = @concept_key_population
AND o.value_coded IS NOT NULL
ON DUPLICATE KEY UPDATE
  voided = o.voided,
  last_updated_date = now();

/*Insertion for Planning Familial */
INSERT INTO family_planning
(
  patient_id,
  encounter_id,
  location_id,
  planning,
  encounter_date,
  voided,
  last_updated_date
)
SELECT DISTINCT o.person_id,o.encounter_id,
  o.location_id,o.value_coded, o.obs_datetime, o.voided, now()
FROM _tmp_obs o
WHERE o.concept_id = 374
AND o.value_coded IN (780,190, 1359, 5279, 163759)
AND o.voided = 0
ON DUPLICATE KEY UPDATE
  voided = o.voided,
  last_updated_date = now();

/*Update for planning familial*/
UPDATE isanteplus.family_planning fp
SET  fp.family_planning_method_name = (
  CASE WHEN fp.planning = 780 THEN 'PILLS'
  WHEN fp.planning = 190  THEN 'CONDOM'
  WHEN fp.planning = 1359 THEN 'IMPLANTS'
  WHEN fp.planning = 5279 THEN 'INJECT'
  WHEN fp.planning = 163759 THEN 'NECKLACE' END
)
WHERE fp.planning IN (780,190, 1359, 5279, 163759)
AND fp.voided = 0;

/*Update for Accepting or using Family Planning : Accepting = 1, Using = 2*/
UPDATE isanteplus.family_planning fp
INNER JOIN (
  SELECT fpl.patient_id,
  MIN(fpl.encounter_date) AS encounter_date
  FROM family_planning fpl
  GROUP BY 1
) B ON fp.patient_id = B.patient_id
    AND DATE(fp.encounter_date) = DATE(B.encounter_date)
SET fp.accepting_or_using_fp = 1;

UPDATE isanteplus.family_planning fp SET fp.accepting_or_using_fp = 2
WHERE fp.accepting_or_using_fp IS NULL;

/* viral_load_routine = 1, viral_load_target = 2 */

UPDATE isanteplus.patient_laboratory pl SET pl.viral_load_target_or_routine = 1
WHERE pl.test_id IN (856,1305)
AND (pl.viral_load_target_or_routine IS NULL OR pl.viral_load_target_or_routine <> 2);

UPDATE isanteplus.patient_laboratory pl
INNER JOIN _tmp_obs o ON pl.patient_id = o.person_id
    AND pl.encounter_id = o.encounter_id
SET pl.viral_load_target_or_routine =
CASE WHEN (o.value_coded = @concept_viral_load_targeted) THEN 2
ELSE 1 END
WHERE o.concept_id = @concept_viral_load_type
AND o.value_coded IN (@concept_viral_load_routine, @concept_viral_load_targeted)
AND pl.test_id IN (856,1305);

/*Update for regimen First line, second line, third line*/
UPDATE isanteplus.patient_dispensing pdi
INNER JOIN _tmp_obs o ON pdi.patient_id = o.person_id
    AND pdi.encounter_id = o.encounter_id
SET treatment_regime_lines =
CASE WHEN (o.value_coded = @concept_first_line_regimen) THEN 'FIRST_LINE'
WHEN (o.value_coded = @concept_second_line_regimen) THEN 'SECOND_LINE'
WHEN (o.value_coded = @concept_third_line_regimen) THEN 'THIRD_LINE'
ELSE null END
WHERE o.concept_id = 164432
AND o.value_coded IN (@concept_first_line_regimen,
  @concept_second_line_regimen,
  @concept_third_line_regimen
)
AND pdi.arv_drug = 1065;

COMMIT;

-- =============================================================================
-- NETTOYAGE : Supprimer les tables temporaires
-- =============================================================================
DROP TEMPORARY TABLE IF EXISTS _tmp_obs;
DROP TEMPORARY TABLE IF EXISTS _tmp_obs_grp;
DROP TEMPORARY TABLE IF EXISTS _tmp_obs_sib;
DROP TEMPORARY TABLE IF EXISTS _tmp_encounter;
DROP TEMPORARY TABLE IF EXISTS _tmp_encounter_2;
DROP TEMPORARY TABLE IF EXISTS _tmp_visit;
DROP TEMPORARY TABLE IF EXISTS _tmp_visit_2;
DROP TEMPORARY TABLE IF EXISTS _tmp_encounter_provider;
DROP TEMPORARY TABLE IF EXISTS _tmp_person;
DROP TEMPORARY TABLE IF EXISTS _tmp_patient;
DROP TEMPORARY TABLE IF EXISTS _tmp_person_attribute;
