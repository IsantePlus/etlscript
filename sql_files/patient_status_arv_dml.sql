use isanteplus;

-- =============================================================================
-- patient_status_arv
--
-- Calculates patient ARV treatment status (1-11) and exposed infant
-- classifications. Updates patient_status_arv, exposed_infants, and
-- patient.arv_status tables.
-- =============================================================================
DELIMITER $$
DROP PROCEDURE IF EXISTS patient_status_arv$$
CREATE PROCEDURE patient_status_arv()
BEGIN
    -- Save original transaction isolation level
    DECLARE original_isolation VARCHAR(50);

    SET SQL_SAFE_UPDATES = 0;

    -- =========================================================================
    -- PHASE 1: READ FROM OPENMRS TABLES (LOW ISOLATION)
    -- =========================================================================

    SET SESSION TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
    START TRANSACTION;

    -- -------------------------------------------------------------------------
    -- Resolve encounter type UUIDs to IDs (small table, quick read)
    -- -------------------------------------------------------------------------
    SET @et_pediatric := (
        SELECT encounter_type_id FROM openmrs.encounter_type
        WHERE uuid = '349ae0b4-65c1-4122-aa06-480f186c8350'
    );
    SET @et_lab := (
        SELECT encounter_type_id FROM openmrs.encounter_type
        WHERE uuid = 'f037e97b-471e-4898-a07c-b8e169e0ddc4'
    );
    SET @et_discontinuation := (
        SELECT encounter_type_id FROM openmrs.encounter_type
        WHERE uuid = '9d0113c6-f23a-4461-8428-7e9a7344f2ba'
    );
    SET @et_pediatric_followup := (
        SELECT encounter_type_id FROM openmrs.encounter_type
        WHERE uuid = '33491314-c352-42d0-bd5d-a9d0bffc9bf1'
    );
    SET @et_first_visit := (
        SELECT encounter_type_id FROM openmrs.encounter_type
        WHERE uuid = '17536ba6-dd7c-4f58-8014-08c7cb798ac7'
    );
    SET @et_followup := (
        SELECT encounter_type_id FROM openmrs.encounter_type
        WHERE uuid = '204ad066-c5c2-4229-9a62-644bc5617ca2'
    );
    SET @et_dispensing1 := (
        SELECT encounter_type_id FROM openmrs.encounter_type
        WHERE uuid = '10d73929-54b6-4d18-a647-8b7316bc1ae3'
    );
    SET @et_dispensing2 := (
        SELECT encounter_type_id FROM openmrs.encounter_type
        WHERE uuid = 'a9392241-109f-4d67-885b-57cc4b8c638f'
    );

    -- -------------------------------------------------------------------------
    -- Pre-fetch data from openmrs.visit into temp table
    -- Single scan of visits table instead of multiple queries
    -- -------------------------------------------------------------------------
    DROP TEMPORARY TABLE IF EXISTS tmp_latest_visit;
    CREATE TEMPORARY TABLE tmp_latest_visit (
        patient_id INT NOT NULL,
        visit_date DATE NOT NULL,
        PRIMARY KEY (patient_id)
    ) ENGINE=MEMORY
    SELECT
        pvi.patient_id,
        MAX(DATE(pvi.date_started)) AS visit_date
    FROM openmrs.visit pvi
    WHERE pvi.voided = 0
    GROUP BY pvi.patient_id;

    -- -------------------------------------------------------------------------
    -- Pre-fetch ALL needed obs data in a SINGLE SCAN of openmrs.obs
    -- This is the key optimization - obs is the largest table
    -- Note: Using default engine (InnoDB) instead of MEMORY to avoid size limits
    -- -------------------------------------------------------------------------
    DROP TEMPORARY TABLE IF EXISTS tmp_obs_snapshot;
    CREATE TEMPORARY TABLE tmp_obs_snapshot (
        obs_id INT NOT NULL,
        person_id INT NOT NULL,
        encounter_id INT,
        concept_id INT NOT NULL,
        value_coded INT,
        value_numeric DOUBLE,
        obs_datetime DATETIME,
        obs_group_id INT,
        location_id INT,
        voided TINYINT,
        PRIMARY KEY (obs_id),
        KEY idx_person_concept (person_id, concept_id),
        KEY idx_encounter (encounter_id),
        KEY idx_concept_value (concept_id, value_coded),
        KEY idx_obs_group (obs_group_id)
    )
    SELECT
        o.obs_id,
        o.person_id,
        o.encounter_id,
        o.concept_id,
        o.value_coded,
        o.value_numeric,
        o.obs_datetime,
        o.obs_group_id,
        o.location_id,
        o.voided
    FROM openmrs.obs o
    WHERE o.concept_id IN (
        1030,    -- PCR test
        844,     -- Sero test
        1401,    -- Exposed infant checkbox
        161555,  -- Discontinuation reason
        1667,    -- Stop reason detail
        1282,    -- Drug order
        159367   -- Drug status
    )
    AND o.voided <> 1;

    -- Second copy of obs for self-joins (MySQL can't reopen temp tables)
    DROP TEMPORARY TABLE IF EXISTS tmp_obs_snapshot_2;
    CREATE TEMPORARY TABLE tmp_obs_snapshot_2 (
        obs_id INT NOT NULL,
        person_id INT NOT NULL,
        encounter_id INT,
        concept_id INT NOT NULL,
        value_coded INT,
        PRIMARY KEY (obs_id),
        KEY idx_encounter_concept (encounter_id, concept_id)
    )
    SELECT
        o.obs_id,
        o.person_id,
        o.encounter_id,
        o.concept_id,
        o.value_coded
    FROM openmrs.obs o
    WHERE o.concept_id = 1667  -- Stop reason detail (needed for status 3 self-join)
      AND o.voided <> 1;

    -- -------------------------------------------------------------------------
    -- Pre-fetch encounter data needed for joins
    -- Note: Using default engine (InnoDB) instead of MEMORY to avoid size limits
    -- -------------------------------------------------------------------------
    DROP TEMPORARY TABLE IF EXISTS tmp_encounter_snapshot;
    CREATE TEMPORARY TABLE tmp_encounter_snapshot (
        encounter_id INT NOT NULL,
        patient_id INT NOT NULL,
        visit_id INT,
        encounter_type INT,
        encounter_datetime DATETIME,
        voided TINYINT,
        PRIMARY KEY (encounter_id),
        KEY idx_patient (patient_id),
        KEY idx_visit (visit_id),
        KEY idx_type (encounter_type)
    )
    SELECT
        e.encounter_id,
        e.patient_id,
        e.visit_id,
        e.encounter_type,
        e.encounter_datetime,
        e.voided
    FROM openmrs.encounter e
    WHERE e.voided <> 1;

    -- Commit the read transaction - releases any read locks on openmrs tables
    COMMIT;

    -- =========================================================================
    -- PHASE 2: READ FROM ISANTEPLUS TABLES (also low isolation)
    -- These are ETL tables, but still use READ UNCOMMITTED for consistency
    -- =========================================================================

    START TRANSACTION;

    -- Temp table: Patients with discontinuation reasons (used for statuses 6, 8, 9)
    -- Includes all three reasons: 159 (deceased), 1667 (stopped), 159492 (transferred)
    DROP TEMPORARY TABLE IF EXISTS tmp_discontinued_patients;
    CREATE TEMPORARY TABLE tmp_discontinued_patients (
        patient_id INT NOT NULL,
        PRIMARY KEY (patient_id)
    ) ENGINE=MEMORY
    SELECT DISTINCT patient_id
    FROM isanteplus.discontinuation_reason
    WHERE reason IN (159, 1667, 159492);

    -- Temp table: Patients with discontinuation reasons for pre-ARV statuses (7, 10, 11)
    -- Only excludes deceased (159) and transferred (159492), NOT stopped (1667)
    DROP TEMPORARY TABLE IF EXISTS tmp_discontinued_pre_arv;
    CREATE TEMPORARY TABLE tmp_discontinued_pre_arv (
        patient_id INT NOT NULL,
        PRIMARY KEY (patient_id)
    ) ENGINE=MEMORY
    SELECT DISTINCT patient_id
    FROM isanteplus.discontinuation_reason
    WHERE reason IN (159, 159492);

    -- Temp table: Latest dispensation date per patient
    DROP TEMPORARY TABLE IF EXISTS tmp_latest_dispensation;
    CREATE TEMPORARY TABLE tmp_latest_dispensation (
        patient_id INT NOT NULL,
        next_dispensation_date DATE,
        PRIMARY KEY (patient_id)
    ) ENGINE=MEMORY
    SELECT
        pdisp.patient_id,
        MAX(pdisp.next_dispensation_date) AS next_dispensation_date
    FROM isanteplus.patient_dispensing pdisp
    WHERE pdisp.voided <> 1
      AND pdisp.arv_drug = 1065
    GROUP BY pdisp.patient_id;

    COMMIT;

    -- =========================================================================
    -- PHASE 3: WRITE TO ISANTEPLUS TABLES
    -- =========================================================================

    SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ;

    -- =========================================================================
    -- DELETE patients whose prescription forms are modified
    -- =========================================================================
    START TRANSACTION;

    DELETE poa FROM patient_on_arv poa
    LEFT JOIN (
        SELECT DISTINCT pdisp.patient_id
        FROM patient_dispensing pdisp
        WHERE pdisp.arv_drug = 1065
          AND (pdisp.rx_or_prophy = 138405 OR pdisp.rx_or_prophy IS NULL)
          AND pdisp.voided <> 1
    ) valid_patients ON poa.patient_id = valid_patients.patient_id
    WHERE valid_patients.patient_id IS NULL;

    -- Temp table: Patients on ARV (created AFTER delete to reflect current state)
    DROP TEMPORARY TABLE IF EXISTS tmp_patients_on_arv;
    CREATE TEMPORARY TABLE tmp_patients_on_arv (
        patient_id INT NOT NULL,
        PRIMARY KEY (patient_id)
    ) ENGINE=MEMORY
    SELECT patient_id FROM isanteplus.patient_on_arv;

    COMMIT;

    -- =========================================================================
    -- EXPOSED INFANTS SECTION
    -- =========================================================================
    START TRANSACTION;

    TRUNCATE TABLE exposed_infants;

    -- -------------------------------------------------------------------------
    -- Patients with negative PCR results (condition_exposee = 1)
    -- Now using pre-fetched obs and encounter snapshots
    -- -------------------------------------------------------------------------
    DROP TEMPORARY TABLE IF EXISTS tmp_latest_pcr_encounter;
    CREATE TEMPORARY TABLE tmp_latest_pcr_encounter (
        patient_id INT NOT NULL,
        visit_date DATETIME NOT NULL,
        PRIMARY KEY (patient_id)
    ) ENGINE=MEMORY
    SELECT
        en.patient_id,
        MAX(en.encounter_datetime) AS visit_date
    FROM tmp_obs_snapshot ob
    INNER JOIN tmp_encounter_snapshot en ON ob.encounter_id = en.encounter_id
    WHERE ob.concept_id IN (1030, 844)
      AND en.encounter_type IN (@et_pediatric, @et_lab)
    GROUP BY en.patient_id;

    ALTER TABLE tmp_latest_pcr_encounter ADD INDEX idx_patient_date (patient_id, visit_date);

    DROP TEMPORARY TABLE IF EXISTS patient_pcr_negative;
    CREATE TEMPORARY TABLE patient_pcr_negative (
        patient_id INT NOT NULL,
        encounter_id INT,
        location_id INT,
        encounter_date DATETIME,
        concept_id INT,
        value_coded INT,
        obs_datetime DATETIME,
        KEY idx_patient (patient_id)
    )
    SELECT
        o.person_id AS patient_id,
        o.encounter_id,
        o.location_id,
        e.encounter_datetime AS encounter_date,
        o.concept_id,
        o.value_coded,
        o.obs_datetime
    FROM tmp_obs_snapshot o
    INNER JOIN tmp_encounter_snapshot e
        ON o.encounter_id = e.encounter_id
       AND o.person_id = e.patient_id
    INNER JOIN tmp_latest_pcr_encounter B
        ON e.patient_id = B.patient_id
       AND DATE(e.encounter_datetime) = DATE(B.visit_date)
    WHERE e.encounter_type IN (@et_pediatric, @et_lab)
      AND o.concept_id IN (1030, 844)
      AND o.value_coded IN (664, 1302);  -- Negative results

    INSERT INTO exposed_infants(patient_id, location_id, encounter_id, visit_date, condition_exposee)
    SELECT ppn.patient_id, ppn.location_id, ppn.encounter_id, ppn.encounter_date, 1
    FROM patient_pcr_negative ppn
    WHERE (ppn.concept_id = 1030 AND ppn.value_coded = 664)
       OR (ppn.concept_id = 844 AND ppn.value_coded = 1302);

    DROP TEMPORARY TABLE IF EXISTS patient_pcr_negative;
    DROP TEMPORARY TABLE IF EXISTS tmp_latest_pcr_encounter;

    -- -------------------------------------------------------------------------
    -- Condition B - Enfant exposé checkbox
    -- -------------------------------------------------------------------------
    INSERT INTO exposed_infants(patient_id, location_id, encounter_id, visit_date, condition_exposee)
    SELECT DISTINCT
        ob.person_id,
        ob.location_id,
        ob.encounter_id,
        DATE(enc.encounter_datetime),
        3
    FROM tmp_obs_snapshot ob
    INNER JOIN tmp_encounter_snapshot enc ON ob.encounter_id = enc.encounter_id
    WHERE enc.encounter_type IN (@et_pediatric, @et_pediatric_followup)
      AND ob.concept_id = 1401
      AND ob.value_coded = 1405;

    -- -------------------------------------------------------------------------
    -- Condition D - ARV in prophylaxis
    -- -------------------------------------------------------------------------
    DROP TEMPORARY TABLE IF EXISTS tmp_latest_dispensing;
    CREATE TEMPORARY TABLE tmp_latest_dispensing (
        patient_id INT NOT NULL,
        visit_date DATETIME NOT NULL,
        PRIMARY KEY (patient_id)
    ) ENGINE=MEMORY
    SELECT patient_id, MAX(visit_date) AS visit_date
    FROM patient_dispensing
    WHERE voided <> 1
    GROUP BY patient_id;

    INSERT INTO exposed_infants(patient_id, location_id, encounter_id, visit_date, condition_exposee)
    SELECT DISTINCT
        pdisp.patient_id,
        pdisp.location_id,
        pdisp.encounter_id,
        pdisp.visit_date,
        4
    FROM patient_dispensing pdisp
    INNER JOIN tmp_latest_dispensing B
        ON pdisp.patient_id = B.patient_id
       AND pdisp.visit_date = B.visit_date
    WHERE pdisp.rx_or_prophy = 163768
      AND pdisp.arv_drug = 1065
      AND pdisp.voided <> 1;

    DROP TEMPORARY TABLE IF EXISTS tmp_latest_dispensing;

    -- -------------------------------------------------------------------------
    -- Remove patients with positive PCR from exposed_infants
    -- -------------------------------------------------------------------------
    DROP TEMPORARY TABLE IF EXISTS patient_pcr_positif;
    CREATE TEMPORARY TABLE patient_pcr_positif (
        patient_id INT NOT NULL,
        PRIMARY KEY (patient_id)
    ) ENGINE=MEMORY
    SELECT DISTINCT o.person_id AS patient_id
    FROM tmp_obs_snapshot o
    INNER JOIN tmp_encounter_snapshot e
        ON o.encounter_id = e.encounter_id
       AND o.person_id = e.patient_id
    WHERE e.encounter_type = @et_pediatric
      AND o.concept_id = 1030
      AND o.value_coded = 703;

    INSERT INTO patient_pcr_positif(patient_id)
    SELECT DISTINCT o.person_id
    FROM tmp_obs_snapshot o
    INNER JOIN tmp_encounter_snapshot e
        ON o.encounter_id = e.encounter_id
       AND o.person_id = e.patient_id
    WHERE e.encounter_type = @et_lab
      AND o.concept_id = 844
      AND o.value_coded = 1301
    ON DUPLICATE KEY UPDATE patient_id = VALUES(patient_id);

    DELETE ei FROM exposed_infants ei
    INNER JOIN patient_pcr_positif pcp ON ei.patient_id = pcp.patient_id;

    DROP TEMPORARY TABLE IF EXISTS patient_pcr_positif;

    -- -------------------------------------------------------------------------
    -- Remove patients with HIV positive test (age >= 18 months)
    -- -------------------------------------------------------------------------
    DELETE ei FROM exposed_infants ei
    INNER JOIN (
        SELECT pl.patient_id
        FROM patient_laboratory pl
        INNER JOIN patient p ON pl.patient_id = p.patient_id
        WHERE pl.test_id = 1040
          AND pl.test_done = 1
          AND pl.test_result = 703
          AND pl.voided <> 1
          AND TIMESTAMPDIFF(MONTH, p.birthdate, CURDATE()) >= 18
    ) hiv_positive ON ei.patient_id = hiv_positive.patient_id;

    -- -------------------------------------------------------------------------
    -- Remove patients with HIV confirmed by serological test
    -- -------------------------------------------------------------------------
    DELETE ei FROM exposed_infants ei
    INNER JOIN (
        SELECT DISTINCT ob.person_id
        FROM tmp_obs_snapshot ob
        INNER JOIN tmp_encounter_snapshot enc ON ob.encounter_id = enc.encounter_id
        WHERE enc.encounter_type IN (@et_pediatric, @et_pediatric_followup)
          AND ob.concept_id = 1401
          AND ob.value_coded = 163717
    ) confirmed_hiv ON ei.patient_id = confirmed_hiv.person_id;

    -- -------------------------------------------------------------------------
    -- Condition 5 - Séroréversion
    -- -------------------------------------------------------------------------
    INSERT INTO exposed_infants(patient_id, location_id, encounter_id, visit_date, condition_exposee)
    SELECT DISTINCT
        ob.person_id,
        ob.location_id,
        ob.encounter_id,
        DATE(enc.encounter_datetime),
        5
    FROM tmp_obs_snapshot ob
    INNER JOIN tmp_encounter_snapshot enc ON ob.encounter_id = enc.encounter_id
    WHERE enc.encounter_type = @et_discontinuation
      AND ob.concept_id = 1667
      AND ob.value_coded = 165439;

    COMMIT;

    -- =========================================================================
    -- PATIENT STATUS ARV SECTION
    -- Process in smaller transactions to reduce lock duration
    -- =========================================================================

    -- Transaction: Delete today's status
    START TRANSACTION;
    DELETE FROM patient_status_arv WHERE DATE(date_started_status) = CURDATE();
    COMMIT;

    -- Transaction: Status 4 and 5 (Deceased/Transferred pre-ARV)
    START TRANSACTION;

    -- Status 4: Décédés en Pré-ARV (Deceased pre-ARV)
    INSERT INTO patient_status_arv(patient_id, id_status, start_date, encounter_id, last_updated_date, date_started_status)
    SELECT
        v.patient_id,
        4 AS id_status,
        DATE(v.date_started) AS start_date,
        enc.encounter_id,
        NOW(),
        NOW()
    FROM isanteplus.patient ispat
    INNER JOIN openmrs.visit v ON ispat.patient_id = v.patient_id
    INNER JOIN tmp_encounter_snapshot enc ON v.visit_id = enc.visit_id
    INNER JOIN tmp_obs_snapshot ob
        ON enc.encounter_id = ob.encounter_id
       AND enc.patient_id = ob.person_id
    INNER JOIN tmp_latest_visit B
        ON v.patient_id = B.patient_id
       AND DATE(v.date_started) = B.visit_date
    LEFT JOIN tmp_patients_on_arv parv ON enc.patient_id = parv.patient_id
    WHERE enc.encounter_type = @et_discontinuation
      AND ob.concept_id = 161555
      AND ob.value_coded = 159
      AND ispat.vih_status = 1
      AND parv.patient_id IS NULL
      AND ispat.voided = 0
    GROUP BY v.patient_id
    ON DUPLICATE KEY UPDATE last_updated_date = VALUES(last_updated_date);

    -- Status 5: Transférés en Pré-ARV (Transferred pre-ARV)
    INSERT INTO patient_status_arv(patient_id, id_status, start_date, encounter_id, last_updated_date, date_started_status)
    SELECT
        v.patient_id,
        5 AS id_status,
        DATE(v.date_started) AS start_date,
        enc.encounter_id,
        NOW(),
        NOW()
    FROM isanteplus.patient ispat
    INNER JOIN openmrs.visit v ON ispat.patient_id = v.patient_id
    INNER JOIN tmp_encounter_snapshot enc ON v.visit_id = enc.visit_id
    INNER JOIN tmp_obs_snapshot ob
        ON enc.encounter_id = ob.encounter_id
       AND enc.patient_id = ob.person_id
    INNER JOIN tmp_latest_visit B
        ON v.patient_id = B.patient_id
       AND DATE(v.date_started) = B.visit_date
    LEFT JOIN tmp_patients_on_arv parv ON enc.patient_id = parv.patient_id
    WHERE enc.encounter_type = @et_discontinuation
      AND ob.concept_id = 161555
      AND ob.value_coded = 159492
      AND ispat.vih_status = 1
      AND parv.patient_id IS NULL
      AND ispat.voided = 0
    GROUP BY v.patient_id
    ON DUPLICATE KEY UPDATE last_updated_date = VALUES(last_updated_date);

    COMMIT;

    -- Transaction: Status 6, 8, 9 (Regular, Missed, Lost - dispensation based)
    START TRANSACTION;

    -- Status 6: Réguliers (Regular - on ARV, next dispensation not yet due)
    INSERT INTO patient_status_arv(patient_id, id_status, start_date, encounter_id, last_updated_date, date_started_status)
    SELECT
        pdis.patient_id,
        6 AS id_status,
        MAX(DATE(pdis.visit_date)) AS start_date,
        pdis.encounter_id,
        NOW(),
        NOW()
    FROM isanteplus.patient ipat
    INNER JOIN isanteplus.patient_dispensing pdis ON ipat.patient_id = pdis.patient_id
    INNER JOIN tmp_patients_on_arv p ON pdis.patient_id = p.patient_id
    INNER JOIN tmp_latest_dispensation mndisp
        ON pdis.patient_id = mndisp.patient_id
       AND pdis.next_dispensation_date = mndisp.next_dispensation_date
    INNER JOIN tmp_encounter_snapshot enc ON pdis.visit_id = enc.visit_id
    LEFT JOIN tmp_discontinued_patients dreason ON enc.patient_id = dreason.patient_id
    WHERE enc.encounter_type IN (@et_dispensing1, @et_dispensing2)
      AND dreason.patient_id IS NULL
      AND pdis.arv_drug = 1065
      AND CURDATE() <= pdis.next_dispensation_date
      AND pdis.voided <> 1
    GROUP BY pdis.patient_id
    ON DUPLICATE KEY UPDATE last_updated_date = VALUES(last_updated_date);

    -- Status 8: Rendez-vous ratés (Missed appointment - 1-30 days overdue)
    INSERT INTO patient_status_arv(patient_id, id_status, start_date, encounter_id, last_updated_date, date_started_status)
    SELECT
        pdis.patient_id,
        8 AS id_status,
        MAX(DATE(pdis.visit_date)) AS start_date,
        pdis.encounter_id,
        NOW(),
        NOW()
    FROM isanteplus.patient ipat
    INNER JOIN isanteplus.patient_dispensing pdis ON ipat.patient_id = pdis.patient_id
    INNER JOIN tmp_latest_dispensation mndisp
        ON pdis.patient_id = mndisp.patient_id
       AND pdis.next_dispensation_date = mndisp.next_dispensation_date
    INNER JOIN tmp_encounter_snapshot enc ON pdis.visit_id = enc.visit_id
    INNER JOIN tmp_patients_on_arv parv ON enc.patient_id = parv.patient_id
    LEFT JOIN tmp_discontinued_patients dreason ON enc.patient_id = dreason.patient_id
    WHERE enc.encounter_type IN (@et_dispensing1, @et_dispensing2)
      AND dreason.patient_id IS NULL
      AND DATEDIFF(CURDATE(), pdis.next_dispensation_date) BETWEEN 1 AND 30
      AND pdis.voided <> 1
    GROUP BY pdis.patient_id
    ON DUPLICATE KEY UPDATE last_updated_date = VALUES(last_updated_date);

    -- Status 9: Perdus de vue (Lost to follow-up - >30 days overdue)
    INSERT INTO patient_status_arv(patient_id, id_status, start_date, encounter_id, last_updated_date, date_started_status)
    SELECT
        pdis.patient_id,
        9 AS id_status,
        MAX(DATE(pdis.visit_date)) AS start_date,
        pdis.encounter_id,
        NOW(),
        NOW()
    FROM isanteplus.patient_dispensing pdis
    INNER JOIN tmp_latest_dispensation mndisp
        ON pdis.patient_id = mndisp.patient_id
       AND pdis.next_dispensation_date = mndisp.next_dispensation_date
    INNER JOIN tmp_encounter_snapshot enc ON pdis.visit_id = enc.visit_id
    INNER JOIN tmp_patients_on_arv parv ON enc.patient_id = parv.patient_id
    LEFT JOIN tmp_discontinued_patients dreason ON enc.patient_id = dreason.patient_id
    WHERE enc.encounter_type IN (@et_dispensing1, @et_dispensing2)
      AND dreason.patient_id IS NULL
      AND pdis.arv_drug = 1065
      AND pdis.voided <> 1
      AND DATEDIFF(CURDATE(), pdis.next_dispensation_date) > 30
    GROUP BY pdis.patient_id
    ON DUPLICATE KEY UPDATE last_updated_date = VALUES(last_updated_date);

    COMMIT;

    -- Transaction: Status 10, 7, 11 (Pre-ARV statuses)
    START TRANSACTION;

    -- Status 10: Perdus de vue en Pré-ARV (Lost to follow-up pre-ARV)
    -- Uses tmp_discontinued_pre_arv (only reasons 159, 159492 - NOT 1667)
    INSERT INTO patient_status_arv(patient_id, id_status, start_date, encounter_id, last_updated_date, date_started_status)
    SELECT
        v.patient_id,
        10 AS id_status,
        MAX(DATE(v.date_started)) AS start_date,
        enc.encounter_id,
        NOW(),
        NOW()
    FROM isanteplus.patient ispat
    INNER JOIN openmrs.visit v ON ispat.patient_id = v.patient_id
    INNER JOIN tmp_encounter_snapshot enc ON v.visit_id = enc.visit_id
    INNER JOIN tmp_latest_visit B
        ON v.patient_id = B.patient_id
       AND DATE(v.date_started) = B.visit_date
    LEFT JOIN tmp_discontinued_pre_arv dreason ON enc.patient_id = dreason.patient_id
    LEFT JOIN tmp_patients_on_arv parv ON ispat.patient_id = parv.patient_id
    WHERE v.voided <> 1
      AND enc.encounter_type NOT IN (
          @et_first_visit, @et_pediatric, @et_followup, @et_pediatric_followup,
          @et_dispensing1, @et_dispensing2, @et_lab
      )
      AND dreason.patient_id IS NULL
      AND parv.patient_id IS NULL
      AND ispat.vih_status = 1
      AND TIMESTAMPDIFF(MONTH, v.date_started, CURDATE()) > 12
    GROUP BY v.patient_id
    ON DUPLICATE KEY UPDATE last_updated_date = VALUES(last_updated_date);

    -- Status 7: Récent en Pré-ARV (Recent pre-ARV - within 12 months)
    -- Uses tmp_discontinued_pre_arv (only reasons 159, 159492 - NOT 1667)
    INSERT INTO patient_status_arv(patient_id, id_status, start_date, encounter_id, last_updated_date, date_started_status)
    SELECT
        v.patient_id,
        7 AS id_status,
        MAX(DATE(v.date_started)) AS start_date,
        enc.encounter_id,
        NOW(),
        NOW()
    FROM isanteplus.patient ispat
    INNER JOIN openmrs.visit v ON ispat.patient_id = v.patient_id
    INNER JOIN tmp_encounter_snapshot enc ON v.visit_id = enc.visit_id
    INNER JOIN tmp_latest_visit B
        ON v.patient_id = B.patient_id
       AND DATE(v.date_started) = B.visit_date
    LEFT JOIN tmp_discontinued_pre_arv dreason ON enc.patient_id = dreason.patient_id
    LEFT JOIN tmp_patients_on_arv parv ON ispat.patient_id = parv.patient_id
    WHERE v.voided <> 1
      AND enc.encounter_type IN (@et_first_visit, @et_pediatric)
      AND dreason.patient_id IS NULL
      AND parv.patient_id IS NULL
      AND ispat.vih_status = 1
      AND TIMESTAMPDIFF(MONTH, v.date_started, CURDATE()) <= 12
    GROUP BY v.patient_id
    ON DUPLICATE KEY UPDATE last_updated_date = VALUES(last_updated_date);

    -- Status 11: Actifs en Pré-ARV (Active pre-ARV - within 12 months)
    -- Uses tmp_discontinued_pre_arv (only reasons 159, 159492 - NOT 1667)
    INSERT INTO patient_status_arv(patient_id, id_status, start_date, encounter_id, last_updated_date, date_started_status)
    SELECT
        v.patient_id,
        11 AS id_status,
        MAX(DATE(v.date_started)) AS start_date,
        enc.encounter_id,
        NOW(),
        NOW()
    FROM isanteplus.patient ispat
    INNER JOIN openmrs.visit v ON ispat.patient_id = v.patient_id
    INNER JOIN tmp_encounter_snapshot enc ON v.visit_id = enc.visit_id
    INNER JOIN tmp_latest_visit B
        ON v.patient_id = B.patient_id
       AND DATE(v.date_started) = B.visit_date
    LEFT JOIN tmp_discontinued_pre_arv dreason ON enc.patient_id = dreason.patient_id
    LEFT JOIN tmp_patients_on_arv parv ON ispat.patient_id = parv.patient_id
    WHERE v.voided <> 1
      AND enc.encounter_type IN (@et_followup, @et_pediatric_followup, @et_dispensing1, @et_dispensing2, @et_lab)
      AND dreason.patient_id IS NULL
      AND parv.patient_id IS NULL
      AND ispat.vih_status = 1
      AND TIMESTAMPDIFF(MONTH, v.date_started, CURDATE()) <= 12
    GROUP BY v.patient_id
    ON DUPLICATE KEY UPDATE last_updated_date = VALUES(last_updated_date);

    COMMIT;

    -- Transaction: Status 1, 2, 3 (Deceased, Transferred, Stopped on ARV)
    START TRANSACTION;

    -- Status 1: Décédés (Deceased on ARV)
    INSERT INTO patient_status_arv(patient_id, id_status, start_date, encounter_id, last_updated_date, date_started_status)
    SELECT
        enc.patient_id,
        1 AS id_status,
        MAX(DATE(enc.encounter_datetime)) AS start_date,
        enc.encounter_id,
        NOW(),
        NOW()
    FROM tmp_encounter_snapshot enc
    INNER JOIN tmp_obs_snapshot ob
        ON enc.encounter_id = ob.encounter_id
       AND enc.patient_id = ob.person_id
    INNER JOIN tmp_patients_on_arv parv ON enc.patient_id = parv.patient_id
    WHERE enc.encounter_type = @et_discontinuation
      AND ob.concept_id = 161555
      AND ob.value_coded = 159
    GROUP BY enc.patient_id
    ON DUPLICATE KEY UPDATE last_updated_date = VALUES(last_updated_date);

    -- Status 2: Transférés (Transferred on ARV)
    INSERT INTO patient_status_arv(patient_id, id_status, start_date, encounter_id, last_updated_date, date_started_status)
    SELECT
        enc.patient_id,
        2 AS id_status,
        MAX(DATE(enc.encounter_datetime)) AS start_date,
        enc.encounter_id,
        NOW(),
        NOW()
    FROM tmp_encounter_snapshot enc
    INNER JOIN tmp_obs_snapshot ob
        ON enc.encounter_id = ob.encounter_id
       AND enc.patient_id = ob.person_id
    INNER JOIN tmp_patients_on_arv parv ON enc.patient_id = parv.patient_id
    WHERE enc.encounter_type = @et_discontinuation
      AND ob.concept_id = 161555
      AND ob.value_coded = 159492
    GROUP BY enc.patient_id
    ON DUPLICATE KEY UPDATE last_updated_date = VALUES(last_updated_date);

    -- Status 3: Arrêtés (Stopped ARV treatment)
    INSERT INTO patient_status_arv(patient_id, id_status, start_date, encounter_id, last_updated_date, date_started_status)
    SELECT
        enc.patient_id,
        3 AS id_status,
        MAX(DATE(enc.encounter_datetime)) AS start_date,
        enc.encounter_id,
        NOW(),
        NOW()
    FROM tmp_encounter_snapshot enc
    INNER JOIN tmp_obs_snapshot ob
        ON enc.encounter_id = ob.encounter_id
       AND enc.patient_id = ob.person_id
    INNER JOIN tmp_obs_snapshot_2 ob2
        ON ob.encounter_id = ob2.encounter_id
    INNER JOIN tmp_patients_on_arv parv ON enc.patient_id = parv.patient_id
    WHERE enc.encounter_type = @et_discontinuation
      AND ob.concept_id = 161555
      AND ob.value_coded = 1667
      AND ob2.concept_id = 1667
      AND ob2.value_coded IN (115198, 159737)
    GROUP BY enc.patient_id
    ON DUPLICATE KEY UPDATE last_updated_date = VALUES(last_updated_date);

    COMMIT;

    -- =========================================================================
    -- FINAL UPDATES
    -- =========================================================================
    START TRANSACTION;

    -- Update discontinuation reason
    UPDATE patient_status_arv psarv
    INNER JOIN discontinuation_reason dreason
        ON psarv.patient_id = dreason.patient_id
       AND psarv.start_date <= dreason.visit_date
    SET psarv.dis_reason = dreason.reason;

    -- Delete exposed infants from patient_status_arv
    DELETE psarv FROM patient_status_arv psarv
    INNER JOIN exposed_infants ei ON psarv.patient_id = ei.patient_id;

    COMMIT;

    -- Transaction: Update patient table (separate transaction to limit lock scope)
    START TRANSACTION;

    -- Reset arv_status
    UPDATE patient SET arv_status = NULL WHERE arv_status IS NOT NULL;

    -- Update patient table with last status
    DROP TEMPORARY TABLE IF EXISTS tmp_latest_status;
    CREATE TEMPORARY TABLE tmp_latest_status (
        patient_id INT NOT NULL,
        date_started_status DATETIME NOT NULL,
        PRIMARY KEY (patient_id)
    ) ENGINE=MEMORY
    SELECT patient_id, MAX(date_started_status) AS date_started_status
    FROM patient_status_arv
    GROUP BY patient_id;

    UPDATE patient p
    INNER JOIN patient_status_arv psa ON p.patient_id = psa.patient_id
    INNER JOIN tmp_latest_status B
        ON psa.patient_id = B.patient_id
       AND DATE(psa.date_started_status) = DATE(B.date_started_status)
    SET p.arv_status = psa.id_status;

    COMMIT;

    -- =========================================================================
    -- CLEANUP
    -- =========================================================================
    DROP TEMPORARY TABLE IF EXISTS tmp_latest_visit;
    DROP TEMPORARY TABLE IF EXISTS tmp_patients_on_arv;
    DROP TEMPORARY TABLE IF EXISTS tmp_discontinued_patients;
    DROP TEMPORARY TABLE IF EXISTS tmp_discontinued_pre_arv;
    DROP TEMPORARY TABLE IF EXISTS tmp_latest_dispensation;
    DROP TEMPORARY TABLE IF EXISTS tmp_latest_status;
    DROP TEMPORARY TABLE IF EXISTS tmp_obs_snapshot;
    DROP TEMPORARY TABLE IF EXISTS tmp_obs_snapshot_2;
    DROP TEMPORARY TABLE IF EXISTS tmp_encounter_snapshot;

END$$
DELIMITER ;

-- =============================================================================
-- isanteplusregimen_dml
--
-- Calculates ARV regimen combinations from patient prescriptions and updates
-- the pepfarTable and openmrs.isanteplus_patient_arv tables.
-- =============================================================================
DELIMITER $$
DROP PROCEDURE IF EXISTS isanteplusregimen_dml$$
CREATE PROCEDURE isanteplusregimen_dml()
BEGIN
    SET SQL_SAFE_UPDATES = 0;

    -- =========================================================================
    -- Cleanup temp tables from previous runs
    -- =========================================================================
    DROP TABLE IF EXISTS pepfarTableTemp;
    DROP TABLE IF EXISTS oneDrugRegimenPrefixTemp;
    DROP TABLE IF EXISTS twoDrugRegimenPrefixTemp;

    -- =========================================================================
    -- Remove voided prescriptions from pepfarTable
    -- =========================================================================
    DELETE pt FROM pepfarTable pt
    INNER JOIN patient_prescription pp
        ON pt.patient_id = pp.patient_id
       AND pt.location_id = pp.location_id
       AND DATE(pt.visit_date) = DATE(pp.visit_date)
    WHERE pp.voided = 1;

    -- =========================================================================
    -- Build regimen combinations
    -- =========================================================================

    -- Temp table for final results
    CREATE TEMPORARY TABLE pepfarTableTemp (
        location_id INT(11),
        patient_id INT(11),
        visit_date DATETIME,
        regimen VARCHAR(255),
        rx_or_prophy INT(11)
    );

    -- -------------------------------------------------------------------------
    -- Step 1: Find all single-drug ARV prescriptions
    -- -------------------------------------------------------------------------
    CREATE TEMPORARY TABLE oneDrugRegimenPrefixTemp (
        location_id INT(11),
        patient_id INT(11),
        visit_date DATETIME,
        drugID1 INT(11),
        rx_or_prophy INT(11)
    );

    INSERT INTO oneDrugRegimenPrefixTemp
    SELECT
        d1.location_id,
        d1.patient_id,
        d1.visit_date,
        d1.drug_id,
        d1.rx_or_prophy
    FROM patient_prescription d1
    INNER JOIN patient p ON d1.patient_id = p.patient_id
    INNER JOIN (SELECT DISTINCT drugID1 FROM regimen) r ON r.drugID1 = d1.drug_id
    WHERE d1.arv_drug = 1065
      AND d1.voided <> 1;

    -- Insert single-drug regimens (where drugID2 and drugID3 are 0)
    INSERT INTO pepfarTableTemp (location_id, patient_id, visit_date, regimen, rx_or_prophy)
    SELECT DISTINCT
        d1.location_id,
        d1.patient_id,
        d1.visit_date,
        r.shortname,
        d1.rx_or_prophy
    FROM oneDrugRegimenPrefixTemp d1
    INNER JOIN regimen r ON r.drugID1 = d1.drugID1
    WHERE r.drugID2 = 0
      AND r.drugID3 = 0;

    -- -------------------------------------------------------------------------
    -- Step 2: Find two-drug ARV combinations
    -- -------------------------------------------------------------------------
    CREATE TEMPORARY TABLE twoDrugRegimenPrefixTemp (
        location_id INT(11),
        patient_id INT(11),
        visit_date DATETIME,
        drugID1 INT(11),
        drugID2 INT(11),
        rx_or_prophy INT(11)
    );

    INSERT INTO twoDrugRegimenPrefixTemp
    SELECT
        d1.location_id,
        d1.patient_id,
        d1.visit_date,
        d1.drugID1,
        d2.drug_id,
        d1.rx_or_prophy
    FROM oneDrugRegimenPrefixTemp d1
    INNER JOIN patient_prescription d2 USING (location_id, patient_id, visit_date)
    INNER JOIN (SELECT DISTINCT drugID1, drugID2 FROM regimen) r
        ON r.drugID1 = d1.drugID1
       AND r.drugID2 = d2.drug_id
    WHERE d2.voided <> 1;

    -- Insert two-drug regimens (where drugID3 is 0)
    INSERT INTO pepfarTableTemp (location_id, patient_id, visit_date, regimen, rx_or_prophy)
    SELECT DISTINCT
        prefix.location_id,
        prefix.patient_id,
        prefix.visit_date,
        r.shortname,
        prefix.rx_or_prophy
    FROM twoDrugRegimenPrefixTemp prefix
    INNER JOIN regimen r
        ON prefix.drugID1 = r.drugID1
       AND prefix.drugID2 = r.drugID2
    WHERE r.drugID3 = 0;

    -- -------------------------------------------------------------------------
    -- Step 3: Find three-drug ARV combinations
    -- -------------------------------------------------------------------------
    INSERT INTO pepfarTableTemp (location_id, patient_id, visit_date, regimen, rx_or_prophy)
    SELECT DISTINCT
        prefix.location_id,
        prefix.patient_id,
        prefix.visit_date,
        r.shortname,
        prefix.rx_or_prophy
    FROM twoDrugRegimenPrefixTemp prefix
    INNER JOIN patient_prescription pp USING (location_id, patient_id, visit_date)
    INNER JOIN regimen r
        ON prefix.drugID1 = r.drugID1
       AND prefix.drugID2 = r.drugID2
       AND pp.drug_id = r.drugID3
    WHERE r.drugID3 != 0
      AND pp.voided <> 1;

    -- =========================================================================
    -- Update pepfarTable with results
    -- =========================================================================
    INSERT INTO pepfarTable (location_id, patient_id, visit_date, regimen, rx_or_prophy, last_updated_date)
    SELECT
        p.location_id,
        p.patient_id,
        p.visit_date,
        p.regimen,
        p.rx_or_prophy,
        NOW()
    FROM pepfarTableTemp p
    ON DUPLICATE KEY UPDATE
        rx_or_prophy = p.rx_or_prophy,
        last_updated_date = NOW();

    -- =========================================================================
    -- Update openmrs.isanteplus_patient_arv with latest regimen
    -- =========================================================================
    INSERT INTO openmrs.isanteplus_patient_arv (patient_id, arv_regimen, date_created, date_changed)
    SELECT
        pft.patient_id,
        pft.regimen,
        pft.visit_date,
        NOW()
    FROM pepfarTable pft
    INNER JOIN (
        SELECT pf.patient_id, MAX(pf.visit_date) AS visit_date_regimen
        FROM pepfarTable pf
        GROUP BY pf.patient_id
    ) B ON pft.patient_id = B.patient_id AND pft.visit_date = B.visit_date_regimen
    ON DUPLICATE KEY UPDATE
        arv_regimen = pft.regimen,
        date_changed = NOW();

    -- Cleanup temp tables
    DROP TEMPORARY TABLE oneDrugRegimenPrefixTemp;
    DROP TEMPORARY TABLE twoDrugRegimenPrefixTemp;
    DROP TEMPORARY TABLE pepfarTableTemp;

    -- =========================================================================
    -- Transfer ARV status info to openmrs.isanteplus_patient_arv
    -- =========================================================================
    INSERT INTO openmrs.isanteplus_patient_arv
        (patient_id, arv_status, date_started_arv, next_visit_date, date_created, date_changed)
    SELECT
        p.patient_id,
        asl.name_fr,
        DATE(p.date_started_arv),
        DATE(p.next_visit_date),
        NOW(),
        NOW()
    FROM isanteplus.patient p
    LEFT OUTER JOIN isanteplus.arv_status_loockup asl ON p.arv_status = asl.id
    WHERE p.arv_status IS NOT NULL
       OR p.next_visit_date IS NOT NULL
       OR p.date_started_arv IS NOT NULL
    ON DUPLICATE KEY UPDATE
        arv_status = asl.name_fr,
        date_started_arv = p.date_started_arv,
        next_visit_date = p.next_visit_date,
        date_changed = NOW();

END$$
DELIMITER ;

-- =============================================================================
-- alert_viral_load
--
-- Generates clinical alerts for patients based on viral load results,
-- ARV dispensation, TB co-infection, and INH prophylaxis status.
-- Populates the alert table with alert codes (1-12).
-- =============================================================================
DELIMITER $$
DROP PROCEDURE IF EXISTS alert_viral_load$$
CREATE PROCEDURE alert_viral_load()
BEGIN
    SET SQL_SAFE_UPDATES = 0;

    -- =========================================================================
    -- PHASE 1: READ FROM OPENMRS TABLES (LOW ISOLATION)
    -- Use READ UNCOMMITTED to avoid blocking production queries
    -- =========================================================================

    SET SESSION TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
    START TRANSACTION;

    -- -------------------------------------------------------------------------
    -- Resolve encounter type and concept UUIDs to IDs
    -- -------------------------------------------------------------------------
    SET @et_discontinuation := (
        SELECT encounter_type_id FROM openmrs.encounter_type
        WHERE uuid = '9d0113c6-f23a-4461-8428-7e9a7344f2ba'
    );
    SET @et_first_visit := (
        SELECT encounter_type_id FROM openmrs.encounter_type
        WHERE uuid = '17536ba6-dd7c-4f58-8014-08c7cb798ac7'
    );
    SET @et_followup := (
        SELECT encounter_type_id FROM openmrs.encounter_type
        WHERE uuid = '204ad066-c5c2-4229-9a62-644bc5617ca2'
    );
    SET @et_pediatric := (
        SELECT encounter_type_id FROM openmrs.encounter_type
        WHERE uuid = '349ae0b4-65c1-4122-aa06-480f186c8350'
    );
    SET @et_pediatric_followup := (
        SELECT encounter_type_id FROM openmrs.encounter_type
        WHERE uuid = '33491314-c352-42d0-bd5d-a9d0bffc9bf1'
    );
    SET @et_dispensing1 := (
        SELECT encounter_type_id FROM openmrs.encounter_type
        WHERE uuid = '10d73929-54b6-4d18-a647-8b7316bc1ae3'
    );
    SET @et_dispensing2 := (
        SELECT encounter_type_id FROM openmrs.encounter_type
        WHERE uuid = 'a9392241-109f-4d67-885b-57cc4b8c638f'
    );

    SET @concept_isoniazid_group := (
        SELECT concept_id FROM openmrs.concept
        WHERE uuid = 'fee8bd39-2a95-47f9-b1f5-3f9e9b3ee959'
    );
    SET @concept_rifampicin_group := (
        SELECT concept_id FROM openmrs.concept
        WHERE uuid = '2b2053bd-37f3-429d-be0b-f1f8952fe55e'
    );
    SET @concept_ddp := (
        SELECT concept_id FROM openmrs.concept
        WHERE uuid = 'c2aacdc8-156e-4527-8934-a8fb94162419'
    );

    -- -------------------------------------------------------------------------
    -- Pre-fetch ALL needed obs data in a SINGLE SCAN of openmrs.obs
    -- -------------------------------------------------------------------------
    DROP TEMPORARY TABLE IF EXISTS tmp_obs_snapshot;
    CREATE TEMPORARY TABLE tmp_obs_snapshot (
        obs_id INT NOT NULL,
        person_id INT NOT NULL,
        encounter_id INT,
        concept_id INT NOT NULL,
        value_coded INT,
        value_numeric DOUBLE,
        value_datetime DATETIME,
        obs_datetime DATETIME,
        obs_group_id INT,
        location_id INT,
        voided TINYINT,
        PRIMARY KEY (obs_id),
        KEY idx_person_concept (person_id, concept_id),
        KEY idx_encounter (encounter_id),
        KEY idx_concept_value (concept_id, value_coded),
        KEY idx_obs_group (obs_group_id)
    )
    SELECT
        o.obs_id,
        o.person_id,
        o.encounter_id,
        o.concept_id,
        o.value_coded,
        o.value_numeric,
        o.value_datetime,
        o.obs_datetime,
        o.obs_group_id,
        o.location_id,
        o.voided
    FROM openmrs.obs o
    WHERE o.concept_id IN (
        856,     -- Viral load numeric
        1305,    -- Viral load qualitative
        1282,    -- Drug order
        159367,  -- Drug status
        @concept_ddp
    )
    AND o.voided <> 1;

    -- Second copy of obs for self-joins (MySQL can't reopen temp tables)
    DROP TEMPORARY TABLE IF EXISTS tmp_obs_snapshot_2;
    CREATE TEMPORARY TABLE tmp_obs_snapshot_2 (
        obs_id INT NOT NULL,
        person_id INT NOT NULL,
        encounter_id INT,
        concept_id INT NOT NULL,
        value_coded INT,
        obs_group_id INT,
        PRIMARY KEY (obs_id),
        KEY idx_obs_group (obs_group_id),
        KEY idx_encounter_concept (encounter_id, concept_id)
    )
    SELECT
        o.obs_id,
        o.person_id,
        o.encounter_id,
        o.concept_id,
        o.value_coded,
        o.obs_group_id
    FROM openmrs.obs o
    WHERE o.concept_id IN (1282, 159367)  -- Drug order and Drug status (needed for TB alert self-joins)
      AND o.voided <> 1;

    -- Separate snapshot for obs group lookups (INH/Rifampicin groups)
    DROP TEMPORARY TABLE IF EXISTS tmp_obs_group_snapshot;
    CREATE TEMPORARY TABLE tmp_obs_group_snapshot (
        obs_id INT NOT NULL,
        person_id INT NOT NULL,
        encounter_id INT,
        concept_id INT NOT NULL,
        obs_group_id INT,
        PRIMARY KEY (obs_id),
        KEY idx_obs_group (obs_group_id)
    )
    SELECT
        o.obs_id,
        o.person_id,
        o.encounter_id,
        o.concept_id,
        o.obs_group_id
    FROM openmrs.obs o
    WHERE o.concept_id IN (@concept_isoniazid_group, @concept_rifampicin_group)
      AND o.voided <> 1;

    -- Pre-fetch encounter data needed for joins
    DROP TEMPORARY TABLE IF EXISTS tmp_encounter_snapshot;
    CREATE TEMPORARY TABLE tmp_encounter_snapshot (
        encounter_id INT NOT NULL,
        patient_id INT NOT NULL,
        visit_id INT,
        encounter_type INT,
        encounter_datetime DATETIME,
        voided TINYINT,
        PRIMARY KEY (encounter_id),
        KEY idx_patient (patient_id),
        KEY idx_visit (visit_id),
        KEY idx_type (encounter_type)
    )
    SELECT
        e.encounter_id,
        e.patient_id,
        e.visit_id,
        e.encounter_type,
        e.encounter_datetime,
        e.voided
    FROM openmrs.encounter e
    WHERE e.voided <> 1;

    -- Patients with discontinuation encounter
    DROP TEMPORARY TABLE IF EXISTS tmp_discontinued_patients;
    CREATE TEMPORARY TABLE tmp_discontinued_patients (
        patient_id INT NOT NULL,
        PRIMARY KEY (patient_id)
    ) ENGINE=MEMORY
    SELECT DISTINCT enc.patient_id
    FROM tmp_encounter_snapshot enc
    INNER JOIN isanteplus.discontinuation_reason dr ON enc.patient_id = dr.patient_id
    WHERE enc.encounter_type = @et_discontinuation;

    -- Patients with any discontinuation encounter (without reason check)
    DROP TEMPORARY TABLE IF EXISTS tmp_any_discontinuation;
    CREATE TEMPORARY TABLE tmp_any_discontinuation (
        patient_id INT NOT NULL,
        PRIMARY KEY (patient_id)
    ) ENGINE=MEMORY
    SELECT DISTINCT patient_id
    FROM tmp_encounter_snapshot
    WHERE encounter_type = @et_discontinuation;

    -- Latest obs datetime for viral load concepts
    DROP TEMPORARY TABLE IF EXISTS tmp_latest_obs_viral_load;
    CREATE TEMPORARY TABLE tmp_latest_obs_viral_load (
        person_id INT NOT NULL,
        obs_date DATE NOT NULL,
        PRIMARY KEY (person_id)
    ) ENGINE=MEMORY
    SELECT
        o.person_id,
        MAX(DATE(o.obs_datetime)) AS obs_date
    FROM tmp_obs_snapshot o
    WHERE o.concept_id IN (856, 1305)
    GROUP BY o.person_id;

    -- Commit the read transaction - releases any read locks on openmrs tables
    COMMIT;

    -- =========================================================================
    -- PHASE 2: READ FROM ISANTEPLUS TABLES
    -- =========================================================================

    START TRANSACTION;

    -- First ARV dispensation per patient
    DROP TEMPORARY TABLE IF EXISTS tmp_first_arv_dispensation;
    CREATE TEMPORARY TABLE tmp_first_arv_dispensation (
        patient_id INT NOT NULL,
        encounter_id INT,
        visit_date DATE NOT NULL,
        PRIMARY KEY (patient_id)
    ) ENGINE=MEMORY
    SELECT
        pdis.patient_id,
        MIN(pdis.encounter_id) AS encounter_id,
        MIN(DATE(pdis.visit_date)) AS visit_date
    FROM isanteplus.patient_dispensing pdis
    WHERE pdis.arv_drug = 1065
      AND pdis.voided <> 1
    GROUP BY pdis.patient_id;

    -- Patients with viral load results
    DROP TEMPORARY TABLE IF EXISTS tmp_patients_with_viral_load;
    CREATE TEMPORARY TABLE tmp_patients_with_viral_load (
        patient_id INT NOT NULL,
        PRIMARY KEY (patient_id)
    ) ENGINE=MEMORY
    SELECT DISTINCT pl.patient_id
    FROM isanteplus.patient_laboratory pl
    WHERE pl.test_id IN (856, 1305)
      AND pl.test_done = 1
      AND pl.voided <> 1
      AND pl.test_result IS NOT NULL
      AND pl.test_result <> '';

    -- Patients on ARV
    DROP TEMPORARY TABLE IF EXISTS tmp_patients_on_arv;
    CREATE TEMPORARY TABLE tmp_patients_on_arv (
        patient_id INT NOT NULL,
        PRIMARY KEY (patient_id)
    ) ENGINE=MEMORY
    SELECT patient_id FROM isanteplus.patient_on_arv;

    -- Latest dispensation date per patient
    DROP TEMPORARY TABLE IF EXISTS tmp_latest_dispensation;
    CREATE TEMPORARY TABLE tmp_latest_dispensation (
        patient_id INT NOT NULL,
        next_dispensation_date DATE,
        PRIMARY KEY (patient_id)
    ) ENGINE=MEMORY
    SELECT
        pd.patient_id,
        MAX(pd.next_dispensation_date) AS next_dispensation_date
    FROM isanteplus.patient_dispensing pd
    WHERE pd.arv_drug = 1065
      AND (pd.rx_or_prophy <> 163768 OR pd.rx_or_prophy IS NULL)
      AND pd.voided <> 1
    GROUP BY pd.patient_id;

    -- Latest viral load test date per patient
    DROP TEMPORARY TABLE IF EXISTS tmp_latest_viral_load;
    CREATE TEMPORARY TABLE tmp_latest_viral_load (
        patient_id INT NOT NULL,
        visit_date DATE NOT NULL,
        PRIMARY KEY (patient_id)
    ) ENGINE=MEMORY
    SELECT
        pl.patient_id,
        MAX(IFNULL(DATE(pl.date_test_done), DATE(pl.visit_date))) AS visit_date
    FROM isanteplus.patient_laboratory pl
    WHERE pl.test_id IN (856, 1305)
      AND pl.test_result > 0
      AND pl.voided <> 1
    GROUP BY pl.patient_id;

    -- Latest dispensing visit date per patient
    DROP TEMPORARY TABLE IF EXISTS tmp_latest_dispensing_visit;
    CREATE TEMPORARY TABLE tmp_latest_dispensing_visit (
        patient_id INT NOT NULL,
        visit_date DATE NOT NULL,
        PRIMARY KEY (patient_id)
    ) ENGINE=MEMORY
    SELECT
        pdi.patient_id,
        MAX(DATE(pdi.visit_date)) AS visit_date
    FROM isanteplus.patient_dispensing pdi
    WHERE pdi.voided <> 1
    GROUP BY pdi.patient_id;

    -- Patients with INH prophylaxis (now using snapshot)
    DROP TEMPORARY TABLE IF EXISTS tmp_patients_with_inh;
    CREATE TEMPORARY TABLE tmp_patients_with_inh (
        patient_id INT NOT NULL,
        PRIMARY KEY (patient_id)
    ) ENGINE=MEMORY
    SELECT DISTINCT o.person_id AS patient_id
    FROM tmp_obs_snapshot o
    INNER JOIN tmp_encounter_snapshot e
        ON o.encounter_id = e.encounter_id
       AND o.person_id = e.patient_id
    WHERE e.encounter_type IN (@et_dispensing1, @et_dispensing2)
      AND o.concept_id = 1282
      AND o.value_coded = 78280;

    COMMIT;

    -- =========================================================================
    -- PHASE 3: WRITE TO ISANTEPLUS TABLES (NORMAL ISOLATION)
    -- =========================================================================

    SET SESSION TRANSACTION ISOLATION LEVEL REPEATABLE READ;
    START TRANSACTION;

    TRUNCATE TABLE alert;

    -- -------------------------------------------------------------------------
    -- Alert 1: Patient on ARV >= 6 months without viral load result
    -- -------------------------------------------------------------------------
    INSERT INTO alert(patient_id, id_alert, encounter_id, date_alert, last_updated_date)
    SELECT DISTINCT
        B.patient_id,
        1,
        B.encounter_id,
        B.visit_date,
        NOW()
    FROM isanteplus.patient p
    INNER JOIN tmp_first_arv_dispensation B ON p.patient_id = B.patient_id
    LEFT JOIN tmp_patients_with_viral_load vl ON p.patient_id = vl.patient_id
    LEFT JOIN tmp_discontinued_patients disc ON p.patient_id = disc.patient_id
    WHERE p.date_started_arv = B.visit_date
      AND TIMESTAMPDIFF(MONTH, p.date_started_arv, CURDATE()) >= 6
      AND vl.patient_id IS NULL
      AND disc.patient_id IS NULL
      AND p.vih_status = 1;

    -- -------------------------------------------------------------------------
    -- Alert 2: Patient on ARV = 5 months without viral load result
    -- -------------------------------------------------------------------------
    INSERT INTO alert(patient_id, id_alert, encounter_id, date_alert, last_updated_date)
    SELECT DISTINCT
        B.patient_id,
        2,
        B.encounter_id,
        B.visit_date,
        NOW()
    FROM isanteplus.patient p
    INNER JOIN tmp_first_arv_dispensation B ON p.patient_id = B.patient_id
    LEFT JOIN tmp_patients_with_viral_load vl ON p.patient_id = vl.patient_id
    LEFT JOIN tmp_any_discontinuation disc ON p.patient_id = disc.patient_id
    WHERE p.date_started_arv = B.visit_date
      AND TIMESTAMPDIFF(MONTH, p.date_started_arv, CURDATE()) = 5
      AND vl.patient_id IS NULL
      AND disc.patient_id IS NULL
      AND p.vih_status = 1;

    -- -------------------------------------------------------------------------
    -- Alert 3: Pregnant woman on ARV >= 4 months without viral load result
    -- -------------------------------------------------------------------------
    INSERT INTO alert(patient_id, id_alert, encounter_id, date_alert, last_updated_date)
    SELECT DISTINCT
        B.patient_id,
        3,
        B.encounter_id,
        B.visit_date,
        NOW()
    FROM isanteplus.patient p
    INNER JOIN tmp_first_arv_dispensation B ON p.patient_id = B.patient_id
    INNER JOIN isanteplus.patient_pregnancy pp ON p.patient_id = pp.patient_id
    LEFT JOIN tmp_patients_with_viral_load vl ON p.patient_id = vl.patient_id
    LEFT JOIN tmp_discontinued_patients disc ON p.patient_id = disc.patient_id
    WHERE p.date_started_arv = B.visit_date
      AND TIMESTAMPDIFF(MONTH, p.date_started_arv, CURDATE()) >= 4
      AND vl.patient_id IS NULL
      AND disc.patient_id IS NULL
      AND p.vih_status = 1;

    -- -------------------------------------------------------------------------
    -- Alert 4: Last viral load >= 12 months ago (suppressed)
    -- -------------------------------------------------------------------------
    INSERT INTO alert(patient_id, id_alert, encounter_id, date_alert, last_updated_date)
    SELECT DISTINCT
        plab.patient_id,
        4,
        plab.encounter_id,
        IFNULL(DATE(plab.date_test_done), DATE(plab.visit_date)),
        NOW()
    FROM isanteplus.patient p
    INNER JOIN isanteplus.patient_laboratory plab ON p.patient_id = plab.patient_id
    INNER JOIN tmp_latest_viral_load C ON plab.patient_id = C.patient_id
    INNER JOIN tmp_patients_on_arv parv ON p.patient_id = parv.patient_id
    WHERE IFNULL(DATE(plab.date_test_done), DATE(plab.visit_date)) = C.visit_date
      AND TIMESTAMPDIFF(MONTH, C.visit_date, CURDATE()) >= 12
      AND ((plab.test_id = 856 AND plab.test_result < 1000)
           OR (plab.test_id = 1305 AND plab.test_result = 1306))
      AND p.arv_status NOT IN (1, 2, 3)
      AND p.vih_status = 1;

    -- -------------------------------------------------------------------------
    -- Alert 5: Last viral load >= 3 months ago with result > 1000 copies/ml
    -- (Using snapshot instead of openmrs.obs directly)
    -- -------------------------------------------------------------------------
    INSERT INTO alert(patient_id, id_alert, encounter_id, date_alert, last_updated_date)
    SELECT DISTINCT
        ob.person_id,
        5,
        ob.encounter_id,
        DATE(ob.obs_datetime),
        NOW()
    FROM tmp_obs_snapshot ob
    INNER JOIN isanteplus.patient p ON ob.person_id = p.patient_id
    INNER JOIN tmp_latest_obs_viral_load B ON ob.person_id = B.person_id
    WHERE DATE(ob.obs_datetime) = B.obs_date
      AND ((ob.concept_id = 856 AND ob.value_numeric > 1000)
           OR (ob.concept_id = 1305 AND ob.value_coded = 1301))
      AND TIMESTAMPDIFF(MONTH, DATE(ob.obs_datetime), CURDATE()) >= 3
      AND p.arv_status NOT IN (1, 2, 3);

    -- -------------------------------------------------------------------------
    -- Alert 6: Last viral load > 1000 copies/ml
    -- -------------------------------------------------------------------------
    INSERT INTO alert(patient_id, id_alert, encounter_id, date_alert, last_updated_date)
    SELECT DISTINCT
        plab.patient_id,
        6,
        plab.encounter_id,
        IFNULL(DATE(plab.date_test_done), DATE(plab.visit_date)),
        NOW()
    FROM isanteplus.patient p
    INNER JOIN isanteplus.patient_laboratory plab ON p.patient_id = plab.patient_id
    INNER JOIN (
        SELECT pl.patient_id, MAX(IFNULL(DATE(pl.date_test_done), DATE(pl.visit_date))) AS visit_date
        FROM isanteplus.patient_laboratory pl
        WHERE pl.test_id = 856
          AND pl.test_done = 1
          AND pl.voided <> 1
          AND pl.test_result IS NOT NULL
          AND pl.test_result <> ''
        GROUP BY pl.patient_id
    ) C ON plab.patient_id = C.patient_id
    INNER JOIN tmp_patients_on_arv parv ON p.patient_id = parv.patient_id
    LEFT JOIN tmp_discontinued_patients disc ON p.patient_id = disc.patient_id
    WHERE IFNULL(DATE(plab.date_test_done), DATE(plab.visit_date)) = C.visit_date
      AND plab.test_id = 856
      AND plab.test_result > 1000
      AND disc.patient_id IS NULL
      AND p.vih_status = 1;

    -- -------------------------------------------------------------------------
    -- Alert 7: Patient must refill ARV within 30 days
    -- -------------------------------------------------------------------------
    INSERT INTO alert(patient_id, id_alert, encounter_id, date_alert, last_updated_date)
    SELECT DISTINCT
        pdisp.patient_id,
        7,
        pdisp.encounter_id,
        DATE(pdisp.visit_date),
        NOW()
    FROM isanteplus.patient p
    INNER JOIN isanteplus.patient_dispensing pdisp ON p.patient_id = pdisp.patient_id
    INNER JOIN tmp_latest_dispensation B
        ON pdisp.patient_id = B.patient_id
       AND pdisp.next_dispensation_date = B.next_dispensation_date
    WHERE DATEDIFF(pdisp.next_dispensation_date, CURDATE()) BETWEEN 0 AND 30
      AND p.arv_status NOT IN (1, 2, 3);

    -- -------------------------------------------------------------------------
    -- Alert 8: Patient has no more medications available
    -- -------------------------------------------------------------------------
    INSERT INTO alert(patient_id, id_alert, encounter_id, date_alert, last_updated_date)
    SELECT DISTINCT
        pdisp.patient_id,
        8,
        pdisp.encounter_id,
        DATE(pdisp.visit_date),
        NOW()
    FROM isanteplus.patient p
    INNER JOIN isanteplus.patient_dispensing pdisp ON p.patient_id = pdisp.patient_id
    INNER JOIN tmp_latest_dispensation B
        ON pdisp.patient_id = B.patient_id
       AND pdisp.next_dispensation_date = B.next_dispensation_date
    WHERE DATEDIFF(B.next_dispensation_date, CURDATE()) < 0
      AND p.arv_status NOT IN (1, 2, 3);

    COMMIT;

    -- -------------------------------------------------------------------------
    -- Alert 9: TB/HIV co-infection (separate transaction)
    -- -------------------------------------------------------------------------
    START TRANSACTION;

    -- TB/HIV from dispensing form
    DROP TABLE IF EXISTS traitement_tuberculeux;
    CREATE TABLE traitement_tuberculeux (
        patient_id INT NOT NULL,
        id_alert INT,
        encounter_id INT,
        drug_id INT,
        visit_date DATE,
        last_updated_date DATETIME,
        KEY idx_patient_drug (patient_id, drug_id)
    )
    SELECT DISTINCT
        pd.patient_id,
        9 AS id_alert,
        pd.encounter_id,
        pd.drug_id,
        DATE(pd.visit_date) AS visit_date,
        NOW() AS last_updated_date
    FROM isanteplus.patient_dispensing pd
    INNER JOIN tmp_patients_on_arv poa ON pd.patient_id = poa.patient_id
    INNER JOIN tmp_latest_dispensing_visit B
        ON pd.patient_id = B.patient_id
       AND DATE(pd.visit_date) = B.visit_date
    WHERE pd.rx_or_prophy = 138405
      AND pd.drug_id = 78280
      AND pd.voided <> 1;

    -- Rifampicin
    INSERT INTO traitement_tuberculeux(patient_id, id_alert, encounter_id, drug_id, visit_date, last_updated_date)
    SELECT DISTINCT
        pd.patient_id,
        9,
        pd.encounter_id,
        pd.drug_id,
        DATE(pd.visit_date),
        NOW()
    FROM isanteplus.patient_dispensing pd
    INNER JOIN tmp_patients_on_arv poa ON pd.patient_id = poa.patient_id
    INNER JOIN tmp_latest_dispensing_visit B
        ON pd.patient_id = B.patient_id
       AND DATE(pd.visit_date) = B.visit_date
    WHERE pd.drug_id = 767
      AND pd.voided <> 1;

    -- Insert alert for patients with both drugs
    INSERT INTO alert(patient_id, id_alert, encounter_id, date_alert, last_updated_date)
    SELECT DISTINCT
        tb.patient_id,
        tb.id_alert,
        tb.encounter_id,
        tb.visit_date,
        tb.last_updated_date
    FROM traitement_tuberculeux tb
    INNER JOIN traitement_tuberculeux tb1
        ON tb.patient_id = tb1.patient_id
       AND tb.visit_date = tb1.visit_date
       AND tb.encounter_id = tb1.encounter_id
    INNER JOIN isanteplus.patient p ON tb.patient_id = p.patient_id
    WHERE tb.drug_id = 78280
      AND tb1.drug_id = 767
      AND p.arv_status NOT IN (1, 2, 3);

    DROP TABLE IF EXISTS traitement_tuberculeux;

    COMMIT;

    -- -------------------------------------------------------------------------
    -- Alert 9: TB/HIV from HIV visit forms (separate transaction)
    -- -------------------------------------------------------------------------
    START TRANSACTION;

    DROP TEMPORARY TABLE IF EXISTS tmp_latest_hiv_encounter;
    CREATE TEMPORARY TABLE tmp_latest_hiv_encounter (
        patient_id INT NOT NULL,
        visit_date DATETIME NOT NULL,
        PRIMARY KEY (patient_id)
    ) ENGINE=MEMORY
    SELECT
        en.patient_id,
        MAX(en.encounter_datetime) AS visit_date
    FROM tmp_encounter_snapshot en
    WHERE en.encounter_type IN (@et_first_visit, @et_followup, @et_pediatric, @et_pediatric_followup)
    GROUP BY en.patient_id;

    -- Isoniazid from HIV forms (using snapshots)
    CREATE TABLE traitement_tuberculeux (
        patient_id INT NOT NULL,
        id_alert INT,
        encounter_id INT,
        drug_id INT,
        visit_date DATE,
        last_updated_date DATETIME,
        KEY idx_patient_drug (patient_id, drug_id)
    )
    SELECT DISTINCT
        o.person_id AS patient_id,
        9 AS id_alert,
        o.encounter_id,
        o.value_coded AS drug_id,
        DATE(e.encounter_datetime) AS visit_date,
        NOW() AS last_updated_date
    FROM tmp_obs_group_snapshot o1
    INNER JOIN tmp_obs_snapshot_2 o2 ON o1.obs_id = o2.obs_group_id
    INNER JOIN tmp_obs_snapshot o ON o2.obs_group_id = o.obs_group_id
    INNER JOIN tmp_encounter_snapshot e ON o.encounter_id = e.encounter_id AND o.person_id = e.patient_id
    INNER JOIN tmp_latest_hiv_encounter B
        ON e.patient_id = B.patient_id
       AND DATE(e.encounter_datetime) = DATE(B.visit_date)
    WHERE o1.concept_id = @concept_isoniazid_group
      AND o.concept_id = 1282
      AND o.value_coded = 78280
      AND o2.concept_id = 159367
      AND o2.value_coded = 1065;

    -- Rifampicin from HIV forms
    INSERT INTO traitement_tuberculeux(patient_id, id_alert, encounter_id, drug_id, visit_date, last_updated_date)
    SELECT DISTINCT
        o.person_id AS patient_id,
        9 AS id_alert,
        o.encounter_id,
        o.value_coded AS drug_id,
        DATE(e.encounter_datetime) AS visit_date,
        NOW() AS last_updated_date
    FROM tmp_obs_group_snapshot o1
    INNER JOIN tmp_obs_snapshot_2 o2 ON o1.obs_id = o2.obs_group_id
    INNER JOIN tmp_obs_snapshot o ON o2.obs_group_id = o.obs_group_id
    INNER JOIN tmp_encounter_snapshot e ON o.encounter_id = e.encounter_id AND o.person_id = e.patient_id
    INNER JOIN tmp_latest_hiv_encounter B
        ON e.patient_id = B.patient_id
       AND DATE(e.encounter_datetime) = DATE(B.visit_date)
    WHERE o1.concept_id = @concept_rifampicin_group
      AND o.concept_id = 1282
      AND o.value_coded = 767
      AND o2.concept_id = 159367
      AND o2.value_coded = 1065;

    -- Insert alert for patients with both drugs from HIV forms
    INSERT INTO alert(patient_id, id_alert, encounter_id, date_alert, last_updated_date)
    SELECT DISTINCT
        tb.patient_id,
        tb.id_alert,
        tb.encounter_id,
        tb.visit_date,
        tb.last_updated_date
    FROM traitement_tuberculeux tb
    INNER JOIN traitement_tuberculeux tb1
        ON tb.patient_id = tb1.patient_id
       AND tb.visit_date = tb1.visit_date
       AND tb.encounter_id = tb1.encounter_id
    INNER JOIN isanteplus.patient p ON tb.patient_id = p.patient_id
    WHERE tb.drug_id = 78280
      AND tb1.drug_id = 767
      AND p.arv_status NOT IN (1, 2, 3);

    DROP TABLE IF EXISTS traitement_tuberculeux;
    DROP TEMPORARY TABLE IF EXISTS tmp_latest_hiv_encounter;

    COMMIT;

    -- -------------------------------------------------------------------------
    -- Alert 10: Patient on ARV >= 3 months without viral load result
    -- -------------------------------------------------------------------------
    START TRANSACTION;

    DROP TEMPORARY TABLE IF EXISTS tmp_patient_viral_load;
    CREATE TEMPORARY TABLE tmp_patient_viral_load (
        patient_id INT NOT NULL,
        PRIMARY KEY (patient_id)
    ) ENGINE=MEMORY
    SELECT DISTINCT pl.patient_id
    FROM isanteplus.patient_laboratory pl
    WHERE pl.test_id = 856
      AND pl.test_result IS NOT NULL
      AND pl.test_result <> ''
      AND pl.voided <> 1;

    INSERT INTO tmp_patient_viral_load(patient_id)
    SELECT pl1.patient_id
    FROM isanteplus.patient_laboratory pl1
    WHERE pl1.test_id = 1305
      AND pl1.test_result IN (1301, 1306)
      AND pl1.voided <> 1
    ON DUPLICATE KEY UPDATE patient_id = VALUES(patient_id);

    INSERT INTO alert(patient_id, id_alert, encounter_id, date_alert, last_updated_date)
    SELECT DISTINCT
        B.patient_id,
        10,
        B.encounter_id,
        B.visit_date,
        NOW()
    FROM isanteplus.patient p
    INNER JOIN isanteplus.patient_dispensing padis ON p.patient_id = padis.patient_id
    INNER JOIN tmp_first_arv_dispensation B
        ON padis.patient_id = B.patient_id
       AND DATE(padis.visit_date) = B.visit_date
    LEFT JOIN tmp_patient_viral_load pvl ON p.patient_id = pvl.patient_id
    WHERE TIMESTAMPDIFF(MONTH, B.visit_date, CURDATE()) >= 3
      AND pvl.patient_id IS NULL
      AND p.arv_status NOT IN (1, 2, 3, 4)
      AND p.vih_status = 1
      AND padis.voided <> 1;

    DROP TEMPORARY TABLE IF EXISTS tmp_patient_viral_load;

    COMMIT;

    -- -------------------------------------------------------------------------
    -- Alert 11: Patient on ARV without INH prophylaxis
    -- -------------------------------------------------------------------------
    START TRANSACTION;

    INSERT INTO alert(patient_id, id_alert, encounter_id, date_alert, last_updated_date)
    SELECT DISTINCT
        pdisp.patient_id,
        11,
        pdisp.encounter_id,
        DATE(pdisp.visit_date),
        NOW()
    FROM isanteplus.patient p
    INNER JOIN isanteplus.patient_dispensing pdisp ON p.patient_id = pdisp.patient_id
    LEFT JOIN tmp_patients_with_inh inh ON p.patient_id = inh.patient_id
    WHERE pdisp.arv_drug = 1065
      AND pdisp.rx_or_prophy <> 163768
      AND p.arv_status NOT IN (1, 2, 3)
      AND p.vih_status = 1
      AND inh.patient_id IS NULL;

    COMMIT;

    -- -------------------------------------------------------------------------
    -- Alert 12: DDP subscription (using snapshot)
    -- -------------------------------------------------------------------------
    START TRANSACTION;

    INSERT INTO alert(patient_id, id_alert, encounter_id, date_alert, last_updated_date)
    SELECT DISTINCT
        o.person_id,
        12,
        o.encounter_id,
        DATE(o.obs_datetime),
        NOW()
    FROM tmp_obs_snapshot o
    WHERE o.concept_id = @concept_ddp
      AND o.value_coded = 1065;

    COMMIT;

    -- =========================================================================
    -- CLEANUP
    -- =========================================================================
    DROP TEMPORARY TABLE IF EXISTS tmp_first_arv_dispensation;
    DROP TEMPORARY TABLE IF EXISTS tmp_patients_with_viral_load;
    DROP TEMPORARY TABLE IF EXISTS tmp_patients_on_arv;
    DROP TEMPORARY TABLE IF EXISTS tmp_latest_dispensation;
    DROP TEMPORARY TABLE IF EXISTS tmp_latest_viral_load;
    DROP TEMPORARY TABLE IF EXISTS tmp_latest_obs_viral_load;
    DROP TEMPORARY TABLE IF EXISTS tmp_latest_dispensing_visit;
    DROP TEMPORARY TABLE IF EXISTS tmp_discontinued_patients;
    DROP TEMPORARY TABLE IF EXISTS tmp_any_discontinuation;
    DROP TEMPORARY TABLE IF EXISTS tmp_patients_with_inh;
    DROP TEMPORARY TABLE IF EXISTS tmp_obs_snapshot;
    DROP TEMPORARY TABLE IF EXISTS tmp_obs_snapshot_2;
    DROP TEMPORARY TABLE IF EXISTS tmp_obs_group_snapshot;
    DROP TEMPORARY TABLE IF EXISTS tmp_encounter_snapshot;

END$$
DELIMITER ;

-- =============================================================================
-- isanteplus_patient_alert
--
-- Configures the OpenMRS Patient Flags module with alert definitions.
-- Maps isanteplus.alert table entries to patient flag displays.
-- =============================================================================
DELIMITER $$
DROP PROCEDURE IF EXISTS isanteplus_patient_alert$$
CREATE PROCEDURE isanteplus_patient_alert()
BEGIN
    SET SQL_SAFE_UPDATES = 0;
    SET FOREIGN_KEY_CHECKS = 0;

    -- =========================================================================
    -- Clear existing flag configuration
    -- =========================================================================
    TRUNCATE TABLE openmrs.patientflags_flag_tag;
    TRUNCATE TABLE openmrs.patientflags_tag_displaypoint;
    TRUNCATE TABLE openmrs.patientflags_flag;
    TRUNCATE TABLE openmrs.patientflags_tag;
    TRUNCATE TABLE openmrs.patientflags_priority;

    SET SQL_SAFE_UPDATES = 1;
    SET FOREIGN_KEY_CHECKS = 1;

    -- =========================================================================
    -- Create tag for alerts
    -- =========================================================================
    INSERT INTO openmrs.patientflags_tag VALUES (
        2,                                          -- tag_id
        'Tag',                                      -- name
        NULL,                                       -- description
        1,                                          -- creator
        '2018-05-28 09:44:50',                      -- date_created
        NULL, NULL, 0, NULL, NULL, NULL,
        '4dbe134d-a67a-44be-871f-5890b05d328c'      -- uuid
    );

    -- =========================================================================
    -- Create priority levels
    -- =========================================================================
    -- Priority 1: Viral Load alerts (red)
    INSERT INTO openmrs.patientflags_priority VALUES (
        1, 'Liste VL', 'color:red', 1, NULL,
        1, '2018-05-28 02:17:38', 1, '2018-05-28 02:19:27',
        0, NULL, NULL, NULL,
        'f2e0e461-170e-4df9-80fc-da2d93663328'
    );

    -- Priority 2: Medication alerts (red)
    INSERT INTO openmrs.patientflags_priority VALUES (
        2, 'Liste Medicament', 'color: red', 2, NULL,
        1, '2018-05-31 15:02:47', NULL, NULL,
        0, NULL, NULL, NULL,
        '5d87ef2b-5cc2-4ef5-a241-a122977170d6'
    );

    -- Priority 3: TB alerts (blue)
    INSERT INTO openmrs.patientflags_priority VALUES (
        3, 'Liste TB', 'color: blue', 3, NULL,
        1, '2018-05-31 15:02:47', NULL, NULL,
        0, NULL, NULL, NULL,
        '439d2dfa-29ee-4271-9e18-97a80d0eb475'
    );

    -- =========================================================================
    -- Create flag definitions
    -- =========================================================================

    -- Flag 2: Last viral load >= 12 months ago
    INSERT INTO openmrs.patientflags_flag VALUES (
        2,
        'Dernière charge virale de ce patient remonte à 12 mois ou plus',
        'select distinct a.patient_id FROM isanteplus.alert a WHERE a.id_alert = 4',
        'Dernière charge virale de ce patient remonte à 12 mois ou plus',
        1, 'org.openmrs.module.patientflags.evaluator.SQLFlagEvaluator',
        NULL, 1, '2018-05-28 02:18:18', 1, '2018-05-31 13:43:43',
        0, NULL, NULL, NULL,
        '8c176fcb-9354-43fa-b13c-c293e6f910dc',
        1  -- priority_id (VL)
    );

    -- Flag 4: TB/HIV co-infection
    INSERT INTO openmrs.patientflags_flag VALUES (
        4,
        'Coïnfection TB/VIH',
        'select distinct a.patient_id FROM isanteplus.alert a WHERE a.id_alert = 9',
        'Coïnfection TB/VIH',
        1, 'org.openmrs.module.patientflags.evaluator.SQLFlagEvaluator',
        NULL, 1, '2018-05-31 15:03:40', NULL, NULL,
        0, NULL, NULL, NULL,
        'a1d4c4ba-348c-456d-aca1-755190b78b0c',
        3  -- priority_id (TB)
    );

    -- Flag 5: Last viral load >= 3 months ago with > 1000 copies/ml
    INSERT INTO openmrs.patientflags_flag VALUES (
        5,
        'Le patient a au moins 3 mois de sa dernière charge virale supérieur à 1000 copies/ml',
        'select distinct a.patient_id FROM isanteplus.alert a WHERE a.id_alert = 5',
        'Le patient a au moins 3 mois de sa dernière charge virale supérieur à 1000 copies/ml',
        1, 'org.openmrs.module.patientflags.evaluator.SQLFlagEvaluator',
        NULL, 1, '2018-05-28 02:18:18', 1, '2018-05-31 13:43:43',
        0, NULL, NULL, NULL,
        '8c176fcb-9354-43fa-b13c-c293e6f910dc',
        1  -- priority_id (VL)
    );

    -- Flag 7: Patient must refill ARV within 30 days
    INSERT INTO openmrs.patientflags_flag VALUES (
        7,
        'Le patient doit venir renflouer ses ARV dans les 30 prochains jours',
        'select distinct a.patient_id FROM isanteplus.alert a WHERE a.id_alert = 7',
        'Le patient doit venir renflouer ses ARV dans les 30 prochains jours',
        1, 'org.openmrs.module.patientflags.evaluator.SQLFlagEvaluator',
        NULL, 1, '2018-05-28 02:18:18', 1, '2018-05-31 13:43:43',
        0, NULL, NULL, NULL,
        '8c176fcb-9354-43fa-b13c-c293e6f910dc',
        2  -- priority_id (Medication)
    );

    -- Flag 8: Patient has no more medications available
    INSERT INTO openmrs.patientflags_flag VALUES (
        8,
        'Le patient n\'a plus de médicaments disponibles',
        'select distinct a.patient_id FROM isanteplus.alert a WHERE a.id_alert = 8',
        'Le patient n\'a plus de médicaments disponibles',
        1, 'org.openmrs.module.patientflags.evaluator.SQLFlagEvaluator',
        NULL, 1, '2018-05-28 02:18:18', 1, '2018-05-31 13:43:43',
        0, NULL, NULL, NULL,
        '8c176fcb-9354-43fa-b13c-c293e6f910dc',
        2  -- priority_id (Medication)
    );

    -- Flag 9: Patient on ARV >= 3 months without viral load
    INSERT INTO openmrs.patientflags_flag VALUES (
        9,
        'Patient sous ARV depuis au moins 3 mois sans un résultat de charge virale',
        'select distinct a.patient_id FROM isanteplus.alert a WHERE a.id_alert = 10',
        'Patient sous ARV depuis au moins 3 mois sans un résultat de charge virale',
        1, 'org.openmrs.module.patientflags.evaluator.SQLFlagEvaluator',
        NULL, 1, '2020-02-05 14:58:13', 1, '2020-02-05 14:59:31',
        0, NULL, NULL, NULL,
        'c874aaf5-9e64-4fca-ba49-3f903158fa5f',
        1  -- priority_id (VL)
    );

    -- Flag 10: New ARV patient without INH prophylaxis
    INSERT INTO openmrs.patientflags_flag VALUES (
        10,
        'Nouveau enrôlé aux ARV sans prophylaxie INH',
        'select distinct a.patient_id FROM isanteplus.alert a WHERE a.id_alert = 11',
        'Nouveau enrôlé aux ARV sans prophylaxie INH',
        1, 'org.openmrs.module.patientflags.evaluator.SQLFlagEvaluator',
        NULL, 1, '2020-02-05 02:18:18', 1, '2020-02-05 13:43:43',
        0, NULL, NULL, NULL,
        'c26c358d-ec66-4588-8546-e39511723ded',
        2  -- priority_id (Medication)
    );

    -- Flag 11: Patient subscribed to DDP
    INSERT INTO openmrs.patientflags_flag VALUES (
        11,
        'Ce patient est abonné au DDP',
        'select distinct a.patient_id FROM isanteplus.alert a WHERE a.id_alert = 12',
        'Ce patient est abonné au DDP',
        1, 'org.openmrs.module.patientflags.evaluator.SQLFlagEvaluator',
        NULL, 1, '2021-08-02 13:18:18', 1, '2021-08-02 13:18:18',
        0, NULL, NULL, NULL,
        '38125986-383c-4426-b825-87dc0effa6de',
        2  -- priority_id (Medication)
    );

    -- =========================================================================
    -- Associate flags with tags
    -- =========================================================================
    INSERT INTO openmrs.patientflags_flag_tag VALUES
        (2, 2), (4, 2), (5, 2), (7, 2), (8, 2), (9, 2), (10, 2), (11, 2);

    -- =========================================================================
    -- Set tag display point
    -- =========================================================================
    INSERT INTO openmrs.patientflags_tag_displaypoint VALUES (2, 1);

    -- =========================================================================
    -- Configure global properties for alert display location
    -- =========================================================================
    UPDATE openmrs.global_property
    SET property_value = 'false'
    WHERE property = 'patientflags.patientHeaderDisplay';

    UPDATE openmrs.global_property
    SET property_value = 'true'
    WHERE property = 'patientflags.patientOverviewDisplay';

END$$
DELIMITER ;

-- =============================================================================
-- role_alert
--
-- Configures which roles can see patient flags/alerts.
-- Associates tag_id 2 with all standard OpenMRS roles.
-- =============================================================================
DELIMITER $$
DROP PROCEDURE IF EXISTS role_alert$$
CREATE PROCEDURE role_alert()
BEGIN
    SET SQL_SAFE_UPDATES = 0;
    SET FOREIGN_KEY_CHECKS = 0;

    TRUNCATE TABLE openmrs.patientflags_tag_role;

    -- =========================================================================
    -- Grant tag visibility to all roles
    -- =========================================================================
    INSERT INTO openmrs.patientflags_tag_role (tag_id, role) VALUES
        (2, 'Anonymous'),
        (2, 'Application: Administers System'),
        (2, 'Application: Configures Appointment Scheduling'),
        (2, 'Application: Configures Forms'),
        (2, 'Application: Configures Metadata'),
        (2, 'Application: Edits Existing Encounters'),
        (2, 'Application: Enters ADT Events'),
        (2, 'Application: Enters Vitals'),
        (2, 'Application: Has Super User Privileges'),
        (2, 'Application: Manages Atlas'),
        (2, 'Application: Manages Provider Schedules'),
        (2, 'Application: Records Allergies'),
        (2, 'Application: Registers Patients'),
        (2, 'Application: Requests Appointments'),
        (2, 'Application: Schedules And Overbooks Appointments'),
        (2, 'Application: Schedules Appointments'),
        (2, 'Application: Sees Appointment Schedule'),
        (2, 'Application: Uses Capture Vitals App'),
        (2, 'Application: Uses Patient Summary'),
        (2, 'Application: View Reports'),
        (2, 'Application: Writes Clinical Notes'),
        (2, 'Authenticated'),
        (2, 'Organizational: Doctor'),
        (2, 'Organizational: Hospital Administrator'),
        (2, 'Organizational: Nurse'),
        (2, 'Organizational: Registration Clerk'),
        (2, 'Organizational: System Administrator'),
        (2, 'Privilege Level: Full'),
        (2, 'Provider'),
        (2, 'System Developer');

    SET SQL_SAFE_UPDATES = 1;
    SET FOREIGN_KEY_CHECKS = 1;

END$$
DELIMITER ;

-- =============================================================================
-- isanteplus_patient_immunization
--
-- Syncs immunization data from openmrs.obs to isanteplus tables.
-- Extracts vaccine type, dose number, and date from obs groups.
-- Pivots dose data into immunization_dose table columns.
-- =============================================================================
DELIMITER $$
DROP PROCEDURE IF EXISTS isanteplus_patient_immunization$$
CREATE PROCEDURE isanteplus_patient_immunization()
BEGIN
    SET SQL_SAFE_UPDATES = 0;

    -- =========================================================================
    -- Extract immunization records from obs groups
    -- Concept 1421 = Immunization history construct (group)
    -- Concept 984 = Immunization given (answer)
    -- =========================================================================
    INSERT INTO isanteplus.patient_immunization (
        patient_id, location_id, encounter_id, vaccine_obs_group_id,
        vaccine_concept_id, encounter_date, vaccine_uuid, voided
    )
    SELECT
        o.person_id,
        o.location_id,
        o.encounter_id,
        o.obs_group_id,
        o.value_coded,
        o.obs_datetime,
        c.uuid,
        o.voided
    FROM openmrs.obs ob
    INNER JOIN openmrs.obs o ON ob.obs_id = o.obs_group_id
    INNER JOIN openmrs.concept c ON o.value_coded = c.concept_id
    WHERE o.concept_id = 984      -- Immunization given
      AND ob.concept_id = 1421    -- Immunization history construct
    ON DUPLICATE KEY UPDATE
        voided = o.voided;

    -- =========================================================================
    -- Update dose number from obs group
    -- Concept 1418 = Immunization sequence number
    -- =========================================================================
    UPDATE isanteplus.patient_immunization pim
    INNER JOIN openmrs.obs o ON pim.vaccine_obs_group_id = o.obs_group_id
    SET pim.dose = o.value_numeric
    WHERE o.concept_id = 1418;

    -- =========================================================================
    -- Update vaccine date from obs group
    -- Concept 1410 = Date immunization given
    -- =========================================================================
    UPDATE isanteplus.patient_immunization pim
    INNER JOIN openmrs.obs o ON pim.vaccine_obs_group_id = o.obs_group_id
    SET pim.vaccine_date = o.value_datetime
    WHERE o.concept_id = 1410;

    -- =========================================================================
    -- Pivot dose data into immunization_dose table
    -- Creates one row per patient/vaccine with columns for each dose date
    -- =========================================================================
    TRUNCATE TABLE immunization_dose;

    -- Insert unique patient/vaccine combinations
    INSERT INTO immunization_dose (patient_id, vaccine_concept_id)
    SELECT DISTINCT pati.patient_id, pati.vaccine_concept_id
    FROM patient_immunization pati
    WHERE pati.voided <> 1
    ON DUPLICATE KEY UPDATE
        vaccine_concept_id = pati.vaccine_concept_id;

    -- Update dose0
    UPDATE immunization_dose idose
    INNER JOIN patient_immunization pati
        ON idose.patient_id = pati.patient_id
       AND idose.vaccine_concept_id = pati.vaccine_concept_id
    SET idose.dose0 = pati.vaccine_date
    WHERE CONVERT(pati.dose, SIGNED INTEGER) = 0
      AND pati.voided <> 1;

    -- Update dose1
    UPDATE immunization_dose idose
    INNER JOIN patient_immunization pati
        ON idose.patient_id = pati.patient_id
       AND idose.vaccine_concept_id = pati.vaccine_concept_id
    SET idose.dose1 = pati.vaccine_date
    WHERE CONVERT(pati.dose, SIGNED INTEGER) = 1
      AND pati.voided <> 1;

    -- Update dose2
    UPDATE immunization_dose idose
    INNER JOIN patient_immunization pati
        ON idose.patient_id = pati.patient_id
       AND idose.vaccine_concept_id = pati.vaccine_concept_id
    SET idose.dose2 = pati.vaccine_date
    WHERE CONVERT(pati.dose, SIGNED INTEGER) = 2
      AND pati.voided <> 1;

    -- Update dose3
    UPDATE immunization_dose idose
    INNER JOIN patient_immunization pati
        ON idose.patient_id = pati.patient_id
       AND idose.vaccine_concept_id = pati.vaccine_concept_id
    SET idose.dose3 = pati.vaccine_date
    WHERE CONVERT(pati.dose, SIGNED INTEGER) = 3
      AND pati.voided <> 1;

    -- Update dose4
    UPDATE immunization_dose idose
    INNER JOIN patient_immunization pati
        ON idose.patient_id = pati.patient_id
       AND idose.vaccine_concept_id = pati.vaccine_concept_id
    SET idose.dose4 = pati.vaccine_date
    WHERE CONVERT(pati.dose, SIGNED INTEGER) = 4
      AND pati.voided <> 1;

    -- Update dose5
    UPDATE immunization_dose idose
    INNER JOIN patient_immunization pati
        ON idose.patient_id = pati.patient_id
       AND idose.vaccine_concept_id = pati.vaccine_concept_id
    SET idose.dose5 = pati.vaccine_date
    WHERE CONVERT(pati.dose, SIGNED INTEGER) = 5
      AND pati.voided <> 1;

    -- Update dose6
    UPDATE immunization_dose idose
    INNER JOIN patient_immunization pati
        ON idose.patient_id = pati.patient_id
       AND idose.vaccine_concept_id = pati.vaccine_concept_id
    SET idose.dose6 = pati.vaccine_date
    WHERE CONVERT(pati.dose, SIGNED INTEGER) = 6
      AND pati.voided <> 1;

    -- Update dose7
    UPDATE immunization_dose idose
    INNER JOIN patient_immunization pati
        ON idose.patient_id = pati.patient_id
       AND idose.vaccine_concept_id = pati.vaccine_concept_id
    SET idose.dose7 = pati.vaccine_date
    WHERE CONVERT(pati.dose, SIGNED INTEGER) = 7
      AND pati.voided <> 1;

    -- Update dose8
    UPDATE immunization_dose idose
    INNER JOIN patient_immunization pati
        ON idose.patient_id = pati.patient_id
       AND idose.vaccine_concept_id = pati.vaccine_concept_id
    SET idose.dose8 = pati.vaccine_date
    WHERE CONVERT(pati.dose, SIGNED INTEGER) = 8
      AND pati.voided <> 1;

    SET SQL_SAFE_UPDATES = 1;

END$$
DELIMITER ;

-- =============================================================================
-- calling_arv_status_and_regimen
--
-- Orchestrates the procedures that update the patient's ARV status and regimen.
-- This procedure exists to simplify the definition of the scheduled event.
-- =============================================================================
DELIMITER $$
	DROP PROCEDURE IF EXISTS calling_arv_status_and_regimen$$
	CREATE PROCEDURE calling_arv_status_and_regimen()
	BEGIN
		call patient_status_arv();
		call isanteplusregimen_dml();
	END$$
DELIMITER ;

-- =============================================================================
-- calling_patient_alert
--
-- This procedure orchestrates the procedures that generate patient alerts
-- so that they are called in the correct order. It's used to simplify the
-- definition of the EVENT that triggers these procedures.
-- =============================================================================
DELIMITER $$
	DROP PROCEDURE IF EXISTS calling_patient_alert$$
	CREATE PROCEDURE calling_patient_alert()
	BEGIN
		call isanteplus_patient_alert();
		call alert_viral_load();
		call isanteplus_patient_immunization();
	END$$
DELIMITER ;

-- =============================================================================
-- patient_status_arv_event
--
-- This event is triggered every hour and updates patients ARV status and
-- current regimen
-- =============================================================================
DROP EVENT if exists patient_status_arv_event;
CREATE EVENT if not exists patient_status_arv_event
ON SCHEDULE EVERY 1 HOUR
 STARTS now()
	DO
	call calling_arv_status_and_regimen();

-- =============================================================================
-- isanteplus_patient_alert_event
--
-- This event is triggered every 20 minutes and generates any alerts
-- =============================================================================
DROP EVENT if exists isanteplus_patient_alert_event;
CREATE EVENT if not exists isanteplus_patient_alert_event
ON SCHEDULE EVERY 20 MINUTE
 STARTS now()
	DO
	call calling_patient_alert();

-- We immediately invoke the role_alert procedure to ensure the
-- necessary metadata are preloaded
call role_alert();

-- I can't find this event, but I assume this is here for historical reasons?
DROP EVENT if exists isanteplusregimen_dml_event;
