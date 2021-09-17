USE isanteplus;
DROP TABLE IF EXISTS isanteplus.report_type;
	CREATE TABLE IF NOT EXISTS isanteplus.report_type (
	  report_type_id INT(11),
	  report_type_name_fr text NOT NULL,
	  report_type_name_en text NOT NULL,
	  report_type_description text NOT NULL,
	  created_date datetime NOT NULL,
	  CONSTRAINT pk_report_type PRIMARY KEY (report_type_id)
	) ENGINE=INNODB DEFAULT CHARSET=utf8;
	
	INSERT INTO isanteplus.report_type (report_type_id,report_type_name_fr,report_type_name_en,
	report_type_description,created_date)
	VALUES(1,'Rapport de surveillance hebdomadaire','Weekly monitoring report',
	'Rapport de surveillance hebdomadaire',now());
	
	
	DROP TABLE IF EXISTS isanteplus.indicator_type;
	CREATE TABLE IF NOT EXISTS isanteplus.indicator_type (
	  indicator_type_id INT(11) NOT NULL,
	  report_type_id INT(11) NOT NULL,
	  indicator_name_fr text NOT NULL,
	  indicator_name_en text NOT NULL,
	  indicator_type_description text,
	  date_created datetime NOT NULL,
	  CONSTRAINT pk_indicator_type PRIMARY KEY (indicator_type_id,report_type_id)
	) ENGINE=INNODB DEFAULT CHARSET=utf8;
	
	DROP TABLE IF EXISTS isanteplus.indicators;
	CREATE TABLE IF NOT EXISTS isanteplus.indicators (
	  indicator_id INT(11) NOT NULL,
	  indicator_type_id INT(11) NOT NULL,
	  patient_id INT(11) NOT NULL,
	  location_id INT(11) NOT NULL,
	  encounter_id INT(11) NOT NULL,
	  indicator_date DATETIME NOT NULL,
	  voided TINYINT(1) NOT NULL DEFAULT 0,
	  created_date date NOT NULL,
	  last_updated_date DATE NOT NULL,
	  CONSTRAINT pk_indicators PRIMARY KEY (indicator_type_id,patient_id,indicator_date)
	) ENGINE=INNODB DEFAULT CHARSET=utf8;
	
	/*1 : Agression par animal suspecte de rage*/
	
	INSERT INTO isanteplus.indicator_type (indicator_type_id,report_type_id,indicator_name_fr,
	indicator_name_en,indicator_type_description,date_created)
	VALUES(1,1,'Agression par animal suspecte de rage','Animal aggression suspected of rabies',
			'Agression par animal suspecte de rage', now());
	/*2 : Coqueluche Suspect*/		
	INSERT INTO isanteplus.indicator_type (indicator_type_id,report_type_id,indicator_name_fr,
	indicator_name_en,indicator_type_description,date_created)
	VALUES(2,1,'Coqueluche Suspect','Whooping Cough Suspect',
			'Coqueluche Suspect', now());
	
   /*3 : Cholera Suspect*/	
	INSERT INTO isanteplus.indicator_type (indicator_type_id,report_type_id,indicator_name_fr,
	indicator_name_en,indicator_type_description,date_created)
	VALUES(3,1,'Cholera Suspect','Cholera Suspect',
			'Cholera Suspect', now());
			
	/*4 : Deces Maternel*/
	INSERT INTO isanteplus.indicator_type (indicator_type_id,report_type_id,indicator_name_fr,
	indicator_name_en,indicator_type_description,date_created)
	VALUES(4,1,'Deces Maternel','Cholera Suspect',
			'Maternal Death', now());
	/*5 : Diphterie probable*/
	INSERT INTO isanteplus.indicator_type (indicator_type_id,report_type_id,indicator_name_fr,
	indicator_name_en,indicator_type_description,date_created)
	VALUES(5,1,'Diphterie probable','Diphterie probable',
			'Diphterie probable', now());
	/*6 : Evenement supose etre attribuable a la vaccination et a l’immunisation (esavi)*/		
	INSERT INTO isanteplus.indicator_type (indicator_type_id,report_type_id,indicator_name_fr,
	indicator_name_en,indicator_type_description,date_created)
	VALUES(6,1,'Evenement supose etre attribuable a la vaccination et a l’immunisation (esavi)',
	'Event assumed to be attributable to vaccination and immunization (esavi)',
	'Evenement supose etre attribuable a la vaccination et a l’immunisation (esavi)', now());
	/*7 : Meningite Suspect*/
	INSERT INTO isanteplus.indicator_type (indicator_type_id,report_type_id,indicator_name_fr,
	indicator_name_en,indicator_type_description,date_created)
	VALUES(7,1,'Meningite Suspect','Meningite Suspect','Meningite Suspect', now());
	/*8 : Microcephalie congenitale*/
	INSERT INTO isanteplus.indicator_type (indicator_type_id,report_type_id,indicator_name_fr,
	indicator_name_en,indicator_type_description,date_created)
	VALUES(8,1,'Microcephalie congenitale','congenital microcephaly','Microcephalie congenitale', now());
	/*9 : Paludisme confirme*/
	INSERT INTO isanteplus.indicator_type (indicator_type_id,report_type_id,indicator_name_fr,
	indicator_name_en,indicator_type_description,date_created)
	VALUES(9,1,'Paludisme confirme','Malaria confirms','Paludisme confirme', now());
	/*10 : Paralysie flasque aigue(pfa)*/
	INSERT INTO isanteplus.indicator_type (indicator_type_id,report_type_id,indicator_name_fr,
	indicator_name_en,indicator_type_description,date_created)
	VALUES(10,1,'Paralysie flasque aigue(pfa)','Acute flaccid paralysis (pfa)','Paralysie flasque aigue(pfa)', now());
	/*11 : Peste suspecte*/
	INSERT INTO isanteplus.indicator_type (indicator_type_id,report_type_id,indicator_name_fr,
	indicator_name_en,indicator_type_description,date_created)
	VALUES(11,1,'Peste suspecte','Suspicious plague','Peste suspecte', now());
	/*12 : Rage humaine*/
	INSERT INTO isanteplus.indicator_type (indicator_type_id,report_type_id,indicator_name_fr,
	indicator_name_en,indicator_type_description,date_created)
	VALUES(12,1,'Rage humaine','Human rabies','Rage humaine', now());
	/*13 : Rougeole/rubeole suspecte*/
	INSERT INTO isanteplus.indicator_type (indicator_type_id,report_type_id,indicator_name_fr,
	indicator_name_en,indicator_type_description,date_created)
	VALUES(13,1,'Rougeole/rubeole suspecte','Measles / rubella suspicious ','Rougeole/rubeole suspecte', now());
	/*14 : Syndrome de guillain barre*/
	INSERT INTO isanteplus.indicator_type (indicator_type_id,report_type_id,indicator_name_fr,
	indicator_name_en,indicator_type_description,date_created)
	VALUES(14,1,'Syndrome de guillain barre','Guillain barre syndrome','Syndrome de guillain barre', now());
	/*15 : Syndrome de fievre hemmoragique aigue*/
	INSERT INTO isanteplus.indicator_type (indicator_type_id,report_type_id,indicator_name_fr,
	indicator_name_en,indicator_type_description,date_created)
	VALUES(15,1,'Syndrome de fievre hemmoragique aigue','Guillain barre syndrome','Syndrome de fievre hemmoragique aigue', now());
	/*16 : Syndrome de rubeole congenitale*/
	INSERT INTO isanteplus.indicator_type (indicator_type_id,report_type_id,indicator_name_fr,
	indicator_name_en,indicator_type_description,date_created)
	VALUES(16,1,'Syndrome de rubeole congenitale','Congenital rubella syndrome',
	'Syndrome de rubeole congenitale', now());
	/*17 : Tetanos neonatal (tnn)*/
	INSERT INTO isanteplus.indicator_type (indicator_type_id,report_type_id,indicator_name_fr,
	indicator_name_en,indicator_type_description,date_created)
	VALUES(17,1,'Tetanos neonatal (tnn)','Tetanus neonatal (tnn)',
	'Tetanos neonatal (tnn)', now());
	/*18 : Toxi-infection alimentaire collective (tiac)*/
	INSERT INTO isanteplus.indicator_type (indicator_type_id,report_type_id,indicator_name_fr,
	indicator_name_en,indicator_type_description,date_created)
	VALUES(18,1,'Toxi-infection alimentaire collective (tiac)','Collective food poisoning (tiac)',
	'Toxi-infection alimentaire collective (tiac)', now());
	/*19 : Charbon cutané suspect*/
	INSERT INTO isanteplus.indicator_type (indicator_type_id,report_type_id,indicator_name_fr,
	indicator_name_en,indicator_type_description,date_created)
	VALUES(19,1,'Charbon cutané suspect','Suspicious skin anthrax',
	'Charbon cutané suspect', now());
	
	/*20 : Dengue suspecte*/
	INSERT INTO isanteplus.indicator_type (indicator_type_id,report_type_id,indicator_name_fr,
	indicator_name_en,indicator_type_description,date_created)
	VALUES(20,1,'Dengue suspecte','Diabetes','Dengue suspecte', now());
	/*21:Diabète*/
	INSERT INTO isanteplus.indicator_type (indicator_type_id,report_type_id,indicator_name_fr,
	indicator_name_en,indicator_type_description,date_created)
	VALUES(21,1,'Diabète','Suspicious dengue','Diabète', now());
	/*22 : Diarrhée aigue aqueuse*/
	INSERT INTO isanteplus.indicator_type (indicator_type_id,report_type_id,indicator_name_fr,
	indicator_name_en,indicator_type_description,date_created)
	VALUES(22,1,'Diarrhée aigue aqueuse','Acute watery diarrhea','Diarrhée aigue aqueuse', now());
	/*23 : Diarrhée aigue sanglante*/
	INSERT INTO isanteplus.indicator_type (indicator_type_id,report_type_id,indicator_name_fr,
	indicator_name_en,indicator_type_description,date_created)
	VALUES(23,1,'Diarrhée aigue sanglante','Acute bloody diarrhea','Diarrhée aigue sanglante', now());
	/*24:Fièvre typhoïde suspecte*/
	INSERT INTO isanteplus.indicator_type (indicator_type_id,report_type_id,indicator_name_fr,
	indicator_name_en,indicator_type_description,date_created)
	VALUES(24,1,'Fièvre typhoïde suspecte','Suspicious typhoid fever','Fièvre typhoïde suspecte', now());
	/*25 : Filariose probable*/
	INSERT INTO isanteplus.indicator_type (indicator_type_id,report_type_id,indicator_name_fr,
	indicator_name_en,indicator_type_description,date_created)
	VALUES(25,1,'Filariose probable','Probable filariasis','Filariose probable', now());
	/*26 : Infection respiratoire aigue*/
	INSERT INTO isanteplus.indicator_type (indicator_type_id,report_type_id,indicator_name_fr,
	indicator_name_en,indicator_type_description,date_created)
	VALUES(26,1,'Infection respiratoire aigue','Acute respiratory infection','Infection respiratoire aigue', now());
	/*27 : Syndrome ictérique fébrile*/
	INSERT INTO isanteplus.indicator_type (indicator_type_id,report_type_id,indicator_name_fr,
	indicator_name_en,indicator_type_description,date_created)
	VALUES(27,1,'Syndrome ictérique fébrile','Febrile jaundice syndrome','Syndrome ictérique fébrile', now());
	/*28 : Tétanos*/
	INSERT INTO isanteplus.indicator_type (indicator_type_id,report_type_id,indicator_name_fr,
	indicator_name_en,indicator_type_description,date_created)
	VALUES(28,1,'Tétanos','Tetanus','Tétanos', now());
	/*29 : Accidents (domestiques, voie publique)*/
	INSERT INTO isanteplus.indicator_type (indicator_type_id,report_type_id,indicator_name_fr,
	indicator_name_en,indicator_type_description,date_created)
	VALUES(29,1,'Accidents (domestiques, voie publique)','Accidents (domestic, public roads)',
	'Accidents (domestiques, voie publique)', now());
	/*30 : Cancers (seins, col de l’utérus, prostate, autres)*/
	INSERT INTO isanteplus.indicator_type (indicator_type_id,report_type_id,indicator_name_fr,
	indicator_name_en,indicator_type_description,date_created)
	VALUES(30,1,'Cancers (seins, col de l’utérus, prostate, autres)','Cancers (seins, col de l’utérus, prostate, autres)',
	'Cancers (seins, col de l’utérus, prostate, autres)', now());
	
	/*31 : Epilepsie*/

	INSERT INTO isanteplus.indicator_type (indicator_type_id,report_type_id,indicator_name_fr,
	indicator_name_en,indicator_type_description,date_created)
	VALUES(31,1,'Epilepsie','Epilepsy','Epilepsie', now());
	
	/*32 : Hypertension artérielle (hta)*/
	INSERT INTO isanteplus.indicator_type (indicator_type_id,report_type_id,indicator_name_fr,
	indicator_name_en,indicator_type_description,date_created)
	VALUES(32,1,'Hypertension artérielle (hta)','High blood pressure','Hypertension artérielle (hta)', now());
	
	/*33 : Infection sexuellement transmissible (ist)*/
	INSERT INTO isanteplus.indicator_type (indicator_type_id,report_type_id,indicator_name_fr,
	indicator_name_en,indicator_type_description,date_created)
	VALUES(33,1,'Infection sexuellement transmissible (IST)','Sexually transmitted infection (STI)','Infection sexuellement transmissible (ist)', now());
	
	/* 34 : Lèpre suspecte */
	INSERT INTO isanteplus.indicator_type (indicator_type_id,report_type_id,indicator_name_fr,
	indicator_name_en,indicator_type_description,date_created)
	VALUES(34,1,'Lèpre suspecte','Suspicious leprosy','Lèpre suspecte', now());
	
	/*35 : Malnutrition*/
	INSERT INTO isanteplus.indicator_type (indicator_type_id,report_type_id,indicator_name_fr,
	indicator_name_en,indicator_type_description,date_created)
	VALUES(35,1,'Malnutrition','Malnutrition','Malnutrition', now());
	
	/*36 : Syphilis congénitale*/
	INSERT INTO isanteplus.indicator_type (indicator_type_id,report_type_id,indicator_name_fr,
	indicator_name_en,indicator_type_description,date_created)
	VALUES(36,1,'Syphilis congénitale','Congenital syphilis','Syphilis congénitale', now());
	
	/*37 : Violences (physique, sexuelle)*/
	
	INSERT INTO isanteplus.indicator_type (indicator_type_id,report_type_id,indicator_name_fr,
	indicator_name_en,indicator_type_description,date_created)
	VALUES(37,1,'Violences (physique, sexuelle)','Violence (physical, sexual) ','Violences (physique, sexuelle)', now());
 
 DELIMITER $$
	DROP PROCEDURE IF EXISTS patient_diagnosis$$
	CREATE PROCEDURE patient_diagnosis()
	BEGIN
	/*insertion of all diagnosis in the table patient_diagnosis*/
INSERT into patient_diagnosis
					(
					 patient_id,
					 encounter_id,
					 location_id,
					 concept_group,
					 obs_group_id,
					 concept_id,
					 answer_concept_id,
					 voided
					)
					select distinct ob.person_id,ob.encounter_id,
					ob.location_id,ob1.concept_id,ob.obs_group_id,ob.concept_id, ob.value_coded, ob.voided
					from openmrs.obs ob, openmrs.obs ob1, openmrs.encounter e, openmrs.encounter_type et
					where ob.person_id = ob1.person_id
					AND ob.encounter_id = ob1.encounter_id
					AND ob.obs_group_id = ob1.obs_id
					AND ob.encounter_id = e.encounter_id
					AND e.encounter_type = et.encounter_type_id	
					AND ob.concept_id = 1284
					AND (ob.value_coded <> '' OR ob.value_coded is not null)
					AND et.uuid IN (
									'5c312603-25c1-4dbe-be18-1a167eb85f97',
									'49592bec-dd22-4b6c-a97f-4dd2af6f2171',
									'12f4d7c3-e047-4455-a607-47a40fe32460',
									'a5600919-4dde-4eb8-a45b-05c204af8284',
									'709610ff-5e39-4a47-9c27-a60e740b0944',
									'fdb5b14f-555f-4282-b4c1-9286addf0aae'
								   )
					on duplicate key update
					encounter_id = ob.encounter_id,
					voided = ob.voided;
	/*update patient diagnosis for suspected_confirmed area*/					
	update patient_diagnosis pdiag, openmrs.obs ob
	 SET pdiag.suspected_confirmed = ob.value_coded
	 WHERE pdiag.patient_id = ob.person_id
		   AND pdiag.obs_group_id = ob.obs_group_id
		   AND pdiag.encounter_id = ob.encounter_id
		   AND ob.concept_id = 159394
		   AND ob.value_coded IN (159392,159393)
		   AND ob.voided = 0;
	/*Update for primary_secondary area*/
	update patient_diagnosis pdiag, openmrs.obs ob
	 SET pdiag.primary_secondary = ob.value_coded
	 WHERE pdiag.patient_id = ob.person_id
		   AND pdiag.obs_group_id = ob.obs_group_id
		   AND pdiag.encounter_id = ob.encounter_id
		   AND ob.concept_id = 159946
		   AND ob.value_coded IN (159943,159944)
		   AND ob.voided = 0;
	/*Update encounter date for patient_diagnosis*/	   
	update patient_diagnosis pdiag, openmrs.encounter enc
    SET pdiag.encounter_date = DATE(enc.encounter_datetime)
    WHERE pdiag.location_id = enc.location_id
          AND pdiag.encounter_id = enc.encounter_id
		  AND enc.voided = 0;
/*Ending patient_diagnosis*/
 END$$
DELIMITER ;
 
 DELIMITER $$
	DROP PROCEDURE IF EXISTS report_indicators$$
	CREATE PROCEDURE report_indicators()
	BEGIN
	/*Indicateur 1 - 1 : Agression par animal suspecte de rage*/
	INSERT INTO isanteplus.indicators (indicator_id,indicator_type_id,patient_id,location_id,encounter_id,
										indicator_date,voided,created_date,last_updated_date)
	SELECT 1,1,pdiag.patient_id, pdiag.location_id, pdiag.encounter_id, pdiag.encounter_date,
	pdiag.voided, now(), now() FROM isanteplus.patient p, isanteplus.patient_diagnosis pdiag
	WHERE p.patient_id = pdiag.patient_id
	AND pdiag.concept_id = 1284
	AND pdiag.answer_concept_id = 160146
	AND pdiag.suspected_confirmed = 159393
	AND pdiag.voided <> 1
	ON DUPLICATE KEY UPDATE
	last_updated_date = NOW(),
	voided = pdiag.voided;
	
	/*2 : Coqueluche Suspect*/
	
	INSERT INTO isanteplus.indicators (indicator_id,indicator_type_id,patient_id,location_id,encounter_id,
										indicator_date,voided,created_date,last_updated_date)
	SELECT 2,2,pdiag.patient_id, pdiag.location_id, pdiag.encounter_id, pdiag.encounter_date,
	pdiag.voided, now(), now() FROM isanteplus.patient p, isanteplus.patient_diagnosis pdiag
	WHERE p.patient_id = pdiag.patient_id
	AND pdiag.concept_id = 1284
	AND pdiag.answer_concept_id = 114190
	AND pdiag.suspected_confirmed = 159393
	AND pdiag.voided <> 1
	ON DUPLICATE KEY UPDATE
	last_updated_date = NOW(),
	voided = pdiag.voided;
	
	/*3 : Cholera Suspect*/
	INSERT INTO isanteplus.indicators (indicator_id,indicator_type_id,patient_id,location_id,encounter_id,
										indicator_date,voided,created_date,last_updated_date)
	SELECT 3,3,pdiag.patient_id, pdiag.location_id, pdiag.encounter_id, pdiag.encounter_date,
	pdiag.voided, now(), now() FROM isanteplus.patient p, isanteplus.patient_diagnosis pdiag
	WHERE p.patient_id = pdiag.patient_id
	AND pdiag.concept_id = 1284
	AND pdiag.answer_concept_id = 122604
	AND pdiag.suspected_confirmed = 159393
	AND pdiag.voided <> 1
	ON DUPLICATE KEY UPDATE
	last_updated_date = NOW(),
	voided = pdiag.voided;
	
	/*Deces maternel*/
	INSERT INTO isanteplus.indicators (indicator_id,indicator_type_id,patient_id,location_id,encounter_id,
										indicator_date,voided,created_date,last_updated_date)
	SELECT 4,4,pdiag.patient_id, pdiag.location_id, pdiag.encounter_id, pdiag.encounter_date,
	pdiag.voided, now(), now() FROM isanteplus.patient p, isanteplus.patient_diagnosis pdiag
	WHERE p.patient_id = pdiag.patient_id
	AND pdiag.concept_id = 1284
	AND pdiag.answer_concept_id = 134612
	AND pdiag.voided <> 1
	ON DUPLICATE KEY UPDATE
	last_updated_date = NOW(),
	voided = pdiag.voided;
	
	/*5 : Diphterie probable*/
	INSERT INTO isanteplus.indicators (indicator_id,indicator_type_id,patient_id,location_id,encounter_id,
										indicator_date,voided,created_date,last_updated_date)
	SELECT 5,5,pdiag.patient_id, pdiag.location_id, pdiag.encounter_id, pdiag.encounter_date,
	pdiag.voided, now(), now() FROM isanteplus.patient p, isanteplus.patient_diagnosis pdiag
	WHERE p.patient_id = pdiag.patient_id
	AND pdiag.concept_id = 1284
	AND pdiag.answer_concept_id = 119399
	AND pdiag.suspected_confirmed = 159393
	AND pdiag.voided <> 1
	ON DUPLICATE KEY UPDATE
	last_updated_date = NOW(),
	voided = pdiag.voided;
	
	/*6 : Evenement supose etre attribuable a la vaccination et a l’immunisation (esavi)*/
		INSERT INTO isanteplus.indicators (indicator_id,indicator_type_id,patient_id,location_id,encounter_id,
										indicator_date,voided,created_date,last_updated_date)
		SELECT 6,6,pdiag.patient_id, pdiag.location_id, pdiag.encounter_id, pdiag.encounter_date,
		pdiag.voided, now(), now() FROM isanteplus.patient p, isanteplus.patient_diagnosis pdiag,
		openmrs.concept c
		WHERE p.patient_id = pdiag.patient_id
		AND pdiag.answer_concept_id = c.concept_id
		AND pdiag.concept_id = 1284
		AND c.uuid = '1b4d09df-4f9f-44ff-9e7b-c1eba6514289'
		AND pdiag.voided <> 1
		ON DUPLICATE KEY UPDATE
		last_updated_date = NOW(),
		voided = pdiag.voided;
	/*7 : Meningite Suspect*/
	INSERT INTO isanteplus.indicators (indicator_id,indicator_type_id,patient_id,location_id,encounter_id,
										indicator_date,voided,created_date,last_updated_date)
	SELECT 7,7,pdiag.patient_id, pdiag.location_id, pdiag.encounter_id, pdiag.encounter_date,
	pdiag.voided, now(), now() FROM isanteplus.patient p, isanteplus.patient_diagnosis pdiag
	WHERE p.patient_id = pdiag.patient_id
	AND pdiag.concept_id = 1284
	AND pdiag.answer_concept_id = 115835
	AND pdiag.suspected_confirmed = 159393
	AND pdiag.voided <> 1
	ON DUPLICATE KEY UPDATE
	last_updated_date = NOW(),
	voided = pdiag.voided;
	
	/*8 : Microcephalie congenitale*/
	INSERT INTO isanteplus.indicators (indicator_id,indicator_type_id,patient_id,location_id,encounter_id,
										indicator_date,voided,created_date,last_updated_date)
		SELECT 8,8,pdiag.patient_id, pdiag.location_id, pdiag.encounter_id, pdiag.encounter_date,
		pdiag.voided, now(), now() FROM isanteplus.patient p, isanteplus.patient_diagnosis pdiag,
		openmrs.concept c
		WHERE p.patient_id = pdiag.patient_id
		AND pdiag.answer_concept_id = c.concept_id
		AND pdiag.concept_id = 1284
		AND c.uuid = '87275706-5e87-4562-8cdc-b9d1e1649f83'
		AND pdiag.voided <> 1
		ON DUPLICATE KEY UPDATE
		last_updated_date = NOW(),
		voided = pdiag.voided;
	
	/*9 : Paludisme confirme*/
	INSERT INTO isanteplus.indicators (indicator_id,indicator_type_id,patient_id,location_id,encounter_id,
										indicator_date,voided,created_date,last_updated_date)
	SELECT 9,9,pdiag.patient_id, pdiag.location_id, pdiag.encounter_id, pdiag.encounter_date,
	pdiag.voided, now(), now() FROM isanteplus.patient p, isanteplus.patient_diagnosis pdiag
	WHERE p.patient_id = pdiag.patient_id
	AND pdiag.concept_id = 1284
	AND pdiag.answer_concept_id = 116128
	AND pdiag.suspected_confirmed = 159392
	AND pdiag.voided <> 1
	ON DUPLICATE KEY UPDATE
	last_updated_date = NOW(),
	voided = pdiag.voided;
	
	/*10 : Paralysie flasque aigue(pfa)*/
	
	INSERT INTO isanteplus.indicators (indicator_id,indicator_type_id,patient_id,location_id,encounter_id,
										indicator_date,voided,created_date,last_updated_date)
	SELECT 10,10,pdiag.patient_id, pdiag.location_id, pdiag.encounter_id, pdiag.encounter_date,
	pdiag.voided, now(), now() FROM isanteplus.patient p, isanteplus.patient_diagnosis pdiag
	WHERE p.patient_id = pdiag.patient_id
	AND pdiag.concept_id = 1284
	AND pdiag.answer_concept_id = 160426
	AND pdiag.voided <> 1
	ON DUPLICATE KEY UPDATE
	last_updated_date = NOW(),
	voided = pdiag.voided;
	
	/*11 : Peste suspecte*/
	
	INSERT INTO isanteplus.indicators (indicator_id,indicator_type_id,patient_id,location_id,encounter_id,
										indicator_date,voided,created_date,last_updated_date)
	SELECT 11,11,pdiag.patient_id, pdiag.location_id, pdiag.encounter_id, pdiag.encounter_date,
	pdiag.voided, now(), now() FROM isanteplus.patient p, isanteplus.patient_diagnosis pdiag
	WHERE p.patient_id = pdiag.patient_id
	AND pdiag.concept_id = 1284
	AND pdiag.answer_concept_id = 114120
	AND pdiag.suspected_confirmed = 159393
	AND pdiag.voided <> 1
	ON DUPLICATE KEY UPDATE
	last_updated_date = NOW(),
	voided = pdiag.voided;
	
	/*12 : Rage humaine*/
	INSERT INTO isanteplus.indicators (indicator_id,indicator_type_id,patient_id,location_id,encounter_id,
										indicator_date,voided,created_date,last_updated_date)
	SELECT 12,12,pdiag.patient_id, pdiag.location_id, pdiag.encounter_id, pdiag.encounter_date,
	pdiag.voided, now(), now() FROM isanteplus.patient p, isanteplus.patient_diagnosis pdiag
	WHERE p.patient_id = pdiag.patient_id
	AND pdiag.concept_id = 1284
	AND pdiag.answer_concept_id = 160146
	AND pdiag.voided <> 1
	ON DUPLICATE KEY UPDATE
	last_updated_date = NOW(),
	voided = pdiag.voided;
	
	/*13 : Rougeole/rubeole suspecte*/
	INSERT INTO isanteplus.indicators (indicator_id,indicator_type_id,patient_id,location_id,encounter_id,
										indicator_date,voided,created_date,last_updated_date)
	SELECT 13,13,pdiag.patient_id, pdiag.location_id, pdiag.encounter_id, pdiag.encounter_date,
	pdiag.voided, now(), now() FROM isanteplus.patient p, isanteplus.patient_diagnosis pdiag
	WHERE p.patient_id = pdiag.patient_id
	AND pdiag.concept_id = 1284
	AND pdiag.answer_concept_id = 134561
	AND pdiag.suspected_confirmed = 159393
	AND pdiag.voided <> 1
	ON DUPLICATE KEY UPDATE
	last_updated_date = NOW(),
	voided = pdiag.voided;
	
	/*14 : Syndrome de guillain barre*/
		INSERT INTO isanteplus.indicators (indicator_id,indicator_type_id,patient_id,location_id,encounter_id,
										indicator_date,voided,created_date,last_updated_date)
	SELECT 14,14,pdiag.patient_id, pdiag.location_id, pdiag.encounter_id, pdiag.encounter_date,
	pdiag.voided, now(), now() FROM isanteplus.patient p, isanteplus.patient_diagnosis pdiag
	WHERE p.patient_id = pdiag.patient_id
	AND pdiag.concept_id = 1284
	AND pdiag.answer_concept_id = 139233
	AND pdiag.voided <> 1
	ON DUPLICATE KEY UPDATE
	last_updated_date = NOW(),
	voided = pdiag.voided;
	/*15 : Syndrome de fievre hemorragique aigue*/
		INSERT INTO isanteplus.indicators (indicator_id,indicator_type_id,patient_id,location_id,encounter_id,
										indicator_date,voided,created_date,last_updated_date)
	SELECT 15,15,pdiag.patient_id, pdiag.location_id, pdiag.encounter_id, pdiag.encounter_date,
	pdiag.voided, now(), now() FROM isanteplus.patient p, isanteplus.patient_diagnosis pdiag
	WHERE p.patient_id = pdiag.patient_id
	AND pdiag.concept_id = 1284
	AND pdiag.answer_concept_id = 163392
	AND pdiag.voided <> 1
	ON DUPLICATE KEY UPDATE
	last_updated_date = NOW(),
	voided = pdiag.voided;
	/*16 : Syndrome de rubeole congenitale*/
	INSERT INTO isanteplus.indicators (indicator_id,indicator_type_id,patient_id,location_id,encounter_id,
										indicator_date,voided,created_date,last_updated_date)
	SELECT 16,16,pdiag.patient_id, pdiag.location_id, pdiag.encounter_id, pdiag.encounter_date,
	pdiag.voided, now(), now() FROM isanteplus.patient p, isanteplus.patient_diagnosis pdiag
	WHERE p.patient_id = pdiag.patient_id
	AND pdiag.concept_id = 1284
	AND pdiag.answer_concept_id = 113205
	AND pdiag.voided <> 1
	ON DUPLICATE KEY UPDATE
	last_updated_date = NOW(),
	voided = pdiag.voided;
	/*17 : Tetanos neonatal (tnn)*/
		INSERT INTO isanteplus.indicators (indicator_id,indicator_type_id,patient_id,location_id,encounter_id,
										indicator_date,voided,created_date,last_updated_date)
	SELECT 17,17,pdiag.patient_id, pdiag.location_id, pdiag.encounter_id, pdiag.encounter_date,
	pdiag.voided, now(), now() FROM isanteplus.patient p, isanteplus.patient_diagnosis pdiag
	WHERE p.patient_id = pdiag.patient_id
	AND pdiag.concept_id = 1284
	AND pdiag.answer_concept_id = 124957
	AND pdiag.voided <> 1
	ON DUPLICATE KEY UPDATE
	last_updated_date = NOW(),
	voided = pdiag.voided;
	
	/*18 : Toxi-infection alimentaire collective (tiac)*/
	
	INSERT INTO isanteplus.indicators (indicator_id,indicator_type_id,patient_id,location_id,encounter_id,
										indicator_date,voided,created_date,last_updated_date)
		SELECT 18,18,pdiag.patient_id, pdiag.location_id, pdiag.encounter_id, pdiag.encounter_date,
		pdiag.voided, now(), now() FROM isanteplus.patient p, isanteplus.patient_diagnosis pdiag,
		openmrs.concept c
		WHERE p.patient_id = pdiag.patient_id
		AND pdiag.answer_concept_id = c.concept_id
		AND pdiag.concept_id = 1284
		AND c.uuid = '50d568a4-2e65-420c-8d9c-8b63f146e2c5'
		AND pdiag.voided <> 1
		ON DUPLICATE KEY UPDATE
		last_updated_date = NOW(),
		voided = pdiag.voided;
	
	/*19 : Charbon cutané suspect*/ /*Cutaneous Anthrax 143086*/
		INSERT INTO isanteplus.indicators (indicator_id,indicator_type_id,patient_id,location_id,encounter_id,
										indicator_date,voided,created_date,last_updated_date)
	SELECT 13,13,pdiag.patient_id, pdiag.location_id, pdiag.encounter_id, pdiag.encounter_date,
	pdiag.voided, now(), now() FROM isanteplus.patient p, isanteplus.patient_diagnosis pdiag
	WHERE p.patient_id = pdiag.patient_id
	AND pdiag.concept_id = 1284
	AND pdiag.answer_concept_id = 121555
	AND pdiag.suspected_confirmed = 159393
	AND pdiag.voided <> 1
	ON DUPLICATE KEY UPDATE
	last_updated_date = NOW(),
	voided = pdiag.voided;
	
	/*20 : Dengue suspecte*/
	INSERT INTO isanteplus.indicators (indicator_id,indicator_type_id,patient_id,location_id,encounter_id,
										indicator_date,voided,created_date,last_updated_date)
	SELECT 20,20,pdiag.patient_id, pdiag.location_id, pdiag.encounter_id, pdiag.encounter_date,
	pdiag.voided, now(), now() FROM isanteplus.patient p, isanteplus.patient_diagnosis pdiag
	WHERE p.patient_id = pdiag.patient_id
	AND pdiag.concept_id = 1284
	AND pdiag.answer_concept_id = 142592
	AND pdiag.suspected_confirmed = 159393
	AND pdiag.voided <> 1
	ON DUPLICATE KEY UPDATE
	last_updated_date = NOW(),
	voided = pdiag.voided;
	
	/*21:Diabète*/
	INSERT INTO isanteplus.indicators (indicator_id,indicator_type_id,patient_id,location_id,encounter_id,
										indicator_date,voided,created_date,last_updated_date)
	SELECT 21,21,pdiag.patient_id, pdiag.location_id, pdiag.encounter_id, pdiag.encounter_date,
	pdiag.voided, now(), now() FROM isanteplus.patient p, isanteplus.patient_diagnosis pdiag
	WHERE p.patient_id = pdiag.patient_id
	AND pdiag.concept_id = 1284
	AND pdiag.answer_concept_id IN (142473,142474)
	AND pdiag.voided <> 1
	ON DUPLICATE KEY UPDATE
	last_updated_date = NOW(),
	voided = pdiag.voided;
	
	/*22 : Diarrhée aigue aqueuse*/
	INSERT INTO isanteplus.indicators (indicator_id,indicator_type_id,patient_id,location_id,encounter_id,
										indicator_date,voided,created_date,last_updated_date)
	SELECT 22,22,pdiag.patient_id, pdiag.location_id, pdiag.encounter_id, pdiag.encounter_date,
	pdiag.voided, now(), now() FROM isanteplus.patient p, isanteplus.patient_diagnosis pdiag
	WHERE p.patient_id = pdiag.patient_id
	AND pdiag.concept_id = 1284
	AND pdiag.answer_concept_id = 161887
	AND pdiag.voided <> 1
	ON DUPLICATE KEY UPDATE
	last_updated_date = NOW(),
	voided = pdiag.voided;
	
	/*23 : Diarrhée aigue sanglante*/
	INSERT INTO isanteplus.indicators (indicator_id,indicator_type_id,patient_id,location_id,encounter_id,
										indicator_date,voided,created_date,last_updated_date)
	SELECT 23,23,pdiag.patient_id, pdiag.location_id, pdiag.encounter_id, pdiag.encounter_date,
	pdiag.voided, now(), now() FROM isanteplus.patient p, isanteplus.patient_diagnosis pdiag
	WHERE p.patient_id = pdiag.patient_id
	AND pdiag.concept_id = 1284
	AND pdiag.answer_concept_id = 138868
	AND pdiag.voided <> 1
	ON DUPLICATE KEY UPDATE
	last_updated_date = NOW(),
	voided = pdiag.voided;
	
	/*24:Fièvre typhoïde suspecte*/
	INSERT INTO isanteplus.indicators (indicator_id,indicator_type_id,patient_id,location_id,encounter_id,
										indicator_date,voided,created_date,last_updated_date)
	SELECT 24,24,pdiag.patient_id, pdiag.location_id, pdiag.encounter_id, pdiag.encounter_date,
	pdiag.voided, now(), now() FROM isanteplus.patient p, isanteplus.patient_diagnosis pdiag
	WHERE p.patient_id = pdiag.patient_id
	AND pdiag.concept_id = 1284
	AND pdiag.answer_concept_id = 141
	AND pdiag.suspected_confirmed = 159393
	AND pdiag.voided <> 1
	ON DUPLICATE KEY UPDATE
	last_updated_date = NOW(),
	voided = pdiag.voided;
	
	/*25 : Filariose probable*/
	
	INSERT INTO isanteplus.indicators (indicator_id,indicator_type_id,patient_id,location_id,encounter_id,
										indicator_date,voided,created_date,last_updated_date)
	SELECT 25,25,pdiag.patient_id, pdiag.location_id, pdiag.encounter_id, pdiag.encounter_date,
	pdiag.voided, now(), now() FROM isanteplus.patient p, isanteplus.patient_diagnosis pdiag
	WHERE p.patient_id = pdiag.patient_id
	AND pdiag.concept_id = 1284
	AND pdiag.answer_concept_id = 119354
	AND pdiag.suspected_confirmed = 159393
	AND pdiag.voided <> 1
	ON DUPLICATE KEY UPDATE
	last_updated_date = NOW(),
	voided = pdiag.voided;
	
	/*26 : Infection respiratoire aigue*/
		INSERT INTO isanteplus.indicators (indicator_id,indicator_type_id,patient_id,location_id,encounter_id,
										indicator_date,voided,created_date,last_updated_date)
	SELECT 26,26,pdiag.patient_id, pdiag.location_id, pdiag.encounter_id, pdiag.encounter_date,
	pdiag.voided, now(), now() FROM isanteplus.patient p, isanteplus.patient_diagnosis pdiag
	WHERE p.patient_id = pdiag.patient_id
	AND pdiag.concept_id = 1284
	AND pdiag.answer_concept_id = 154983
	AND pdiag.voided <> 1
	ON DUPLICATE KEY UPDATE
	last_updated_date = NOW(),
	voided = pdiag.voided;
	
	/*27 : Syndrome ictérique fébrile*/
	INSERT INTO isanteplus.indicators (indicator_id,indicator_type_id,patient_id,location_id,encounter_id,
										indicator_date,voided,created_date,last_updated_date)
	SELECT 27,27,pdiag.patient_id, pdiag.location_id, pdiag.encounter_id, pdiag.encounter_date,
	pdiag.voided, now(), now() FROM isanteplus.patient p, isanteplus.patient_diagnosis pdiag
	WHERE p.patient_id = pdiag.patient_id
	AND pdiag.concept_id = 1284
	AND pdiag.answer_concept_id = 163402
	AND pdiag.voided <> 1
	ON DUPLICATE KEY UPDATE
	last_updated_date = NOW(),
	voided = pdiag.voided;
	
	/*28 : Tétanos*/
	INSERT INTO isanteplus.indicators (indicator_id,indicator_type_id,patient_id,location_id,encounter_id,
										indicator_date,voided,created_date,last_updated_date)
	SELECT 28,28,pdiag.patient_id, pdiag.location_id, pdiag.encounter_id, pdiag.encounter_date,
	pdiag.voided, now(), now() FROM isanteplus.patient p, isanteplus.patient_diagnosis pdiag
	WHERE p.patient_id = pdiag.patient_id
	AND pdiag.concept_id = 1284
	AND pdiag.answer_concept_id = 124957
	AND pdiag.voided <> 1
	ON DUPLICATE KEY UPDATE
	last_updated_date = NOW(),
	voided = pdiag.voided;
	
	/*29 : Accidents (domestiques, voie publique)*/
	INSERT INTO isanteplus.indicators (indicator_id,indicator_type_id,patient_id,location_id,encounter_id,
										indicator_date,voided,created_date,last_updated_date)
	SELECT 29,29,pdiag.patient_id, pdiag.location_id, pdiag.encounter_id, pdiag.encounter_date,
	pdiag.voided, now(), now() FROM isanteplus.patient p, isanteplus.patient_diagnosis pdiag
	WHERE p.patient_id = pdiag.patient_id
	AND pdiag.concept_id = 1284
	AND pdiag.answer_concept_id = 150452
	AND pdiag.voided <> 1
	ON DUPLICATE KEY UPDATE
	last_updated_date = NOW(),
	voided = pdiag.voided;
	/*30 : Cancers (seins, col de l’utérus, prostate, autres)*/
	INSERT INTO isanteplus.indicators (indicator_id,indicator_type_id,patient_id,location_id,encounter_id,
										indicator_date,voided,created_date,last_updated_date)
	SELECT 30,30,pdiag.patient_id, pdiag.location_id, pdiag.encounter_id, pdiag.encounter_date,
	pdiag.voided, now(), now() FROM isanteplus.patient p, isanteplus.patient_diagnosis pdiag
	WHERE p.patient_id = pdiag.patient_id
	AND pdiag.concept_id = 1284
	AND pdiag.answer_concept_id IN (113753,146221,116023)
	AND pdiag.voided <> 1
	ON DUPLICATE KEY UPDATE
	last_updated_date = NOW(),
	voided = pdiag.voided;
	
	/*31 : Epilepsie*/
	INSERT INTO isanteplus.indicators (indicator_id,indicator_type_id,patient_id,location_id,encounter_id,
										indicator_date,voided,created_date,last_updated_date)
	SELECT 31,31,pdiag.patient_id, pdiag.location_id, pdiag.encounter_id, pdiag.encounter_date,
	pdiag.voided, now(), now() FROM isanteplus.patient p, isanteplus.patient_diagnosis pdiag
	WHERE p.patient_id = pdiag.patient_id
	AND pdiag.concept_id = 1284
	AND pdiag.answer_concept_id = 155
	AND pdiag.voided <> 1
	ON DUPLICATE KEY UPDATE
	last_updated_date = NOW(),
	voided = pdiag.voided;
	
	/*32 : Hypertension artérielle (hta)*/
	INSERT INTO isanteplus.indicators (indicator_id,indicator_type_id,patient_id,location_id,encounter_id,
										indicator_date,voided,created_date,last_updated_date)
	SELECT 32,32,pdiag.patient_id, pdiag.location_id, pdiag.encounter_id, pdiag.encounter_date,
	pdiag.voided, now(), now() FROM isanteplus.patient p, isanteplus.patient_diagnosis pdiag
	WHERE p.patient_id = pdiag.patient_id
	AND pdiag.concept_id = 1284
	AND pdiag.answer_concept_id = 117399
	AND pdiag.voided <> 1
	ON DUPLICATE KEY UPDATE
	last_updated_date = NOW(),
	voided = pdiag.voided;
	
	/*33 : Infection sexuellement transmissible (ist)*/
	INSERT INTO isanteplus.indicators (indicator_id,indicator_type_id,patient_id,location_id,encounter_id,
										indicator_date,voided,created_date,last_updated_date)
	SELECT 33,33,pdiag.patient_id, pdiag.location_id, pdiag.encounter_id, pdiag.encounter_date,
	pdiag.voided, now(), now() FROM isanteplus.patient p, isanteplus.patient_diagnosis pdiag
	WHERE p.patient_id = pdiag.patient_id
	AND pdiag.concept_id = 1284
	AND pdiag.answer_concept_id = 112992
	AND pdiag.voided <> 1
	ON DUPLICATE KEY UPDATE
	last_updated_date = NOW(),
	voided = pdiag.voided;
	
	/* 34 : Lèpre suspecte */
	INSERT INTO isanteplus.indicators (indicator_id,indicator_type_id,patient_id,location_id,encounter_id,
										indicator_date,voided,created_date,last_updated_date)
	SELECT 24,24,pdiag.patient_id, pdiag.location_id, pdiag.encounter_id, pdiag.encounter_date,
	pdiag.voided, now(), now() FROM isanteplus.patient p, isanteplus.patient_diagnosis pdiag
	WHERE p.patient_id = pdiag.patient_id
	AND pdiag.concept_id = 1284
	AND pdiag.answer_concept_id = 116344
	AND pdiag.suspected_confirmed = 159393
	AND pdiag.voided <> 1
	ON DUPLICATE KEY UPDATE
	last_updated_date = NOW(),
	voided = pdiag.voided;
	
	/*35 : Malnutrition*/
	INSERT INTO isanteplus.indicators (indicator_id,indicator_type_id,patient_id,location_id,encounter_id,
										indicator_date,voided,created_date,last_updated_date)
	SELECT 35,35,pdiag.patient_id, pdiag.location_id, pdiag.encounter_id, pdiag.encounter_date,
	pdiag.voided, now(), now() FROM isanteplus.patient p, isanteplus.patient_diagnosis pdiag
	WHERE p.patient_id = pdiag.patient_id
	AND pdiag.concept_id = 1284
	AND pdiag.answer_concept_id IN (832,126598,134722,134723)
	AND pdiag.voided <> 1
	ON DUPLICATE KEY UPDATE
	last_updated_date = NOW(),
	voided = pdiag.voided;
	
	/*36 : Syphilis congénitale*/
	
	INSERT INTO isanteplus.indicators (indicator_id,indicator_type_id,patient_id,location_id,encounter_id,
										indicator_date,voided,created_date,last_updated_date)
	SELECT 36,36,pdiag.patient_id, pdiag.location_id, pdiag.encounter_id, pdiag.encounter_date,
	pdiag.voided, now(), now() FROM isanteplus.patient p, isanteplus.patient_diagnosis pdiag
	WHERE p.patient_id = pdiag.patient_id
	AND pdiag.concept_id = 1284
	AND pdiag.answer_concept_id = 143672
	AND pdiag.voided <> 1
	ON DUPLICATE KEY UPDATE
	last_updated_date = NOW(),
	voided = pdiag.voided;
	
	/*37 : Violences (physique, sexuelle)*/
	INSERT INTO isanteplus.indicators (indicator_id,indicator_type_id,patient_id,location_id,encounter_id,
										indicator_date,voided,created_date,last_updated_date)
	SELECT 37,37,pdiag.patient_id, pdiag.location_id, pdiag.encounter_id, pdiag.encounter_date,
	pdiag.voided, now(), now() FROM isanteplus.patient p, isanteplus.patient_diagnosis pdiag
	WHERE p.patient_id = pdiag.patient_id
	AND pdiag.concept_id = 1284
	AND pdiag.answer_concept_id = 158358
	AND pdiag.voided <> 1
	ON DUPLICATE KEY UPDATE
	last_updated_date = NOW(),
	voided = pdiag.voided;
  END$$
DELIMITER ;

DELIMITER $$
	DROP PROCEDURE IF EXISTS report_indicators_procedure$$
	CREATE PROCEDURE report_indicators_procedure()
	BEGIN
		call patient_diagnosis();
		call report_indicators();
	END$$
DELIMITER ;

DROP EVENT if exists report_indicators_event;
	CREATE EVENT if not exists report_indicators_event
	ON SCHEDULE EVERY 10 MINUTE
	 STARTS now()
		DO
		call report_indicators_procedure();
	
	