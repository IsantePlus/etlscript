use isanteplus;

DELIMITER $$
	DROP PROCEDURE IF EXISTS patient_status_arv$$
	CREATE PROCEDURE patient_status_arv()
	BEGIN
     /* DECLARE myIndex INT;
		select count(*) into myIndex from information_schema.statistics where table_name = 'patient_status_arv' and index_name = 'patient_status_arv_index' and table_schema = 'isanteplus';
		if(myIndex=0) then 
			create unique index patient_status_arv_index on patient_status_arv (patient_id, id_status, start_date);
		end if;*/
	SET SQL_SAFE_UPDATES = 0;
	/*SET FOREIGN_KEY_CHECKS = 0;*/
	
	/*Insertion for exposed infants*/
		/*Le dernier PCR en date doit être négatif fiche Premiere visite VIH pediatrique et Laboratoire 
			condition_exposee = 1
		*/
		truncate table exposed_infants;
		INSERT INTO exposed_infants(patient_id,location_id,encounter_id,visit_date,condition_exposee)
	SELECT vt.patient_id,vt.location_id,vt.encounter_id,vt.encounter_date,1
	FROM virological_tests vt,(SELECT vtest.patient_id,vtest.location_id,vtest.encounter_id,
	MAX(vtest.encounter_date) as v_date, 1 FROM virological_tests vtest 
	WHERE vtest.test_id = 162087 AND vtest.answer_concept_id = 1030 GROUP BY 1) B
	WHERE vt.patient_id = B.patient_id
	AND vt.encounter_date = B.v_date
	AND vt.test_id = 162087
	AND vt.answer_concept_id = 1030
	AND vt.test_result = 664;
		
	/*	PCR_Concept_id=844,Positif=1301,Negatif=1302,Equivoque=1300,Echantillon de pauvre qualite=1304
		Fiche laboratoire, condition_exposee = 2
		*/
	INSERT INTO exposed_infants(patient_id,location_id,encounter_id,visit_date,condition_exposee)
	SELECT pl.patient_id,pl.location_id,pl.encounter_id,pl.visit_date,2
	FROM patient_laboratory pl,(SELECT plab.patient_id,plab.location_id,
	plab.encounter_id, MAX(plab.visit_date) as v_date,2 FROM patient_laboratory plab 
	WHERE plab.test_id = 844 GROUP BY 1) B
	WHERE pl.patient_id = B.patient_id
	AND pl.visit_date = B.v_date
	AND pl.test_id = 844
	AND pl.test_done = 1
	AND pl.test_result = 1302;
	/*	Condition B - Enfant exposé doit être coché
		Fiche Premiere visit VIH pediatrique
		condition_exposee = 3
	*/
	INSERT INTO exposed_infants(patient_id,location_id,encounter_id,visit_date,condition_exposee)
					select distinct ob.person_id,ob.location_id,ob.encounter_id,
					DATE(enc.encounter_datetime),3
					from openmrs.obs ob, openmrs.encounter enc, 
					openmrs.encounter_type ent
					WHERE ob.encounter_id	=	enc.encounter_id
					AND enc.encounter_type	=	ent.encounter_type_id
                    AND ob.concept_id = 1401
					AND ob.value_coded = 1405
					AND (ent.uuid =	"349ae0b4-65c1-4122-aa06-480f186c8350"
						OR ent.uuid = "33491314-c352-42d0-bd5d-a9d0bffc9bf1");
	
	/* Condition D - Des ARV prescrits en prophylaxie
		patient_prescription.rx_or_prophy=163768
		Fiche Ordonance medicale, condition_exposee = 4
		*/
		INSERT INTO exposed_infants(patient_id,location_id,encounter_id,visit_date,condition_exposee)
		select distinct pdisp.patient_id,pdisp.location_id,pdisp.encounter_id,pdisp.visit_date,4
		from patient_dispensing pdisp, (select ppres.patient_id, 
					MAX(ppres.visit_date) as visit_date FROM patient_dispensing ppres 
					WHERE ppres.arv_drug = 1065 GROUP BY 1) B
		WHERE pdisp.patient_id = B.patient_id
		AND pdisp.visit_date = B.visit_date
		AND pdisp.rx_or_prophy = 163768
		AND pdisp.arv_drug = 1065; 
		
		
	
	/*End insertion for exposed infants*/
	/*Delete all patient with PCR positive from exposed_infants table*/
	DROP table IF EXISTS patient_pcr_temp;
	CREATE TEMPORARY TABLE IF NOT EXISTS patient_pcr_temp
	SELECT distinct o.person_id as patient_id, o.encounter_id as encounter_id,
	o.concept_id as concept_id, o.value_coded as value_coded
	FROM openmrs.obs o,
	(select e.patient_id, MAX(e.encounter_id) as encounter_id, ob1.concept_id FROM openmrs.encounter e, openmrs.obs ob1 
	WHERE e.patient_id = ob1.person_id AND e.encounter_id = ob1.encounter_id AND (ob1.concept_id IN (844,1030) OR ob1.value_coded = 844) GROUP BY 1) C
	WHERE o.person_id = C.patient_id
		AND o.encounter_id = C.encounter_id
        AND o.concept_id = C.concept_id;
		
	
	DELETE FROM exposed_infants WHERE 
	patient_id IN (SELECT ppt.patient_id FROM patient_pcr_temp ppt WHERE 
	ppt.concept_id IN (844,1030) AND ppt.value_coded IN(703,1301));
	
	truncate table patient_pcr_temp;
	
	/*Delete from exposed_infants where patient has a HIV Positive TEST */
	DELETE FROM exposed_infants WHERE 
	patient_id IN (SELECT pl.patient_id FROM patient_laboratory pl, patient p 
	WHERE pl.patient_id = p.patient_id AND pl.test_id = 1040 
	AND pl.test_done = 1 AND pl.test_result = 703 
	AND (TIMESTAMPDIFF(MONTH, p.birthdate,DATE(now())) >= 18));
	
	/*DELETE from exposed_infants where VIH positif - confirmé par test sérologique > 18 mois*/
	DELETE FROM exposed_infants WHERE patient_id IN
	(select distinct ob.person_id from openmrs.obs ob, openmrs.encounter enc, openmrs.encounter_type ent,
	(select o.person_id, MAX(DATE(o.obs_datetime)) as obs_date 
	from openmrs.obs o WHERE o.concept_id = 1401 AND o.value_coded = 1405 GROUP BY 1) B
					WHERE ob.encounter_id	=	enc.encounter_id
					AND enc.encounter_type	=	ent.encounter_type_id
					AND ob.person_id = B.person_id
                    AND ob.concept_id = 1401
					AND ob.value_coded = 163717
					AND (ent.uuid =	"349ae0b4-65c1-4122-aa06-480f186c8350"
						OR ent.uuid = "33491314-c352-42d0-bd5d-a9d0bffc9bf1")
					AND DATE(enc.encounter_datetime) > B.obs_date);
						
	/*	Condition 5 - Séroréversion doit être coché
		Rapport d'arrêt du programme soins et traitement VIH/SIDA
		condition_exposee = 5
	*/
	INSERT INTO exposed_infants(patient_id,location_id,encounter_id,visit_date,condition_exposee)
					select distinct ob.person_id,ob.location_id,ob.encounter_id,
					DATE(enc.encounter_datetime),5
					from openmrs.obs ob, openmrs.encounter enc, 
					openmrs.encounter_type ent
					WHERE ob.encounter_id	=	enc.encounter_id
					AND enc.encounter_type	=	ent.encounter_type_id
                    AND ob.concept_id = 1667
					AND ob.value_coded = 165439
					AND ent.uuid =	"9d0113c6-f23a-4461-8428-7e9a7344f2ba";
	
	/*TRUNCATE TABLE patient_status_arv;*/
		/*Insertion for patient_status Décédés=1,Arrêtés=2,Transférés=3 on ARV
		We use max(start_date) OR max(date_started) because
		we can't find the historic of the patient status
	*/
	/*Starting patient_status_arv*/
	
/*====================================================*/
/*Insertion for patient_status Décédés en Pré-ARV=4,
Transférés en Pré-ARV=5*/
INSERT INTO patient_status_arv(patient_id,id_status,start_date,encounter_id,last_updated_date, date_started_status)
	SELECT v.patient_id,
	CASE WHEN (ob.value_coded=159) THEN 4
	WHEN (ob.value_coded=159492) THEN 5
	END as id_status,DATE(v.date_started) AS start_date, enc.encounter_id as encounter_id, now(), now()
	FROM isanteplus.patient ispat,openmrs.visit v,
	openmrs.encounter_type entype,openmrs.encounter enc,
	openmrs.obs ob, (SELECT pvi.patient_id, MAX(DATE(pvi.date_started)) as visit_date 
						FROM openmrs.visit pvi where pvi.voided = 0 GROUP BY 1) B
	WHERE ispat.patient_id=v.patient_id
	AND v.visit_id=enc.visit_id
	AND entype.encounter_type_id=enc.encounter_type
	AND enc.encounter_id=ob.encounter_id
	AND v.patient_id = B.patient_id
	AND v.date_started = B.visit_date
	AND entype.uuid='9d0113c6-f23a-4461-8428-7e9a7344f2ba'
	AND ob.concept_id=161555
	AND ispat.vih_status=1
	AND enc.patient_id NOT IN(SELECT parv.patient_id 
	FROM isanteplus.patient_on_arv parv)
	AND ob.value_coded IN(159,159492)
	AND ob.voided = 0
	GROUP BY v.patient_id
	on duplicate key update 
	last_updated_date = values(last_updated_date);
	/*Insertion for patient_status réguliers=6*/
	INSERT INTO patient_status_arv(patient_id,id_status,start_date,encounter_id,last_updated_date, date_started_status)
	SELECT pdis.patient_id,6 as id_status,MAX(DATE(pdis.visit_date)) as start_date,pdis.encounter_id as encounter_id,
	now(), now()
	FROM isanteplus.patient ipat,isanteplus.patient_dispensing pdis,isanteplus.patient_on_arv p,
	(select pdisp.patient_id, MAX(pdisp.next_dispensation_date) as mnext_disp 
	from isanteplus.patient_dispensing pdisp WHERE pdisp.voided <> 1 AND pdisp.arv_drug = 1065 group by 1) mndisp,
	openmrs.encounter enc,
	openmrs.encounter_type entype
	WHERE ipat.patient_id = pdis.patient_id
	AND pdis.visit_id = enc.visit_id
	AND pdis.patient_id = mndisp.patient_id
	AND pdis.next_dispensation_date = mndisp.mnext_disp
	AND enc.encounter_type=entype.encounter_type_id
	AND enc.patient_id	
	NOT IN(SELECT dreason.patient_id FROM discontinuation_reason dreason
	WHERE dreason.reason IN(159,1667,159492))
	AND enc.patient_id = p.patient_id
	AND pdis.arv_drug = 1065
	AND entype.uuid IN ('10d73929-54b6-4d18-a647-8b7316bc1ae3',
	                        'a9392241-109f-4d67-885b-57cc4b8c638f'
							)
	AND((DATE(now()) <= pdis.next_dispensation_date))
	GROUP BY pdis.patient_id
	on duplicate key update 
	last_updated_date = values(last_updated_date);
	
	/*create index patient_status_arv_index_a on patient_status_arv_temp_a (patient_id, id_status, start_date);*/
	/*Adding status into patient_status_arv table */
	/*INSERT INTO patient_status_arv(patient_id,id_status,start_date,last_updated_date)
    select distinct psat.patient_id, psat.id_status, psat.start_date, now() 
	from patient_status_arv_temp_a psat
	on duplicate key update 
	start_date = start_date,
	last_updated_date = now();*/
	
	/*truncate the temporary table after the insertion */
	/*truncate table patient_status_arv_temp_a;*/
/*=========================================================*/
	
/*Insertion for patient_status Rendez-vous ratés=8*/
	/*INSERT INTO patient_status_arv_temp_a*/
	INSERT INTO patient_status_arv(patient_id,id_status,start_date, encounter_id,last_updated_date, date_started_status)
	SELECT pdis.patient_id,8 as id_status,MAX(DATE(pdis.visit_date)) as start_date, pdis.encounter_id as encounter_id,
	now(), now()
	FROM isanteplus.patient ipat,isanteplus.patient_dispensing pdis,
	(select pdisp.patient_id, MAX(pdisp.next_dispensation_date) as mnext_disp 
	from isanteplus.patient_dispensing pdisp WHERE pdisp.voided <> 1 AND pdisp.arv_drug = 1065 group by 1) mndisp,
	openmrs.encounter enc,
	openmrs.encounter_type entype
	WHERE ipat.patient_id=pdis.patient_id
	AND pdis.visit_id=enc.visit_id
	AND pdis.patient_id = mndisp.patient_id
	AND pdis.next_dispensation_date = mndisp.mnext_disp
	AND enc.encounter_type=entype.encounter_type_id
	AND enc.patient_id	
	NOT IN(SELECT dreason.patient_id FROM discontinuation_reason dreason
	WHERE dreason.reason IN(159,1667,159492))
	AND enc.patient_id IN (SELECT parv.patient_id 
	FROM isanteplus.patient_on_arv parv)
	AND entype.uuid IN ('10d73929-54b6-4d18-a647-8b7316bc1ae3',
	                        'a9392241-109f-4d67-885b-57cc4b8c638f'
							) 
	AND (DATEDIFF(DATE(now()),pdis.next_dispensation_date)<=30)
	AND((DATE(now()) > pdis.next_dispensation_date))
	GROUP BY pdis.patient_id
	on duplicate key update 
	last_updated_date = values(last_updated_date);

	/*Insertion for status on the table patient_arv_status Rendez-vous ratés=8*/
	/*Adding status into patient_status_arv table */
	/*INSERT INTO patient_status_arv(patient_id,id_status,start_date,last_updated_date)
    select distinct psat.patient_id,psat.id_status,psat.start_date,now() 
	from patient_status_arv_temp_a psat
	on duplicate key update 
	start_date = psat.start_date,
	last_updated_date = now();*/
	/*truncate the temporary table after the insertion */
	/*truncate table patient_status_arv_temp_a;*/
	
/*Insertion for patient_status Perdus de vue=9*/
	INSERT INTO patient_status_arv(patient_id,id_status,start_date,encounter_id,last_updated_date, date_started_status)
	SELECT pdis.patient_id,9 as id_status,MAX(DATE(pdis.visit_date)) as start_date, pdis.encounter_id as encounter_id,
	now(), now()
	FROM isanteplus.patient_dispensing pdis,
	(select pdisp.patient_id, MAX(pdisp.next_dispensation_date) as mnext_disp 
	from isanteplus.patient_dispensing pdisp WHERE pdisp.voided <> 1 AND pdisp.arv_drug = 1065 group by 1) mndisp,
	openmrs.encounter enc,openmrs.encounter_type entype
	WHERE pdis.visit_id=enc.visit_id
	AND pdis.patient_id = mndisp.patient_id
	AND pdis.next_dispensation_date = mndisp.mnext_disp
	AND enc.encounter_type=entype.encounter_type_id
	AND enc.patient_id 
	NOT IN(SELECT dreason.patient_id FROM discontinuation_reason dreason
	WHERE dreason.reason IN(159,1667,159492))
	AND enc.patient_id IN (SELECT parv.patient_id 
	FROM isanteplus.patient_on_arv parv)
	AND pdis.arv_drug = 1065
	AND (DATE(now()) > pdis.next_dispensation_date)
	AND (DATEDIFF(DATE(now()),pdis.next_dispensation_date)>30)
	AND entype.uuid IN ('10d73929-54b6-4d18-a647-8b7316bc1ae3',
	                        'a9392241-109f-4d67-885b-57cc4b8c638f'
							)
	GROUP BY pdis.patient_id
	on duplicate key update 
	last_updated_date = values(last_updated_date);
	
	/*Insertion for status on the table patient_arv_status Perdus de vue=9*/
	/*Adding status into patient_status_arv table */
	/*INSERT INTO patient_status_arv(patient_id,id_status,start_date,last_updated_date)
    select distinct psat.patient_id,psat.id_status,psat.start_date,now() 
	from patient_status_arv_temp_a psat
	on duplicate key update 
	start_date = psat.start_date,
	last_updated_date = now();*/
	/*truncate the temporary table after the insertion */
	/*truncate table patient_status_arv_temp_a;*/
	
/*INSERTION for patient status,
     Perdus de vue en Pré-ARV=10 */
INSERT INTO patient_status_arv(patient_id,id_status,start_date,encounter_id,last_updated_date,date_started_status)
	SELECT v.patient_id,10,
	MAX(DATE(v.date_started)) AS start_date, enc.encounter_id as encounter_id, now(), now()
	FROM isanteplus.patient ispat,
	openmrs.visit v,openmrs.encounter enc,
	openmrs.encounter_type entype, (SELECT pvi.patient_id, MAX(DATE(pvi.date_started)) as visit_date 
						FROM openmrs.visit pvi WHERE pvi.voided <> 1 GROUP BY 1) B
	WHERE ispat.patient_id=v.patient_id
	AND v.visit_id=enc.visit_id 
	AND enc.encounter_type=entype.encounter_type_id
	AND v.patient_id = B.patient_id
	AND v.date_started = B.visit_date
	AND enc.patient_id NOT IN 
	(SELECT dreason.patient_id FROM discontinuation_reason dreason
	WHERE dreason.reason IN(159,159492))
	AND ispat.vih_status=1
	AND ispat.patient_id NOT IN (SELECT parv.patient_id
	FROM isanteplus.patient_on_arv parv)
	AND entype.uuid NOT IN('17536ba6-dd7c-4f58-8014-08c7cb798ac7',
		'349ae0b4-65c1-4122-aa06-480f186c8350',
		'204ad066-c5c2-4229-9a62-644bc5617ca2',
		'33491314-c352-42d0-bd5d-a9d0bffc9bf1',
		'10d73929-54b6-4d18-a647-8b7316bc1ae3',
		'a9392241-109f-4d67-885b-57cc4b8c638f',
		'f037e97b-471e-4898-a07c-b8e169e0ddc4'
		)
	AND (TIMESTAMPDIFF(MONTH, v.date_started,DATE(now())) > 12)
	GROUP BY v.patient_id
	on duplicate key update 
	last_updated_date = values(last_updated_date);
	/*=========================================================*/
	/*INSERTION for patient status Recent on PRE-ART=7,Actifs en Pré-ARV=11 */
INSERT INTO patient_status_arv(patient_id,id_status,start_date,encounter_id,last_updated_date,date_started_status)
	SELECT v.patient_id,
	CASE WHEN 
		(TIMESTAMPDIFF(MONTH,v.date_started,DATE(now()))<=12)
		AND (entype.uuid IN('17536ba6-dd7c-4f58-8014-08c7cb798ac7',
		'349ae0b4-65c1-4122-aa06-480f186c8350')) THEN 7
	   WHEN 
	   (TIMESTAMPDIFF(MONTH, v.date_started,DATE(now()))<=12) 
		AND (entype.uuid IN('204ad066-c5c2-4229-9a62-644bc5617ca2',
		'33491314-c352-42d0-bd5d-a9d0bffc9bf1',
		'10d73929-54b6-4d18-a647-8b7316bc1ae3',
		'a9392241-109f-4d67-885b-57cc4b8c638f',
		'f037e97b-471e-4898-a07c-b8e169e0ddc4')) THEN 11
	END,
	MAX(DATE(v.date_started)) AS start_date, enc.encounter_id as encounter_id, now(), now()
	FROM isanteplus.patient ispat,
	openmrs.visit v,openmrs.encounter enc,
	openmrs.encounter_type entype,(SELECT pvi.patient_id, MAX(DATE(pvi.date_started)) as visit_date 
						FROM openmrs.visit pvi WHERE pvi.voided <> 1 GROUP BY 1) B
	WHERE ispat.patient_id=v.patient_id
	AND v.visit_id=enc.visit_id 
	AND enc.encounter_type=entype.encounter_type_id
	AND v.patient_id = B.patient_id
	AND v.date_started = B.visit_date
	AND enc.patient_id NOT IN 
	(SELECT dreason.patient_id FROM discontinuation_reason dreason
	WHERE dreason.reason IN(159,159492))
	AND ispat.vih_status=1
	AND ispat.patient_id NOT IN (SELECT parv.patient_id
	FROM isanteplus.patient_on_arv parv)
	AND entype.uuid IN('17536ba6-dd7c-4f58-8014-08c7cb798ac7',
		'349ae0b4-65c1-4122-aa06-480f186c8350',
		'204ad066-c5c2-4229-9a62-644bc5617ca2',
		'33491314-c352-42d0-bd5d-a9d0bffc9bf1',
		'10d73929-54b6-4d18-a647-8b7316bc1ae3',
		'a9392241-109f-4d67-885b-57cc4b8c638f',
		'f037e97b-471e-4898-a07c-b8e169e0ddc4'
		)
	AND (TIMESTAMPDIFF(MONTH,v.date_started,DATE(now()))<=12)
	GROUP BY v.patient_id
	on duplicate key update 
	last_updated_date = values(last_updated_date);
	
	/*Décédés=1*/
	INSERT INTO patient_status_arv(patient_id,id_status,start_date,encounter_id,
	last_updated_date, date_started_status)
	SELECT enc.patient_id, 1 as id_status, MAX(DATE(enc.encounter_datetime)) AS start_date,
	enc.encounter_id as encounter_id, now(), now()
	FROM openmrs.encounter enc,openmrs.encounter_type entype,openmrs.obs ob,
	isanteplus.patient_on_arv parv
	WHERE enc.encounter_type = entype.encounter_type_id
	AND enc.patient_id = ob.person_id
	AND enc.encounter_id = ob.encounter_id
	AND enc.patient_id = parv.patient_id
	AND entype.uuid = '9d0113c6-f23a-4461-8428-7e9a7344f2ba'
	AND ob.concept_id = 161555
	AND ob.value_coded = 159
	AND ob.voided = 0
	AND enc.voided = 0
	GROUP BY enc.patient_id
	on duplicate key update 
	last_updated_date = values(last_updated_date);
	
	/*Transférés=2*/
	
	INSERT INTO patient_status_arv(patient_id,id_status,start_date,encounter_id,
	last_updated_date, date_started_status)
	SELECT enc.patient_id, 2 as id_status, MAX(DATE(enc.encounter_datetime)) AS start_date,
	enc.encounter_id as encounter_id, now(), now()
	FROM openmrs.encounter enc,openmrs.encounter_type entype,openmrs.obs ob,
	isanteplus.patient_on_arv parv
	WHERE enc.encounter_type = entype.encounter_type_id
	AND enc.patient_id = ob.person_id
	AND enc.encounter_id = ob.encounter_id
	AND enc.patient_id = parv.patient_id
	AND entype.uuid = '9d0113c6-f23a-4461-8428-7e9a7344f2ba'
	AND ob.concept_id = 161555
	AND ob.value_coded = 159492
	AND ob.voided = 0
	AND enc.voided = 0
	GROUP BY enc.patient_id
	on duplicate key update 
	last_updated_date = values(last_updated_date);
	
	/*Arrêtés=3*/
	INSERT INTO patient_status_arv(patient_id,id_status,start_date,encounter_id,
	last_updated_date, date_started_status)
	SELECT enc.patient_id,3 as id_status, MAX(DATE(enc.encounter_datetime)) AS start_date, 
	enc.encounter_id as encounter_id, now(), now()
	FROM openmrs.encounter enc,
	openmrs.encounter_type entype,openmrs.obs ob, openmrs.obs ob2,isanteplus.patient_on_arv parv
	WHERE enc.encounter_type = entype.encounter_type_id
	AND enc.patient_id = ob.person_id
	AND enc.encounter_id = ob.encounter_id
	AND enc.patient_id = parv.patient_id
	AND ob.encounter_id = ob2.encounter_id
	AND entype.uuid = '9d0113c6-f23a-4461-8428-7e9a7344f2ba'
	AND ob.concept_id = 161555
	AND ob.value_coded = 1667
	AND ob.voided = 0
	AND enc.voided = 0
	AND ob2.concept_id = 1667
	AND ob2.value_coded IN (115198,159737)
	GROUP BY enc.patient_id
	on duplicate key update 
	last_updated_date = values(last_updated_date);
		
	/*DROP TABLE patient_status_arv_temp_a;*/
	
	/*===========================================================*/
	/*UPDATE Discontinuations reason in table patient_status_ARV*/
	UPDATE patient_status_arv psarv,discontinuation_reason dreason
	       SET psarv.dis_reason=dreason.reason
		   WHERE psarv.patient_id=dreason.patient_id
		   AND psarv.start_date <= dreason.visit_date;	
	/*Delete Exposed infants from patient_arv_status*/
	DELETE FROM patient_status_arv WHERE 
	patient_id IN (SELECT ei.patient_id FROM exposed_infants ei);
   /*Update patient table for having the last patient arv status*/
   update patient p,patient_status_arv psa, 
   (SELECT psarv.patient_id, MAX(psarv.last_updated_date) as last_updated_date 
	                       FROM patient_status_arv psarv GROUP BY 1) B
     SET p.arv_status = psa.id_status
	 WHERE p.patient_id = psa.patient_id
	 AND psa.patient_id = B.patient_id
	 AND DATE(psa.last_updated_date) = DATE(B.last_updated_date);
	/*End of patient Status*/	
		
	END$$
DELIMITER ;
	
DELIMITER $$
	DROP PROCEDURE IF EXISTS isanteplusregimen_dml$$
	CREATE PROCEDURE isanteplusregimen_dml()
		BEGIN
			SET SQL_SAFE_UPDATES = 0;
			/*SET FOREIGN_KEY_CHECKS = 0;*/
			
			DROP TABLE if exists pepfarTableTemp;
			DROP TABLE if exists oneDrugRegimenPrefixTemp;
			DROP TABLE if exists twoDrugRegimenPrefixTemp;
			/*Insertion regimen for one drug arv*/
			create temporary table pepfarTableTemp
			(location_id int(11),
			patient_id int(11),
			visit_date datetime,
			regimen varchar(255),
			rx_or_prophy int(11));

			create temporary table oneDrugRegimenPrefixTemp (
			location_id int(11),
			patient_id int(11),
			visit_date datetime,
			drugID1 int(11),
			rx_or_prophy int(11)
			);

			insert into oneDrugRegimenPrefixTemp
			select d1.location_id, d1.patient_id, d1.visit_date, d1.drug_id, d1.rx_or_prophy
			from patient_prescription d1
			join patient p on d1.patient_id = p.patient_id
			join (select distinct drugID1 from regimen) r
			on r.drugID1 = d1.drug_id
			where p.vih_status = 1
			AND d1.arv_drug = 1065
			AND d1.voided <> 1;

			insert into pepfarTableTemp (location_id, patient_id, visit_date, regimen, rx_or_prophy)
			select distinct location_id, patient_id, visit_date, shortname, rx_or_prophy
			from oneDrugRegimenPrefixTemp d1
			join regimen r
			on r.drugID1 = d1.drugID1
			where r.drugID2 = 0
			and r.drugID3 = 0;

			/*Insertion regimen for two drugs arv*/
			create temporary table twoDrugRegimenPrefixTemp (
			location_id int(11),
			patient_id int(11),
			visit_date datetime,
			drugID1 int(11),
			drugID2 int(11),
			rx_or_prophy int(11)
			);

			insert into twoDrugRegimenPrefixTemp
			select location_id, patient_id, visit_date, d1.drugID1, d2.drug_id, d1.rx_or_prophy
			from oneDrugRegimenPrefixTemp d1
			join patient_prescription d2 using (location_id, patient_id, visit_date)
			join (select distinct drugID1, drugID2 from regimen) r
			on r.drugID1 = d1.drugID1
			and r.drugID2 = d2.drug_id
			WHERE d2.voided <> 1;

			insert into pepfarTableTemp (location_id, patient_id, visit_date, regimen, rx_or_prophy)
			select distinct location_id, patient_id, visit_date, shortname, prefix.rx_or_prophy
			from twoDrugRegimenPrefixTemp prefix
			join regimen r
			on prefix.drugID1 = r.drugID1
			and prefix.drugID2 = r.drugID2
			where r.drugID3 = 0;

			/*Insertion regimen for three drugs arv*/

			insert into pepfarTableTemp (location_id, patient_id, visit_date, regimen, rx_or_prophy)
			select distinct location_id, patient_id, visit_date, shortname,prefix.rx_or_prophy
			from twoDrugRegimenPrefixTemp prefix
			join patient_prescription using (location_id, patient_id, visit_date)
			join regimen r
			on prefix.drugID1 = r.drugID1
			and prefix.drugID2 = r.drugID2
			and patient_prescription.drug_id = r.drugID3
			where r.drugID3 != 0
			AND patient_prescription.voided <> 1;

			insert into pepfarTable (location_id, patient_id, visit_date, regimen, rx_or_prophy, last_updated_date)
			select p.location_id, p.patient_id, p.visit_date, p.regimen, p.rx_or_prophy, now() from pepfarTableTemp p
			ON DUPLICATE KEY UPDATE
			rx_or_prophy = p.rx_or_prophy,
			last_updated_date = now();

			INSERT INTO openmrs.isanteplus_patient_arv (patient_id, arv_regimen, date_created, date_changed)
			SELECT pft.patient_id, pft.regimen, pft.visit_date, now() FROM pepfarTable pft, 
			(SELECT pf.patient_id, max(pf.visit_date) as visit_date_regimen FROM pepfarTable pf GROUP BY 1) B
			WHERE pft.patient_id = B.patient_id
			AND pft.visit_date = B.visit_date_regimen
			ON DUPLICATE KEY UPDATE
			arv_regimen = pft.regimen,
			date_changed = now();


			drop temporary table oneDrugRegimenPrefixTemp;
			drop temporary table twoDrugRegimenPrefixTemp;
			drop temporary table pepfarTableTemp;
			
        /*Transfer next_visit_date, date_started_arv, petient_status to 
		openmrs.isanteplus_patient_arv table*/
		INSERT INTO openmrs.isanteplus_patient_arv
		(patient_id, arv_status, date_started_arv, next_visit_date, date_created, date_changed)
		SELECT p.patient_id, asl.name_fr,DATE(p.date_started_arv), 
		DATE(p.next_visit_date), now(), now() FROM isanteplus.patient p
		LEFT OUTER JOIN isanteplus.arv_status_loockup asl
		ON p.arv_status = asl.id
		WHERE(
			p.arv_status is not null
			OR p.next_visit_date is not null
			OR p.date_started_arv is not null
		)
		ON DUPLICATE KEY UPDATE 
		arv_status = asl.name_fr,
		date_started_arv = p.date_started_arv,
		next_visit_date = p.next_visit_date,
		date_changed = now();

		END$$
	DELIMITER ;
	
	DELIMITER $$
	DROP PROCEDURE IF EXISTS isanteplus_patient_alert$$
	CREATE PROCEDURE isanteplus_patient_alert()
	BEGIN
		
		/*INSERT FLAGS*/
				/*use openmrs;*/
				SET SQL_SAFE_UPDATES = 0;
				SET FOREIGN_KEY_CHECKS = 0;
				truncate table openmrs.patientflags_flag_tag;
				truncate table openmrs.patientflags_tag_displaypoint;
				truncate table openmrs.patientflags_flag;
				truncate table openmrs.patientflags_tag;
				truncate table openmrs.patientflags_priority;
				
				SET SQL_SAFE_UPDATES = 1;
				SET FOREIGN_KEY_CHECKS = 1;

				INSERT INTO openmrs.patientflags_tag VALUES (2,'Tag',NULL,1,'2018-05-28 09:44:50',NULL,NULL,0,NULL,NULL,NULL,'4dbe134d-a67a-44be-871f-5890b05d328c');
			 	
			 	INSERT INTO openmrs.patientflags_priority VALUES 
				(1,'Liste VL','color:red',1,NULL,1,'2018-05-28 02:17:38',1,'2018-05-28 02:19:27',0,NULL,NULL,NULL,'f2e0e461-170e-4df9-80fc-da2d93663328');
				INSERT INTO openmrs.patientflags_priority VALUES 
				(2,'Liste Medicament','color: red',2,NULL,1,'2018-05-31 15:02:47',NULL,NULL,0,NULL,NULL,NULL,'5d87ef2b-5cc2-4ef5-a241-a122977170d6');
				INSERT INTO openmrs.patientflags_priority VALUES 
				(3,'Liste TB','color: blue',3,NULL,1,'2018-05-31 15:02:47',NULL,NULL,0,NULL,NULL,NULL,'439d2dfa-29ee-4271-9e18-97a80d0eb475');
		
				/* Dernière charge virale de ce patient remonte à au moins 12 mois */
			 	INSERT INTO openmrs.patientflags_flag VALUES 
				(2,'Dernière charge virale de ce patient remonte à au moins 12 mois',
				'select distinct a.patient_id FROM isanteplus.alert a WHERE a.id_alert = 4',
				'La dernière charge virale de ce patient remonte à au moins 12 mois',1,
				'org.openmrs.module.patientflags.evaluator.SQLFlagEvaluator',
				NULL,1,'2018-05-28 02:18:18',1,'2018-05-31 13:43:43',0,NULL,NULL,NULL,
				'8c176fcb-9354-43fa-b13c-c293e6f910dc',1);
				
				/*patient sous ARV depuis 6 mois sans un résultat de charge virale*/
				
				INSERT INTO openmrs.patientflags_flag VALUES 
				(3,'patient sous ARV depuis 6 mois sans un résultat de charge virale',
				'select distinct a.patient_id FROM isanteplus.alert a WHERE a.id_alert = 1',
				 'Le patient est sous ARV depuis 6 mois sans un résultat de charge virale',1,
				 'org.openmrs.module.patientflags.evaluator.SQLFlagEvaluator',NULL,
				 1,'2018-05-31 14:58:13',1,'2018-05-31 14:59:31',0,NULL,NULL,NULL,
				 '1d968997-4d6d-41d4-ab91-9b7936030ace',1);

				/* Patient sous ARV et traitement anti tuberculeux */
				
				INSERT INTO openmrs.patientflags_flag VALUES 
				(4,'Patient sous ARV et traitement anti tuberculeux',
				'select distinct a.patient_id FROM isanteplus.alert a WHERE a.id_alert = 9',
				'Patient sous ARV et traitement anti TB',1,
				 'org.openmrs.module.patientflags.evaluator.SQLFlagEvaluator',NULL,1,'2018-05-31 15:03:40',
				 NULL,NULL,0,NULL,NULL,NULL,'a1d4c4ba-348c-456d-aca1-755190b78b0c',3);
				 
				 
				 /* Dernière charge virale de ce patient remonte à au moins 3 mois et le résultat était supérieur 1000 copies/ml */
			 	INSERT INTO openmrs.patientflags_flag VALUES 
				(5,'Dernière charge virale de ce patient remonte à au moins 3 mois et le résultat était supérieur 1000 copies/ml',
				'select distinct a.patient_id FROM isanteplus.alert a WHERE a.id_alert = 5',
				'La dernière charge virale de ce patient remonte à au moins 3 mois et le résultat était > 1000 copies/ml',1,
				'org.openmrs.module.patientflags.evaluator.SQLFlagEvaluator',
				NULL,1,'2018-05-28 02:18:18',1,'2018-05-31 13:43:43',0,NULL,NULL,NULL,
				'8c176fcb-9354-43fa-b13c-c293e6f910dc',1);
				/*Le patient doit venir renflouer ses ARV dans les 30 prochains jours*/
				INSERT INTO openmrs.patientflags_flag VALUES 
				(7,'Le patient doit venir renflouer ses ARV dans les 30 prochains jours',
				'select distinct a.patient_id FROM isanteplus.alert a WHERE a.id_alert = 7',
				'Le patient doit venir renflouer ses ARV dans les 30 prochains jours',1,
				'org.openmrs.module.patientflags.evaluator.SQLFlagEvaluator',
				NULL,1,'2018-05-28 02:18:18',1,'2018-05-31 13:43:43',0,NULL,NULL,NULL,
				'8c176fcb-9354-43fa-b13c-c293e6f910dc',2);
				/*Le patient n'a plus de médicaments disponibles*/
				INSERT INTO openmrs.patientflags_flag VALUES 
				(8,'Le patient n\'a plus de médicaments disponibles',
				'select distinct a.patient_id FROM isanteplus.alert a WHERE a.id_alert = 8',
				'Le patient n\'a plus de médicaments disponibles',1,
				'org.openmrs.module.patientflags.evaluator.SQLFlagEvaluator',
				NULL,1,'2018-05-28 02:18:18',1,'2018-05-31 13:43:43',0,NULL,NULL,NULL,
				'8c176fcb-9354-43fa-b13c-c293e6f910dc',2);
				 
				 INSERT INTO openmrs.patientflags_flag_tag VALUES (2,2),(3,2),(4,2),(5,2),(7,2),(8,2);
				 INSERT INTO openmrs.patientflags_tag_displaypoint VALUES (2,1);
		
		/*Update global_property to Set where the alert should appear*/			
		UPDATE openmrs.global_property SET property_value = 'false' 
		WHERE property = 'patientflags.patientHeaderDisplay';
		UPDATE openmrs.global_property SET property_value = 'true'
		WHERE property = 'patientflags.patientOverviewDisplay';
		
	END$$
	DELIMITER ;
	
	DELIMITER $$
	DROP PROCEDURE IF EXISTS role_alert$$
	CREATE PROCEDURE role_alert()
	BEGIN
			SET SQL_SAFE_UPDATES = 0;
			SET FOREIGN_KEY_CHECKS = 0;
			
			truncate table openmrs.patientflags_tag_role;
		/*Insert patientflags_tag_role*/
                 INSERT INTO openmrs.patientflags_tag_role (`tag_id`,`role`) VALUES (2,'Anonymous');
					INSERT INTO openmrs.patientflags_tag_role (`tag_id`,`role`) VALUES (2,'Application: Administers System');
					INSERT INTO openmrs.patientflags_tag_role (`tag_id`,`role`) VALUES (2,'Application: Configures Appointment Scheduling');
					INSERT INTO openmrs.patientflags_tag_role (`tag_id`,`role`) VALUES (2,'Application: Configures Forms');
					INSERT INTO openmrs.patientflags_tag_role (`tag_id`,`role`) VALUES (2,'Application: Configures Metadata');
					INSERT INTO openmrs.patientflags_tag_role (`tag_id`,`role`) VALUES (2,'Application: Edits Existing Encounters');
					INSERT INTO openmrs.patientflags_tag_role (`tag_id`,`role`) VALUES (2,'Application: Enters ADT Events');
					INSERT INTO openmrs.patientflags_tag_role (`tag_id`,`role`) VALUES (2,'Application: Enters Vitals');
					INSERT INTO openmrs.patientflags_tag_role (`tag_id`,`role`) VALUES (2,'Application: Has Super User Privileges');
					INSERT INTO openmrs.patientflags_tag_role (`tag_id`,`role`) VALUES (2,'Application: Manages Atlas');
					INSERT INTO openmrs.patientflags_tag_role (`tag_id`,`role`) VALUES (2,'Application: Manages Provider Schedules');
					INSERT INTO openmrs.patientflags_tag_role (`tag_id`,`role`) VALUES (2,'Application: Records Allergies');
					INSERT INTO openmrs.patientflags_tag_role (`tag_id`,`role`) VALUES (2,'Application: Registers Patients');
					INSERT INTO openmrs.patientflags_tag_role (`tag_id`,`role`) VALUES (2,'Application: Requests Appointments');
					INSERT INTO openmrs.patientflags_tag_role (`tag_id`,`role`) VALUES (2,'Application: Schedules And Overbooks Appointments');
					INSERT INTO openmrs.patientflags_tag_role (`tag_id`,`role`) VALUES (2,'Application: Schedules Appointments');
					INSERT INTO openmrs.patientflags_tag_role (`tag_id`,`role`) VALUES (2,'Application: Sees Appointment Schedule');
					INSERT INTO openmrs.patientflags_tag_role (`tag_id`,`role`) VALUES (2,'Application: Uses Capture Vitals App');
					INSERT INTO openmrs.patientflags_tag_role (`tag_id`,`role`) VALUES (2,'Application: Uses Patient Summary');
					INSERT INTO openmrs.patientflags_tag_role (`tag_id`,`role`) VALUES (2,'Application: View Reports');
					INSERT INTO openmrs.patientflags_tag_role (`tag_id`,`role`) VALUES (2,'Application: Writes Clinical Notes');
					INSERT INTO openmrs.patientflags_tag_role (`tag_id`,`role`) VALUES (2,'Authenticated');
					INSERT INTO openmrs.patientflags_tag_role (`tag_id`,`role`) VALUES (2,'Organizational: Doctor');
					INSERT INTO openmrs.patientflags_tag_role (`tag_id`,`role`) VALUES (2,'Organizational: Hospital Administrator');
					INSERT INTO openmrs.patientflags_tag_role (`tag_id`,`role`) VALUES (2,'Organizational: Nurse');
					INSERT INTO openmrs.patientflags_tag_role (`tag_id`,`role`) VALUES (2,'Organizational: Registration Clerk');
					INSERT INTO openmrs.patientflags_tag_role (`tag_id`,`role`) VALUES (2,'Organizational: System Administrator');
					INSERT INTO openmrs.patientflags_tag_role (`tag_id`,`role`) VALUES (2,'Privilege Level: Full');
					INSERT INTO openmrs.patientflags_tag_role (`tag_id`,`role`) VALUES (2,'Provider');
					INSERT INTO openmrs.patientflags_tag_role (`tag_id`,`role`) VALUES (2,'System Developer');
					INSERT INTO openmrs.patientflags_tag_role (`tag_id`,`role`) VALUES (2,'Application: View Reports');
				SET SQL_SAFE_UPDATES = 1;
	SET FOREIGN_KEY_CHECKS = 1;
		
	END$$
	DELIMITER ;
	
	
	DELIMITER $$
		DROP PROCEDURE IF EXISTS calling_arv_status_and_regimen$$
		CREATE PROCEDURE calling_arv_status_and_regimen()
		BEGIN
			call patient_status_arv();
			call isanteplusregimen_dml();
		END$$
	DELIMITER ;
  

DROP EVENT if exists patient_status_arv_event;
	CREATE EVENT if not exists patient_status_arv_event
	ON SCHEDULE EVERY 1 HOUR
	 STARTS now()
		DO
		call calling_arv_status_and_regimen();
		
DROP EVENT if exists isanteplus_patient_alert_event;
	CREATE EVENT if not exists isanteplus_patient_alert_event
	ON SCHEDULE EVERY 1 DAY
	 STARTS now()
		DO
		call isanteplus_patient_alert();
		

 call role_alert();
 DROP EVENT if exists isanteplusregimen_dml_event;
		