
-- SET GLOBAL event_scheduler = 1;
-- DROP DATABASE if exists isanteplus; 
-- create database if not exists isanteplus;
-- ALTER DATABASE isanteplus CHARACTER SET utf8 COLLATE utf8_general_ci;  

/*.......... DDL SCRIPTS ............*/
/*create distribution of Vists dataset table */
use isanteplus;
DROP TABLE IF EXISTS vists_distribution;
CREATE TABLE if not exists vists_distribution (
  vists_id INT(11) NOT NULL AUTO_INCREMENT,
  patient_id INT(11) NOT NULL,
  vist_date DATETIME,
  vist_type varchar(10) DEFAULT NULL,
  PRIMARY KEY (vists_id) ,
  CONSTRAINT vist_date_uk UNIQUE (patient_id ,vist_date),
  CONSTRAINT patient_hs_1 FOREIGN KEY(patient_id) REFERENCES openmrs.patient(patient_id)) ;

DROP TABLE IF EXISTS lab_examination;
CREATE TABLE if not exists lab_examination (
  concept_id INT(11) NOT NULL AUTO_INCREMENT,
  patient_id INT(11) NOT NULL,
  value_coded int(11),
  PRIMARY KEY (concept_id) ,
  CONSTRAINT patient_xs_1 FOREIGN KEY(patient_id) REFERENCES openmrs.patient(patient_id)) ;




  /*.......... DML SCRIPTS ............*/
  	DELIMITER $$
	DROP PROCEDURE IF EXISTS hsis_report_procedure$$
	CREATE PROCEDURE hsis_report_procedure()
		BEGIN
		 /*Started DML queries*/
     SET SQL_SAFE_UPDATES = 0;

      INSERT into isanteplus.vists_distribution(patient_id,vist_date)
					SELECT p.patient_id ,v.date_started
                        FROM openmrs.patient p INNER JOIN openmrs.visit v ON p.patient_id = v.patient_id 
                        WHERE v.date_started IS NOT NULL
						  ON DUPLICATE KEY UPDATE
		                   patient_id = p.patient_id ,
						   vist_date = v.date_started ;

     UPDATE isanteplus.vists_distribution ivd ,openmrs.visit ov
	 SET ivd.vist_type  = (CASE 
	                         WHEN ov.date_started = (SELECT MIN(ov2.date_started) FROM openmrs.visit ov2 WHERE ov2.patient_id = ivd.patient_id)
	                               THEN 'NEW'
	                          WHEN ov.date_started > (SELECT MIN(ov2.date_started) FROM openmrs.visit ov2 WHERE ov2.patient_id = ivd.patient_id)               
								   THEN 'SUBSQUENT'
							END ) 
	 WHERE ivd.patient_id = ov.patient_id  
	 AND ivd.vist_date = ov.date_started;	
    
	/* Query for Malaria microscopic test where coded answer is positive*/
	INSERT into isanteplus.lab_examination(patient_id,concept_id,value_coded)
	               SELECT p.patient_id , o.concept_id , o.value_coded
				   FROM openmrs.patient p INNER JOIN openmrs.obs o ON p.patient_id = o.person_id 
				   WHERE concept_id=32 
				   AND value_coded=703 
				      ON DUPLICATE KEY UPDATE
					   patient_id = p.patient_id ,
					   concept_id = o.concept_id ,
					   value_coded = o.value_coded;

    /* Query for Malaria microscopic test where coded answer is negative*/
	INSERT into isanteplus.lab_examination(patient_id,concept_id,value_coded)
	               SELECT p.patient_id , o.concept_id , o.value_coded
				   FROM openmrs.patient p INNER JOIN openmrs.obs o ON p.patient_id = o.person_id 
				   WHERE concept_id=1643 
				   AND value_coded=703
				      ON DUPLICATE KEY UPDATE
					   patient_id = p.patient_id ,
					   concept_id = o.concept_id ,
					   value_coded = o.value_coded;

    /* Query for Malaria microscopic test where answer is undetermined*/
    INSERT into isanteplus.lab_examination(patient_id,concept_id,value_coded)
	               SELECT p.patient_id , o.concept_id , o.value_coded
				   FROM openmrs.patient p INNER JOIN openmrs.obs o ON p.patient_id = o.person_id 
				   WHERE concept_id=32 
				   AND value_coded=1138
				      ON DUPLICATE KEY UPDATE
					   patient_id = p.patient_id ,
					   concept_id = o.concept_id ,
					   value_coded = o.value_coded;

     /* Query for Malaria fast test where answer is undetermined*/
     INSERT into isanteplus.lab_examination(patient_id,concept_id,value_coded)
	               SELECT p.patient_id , o.concept_id , o.value_coded
				   FROM openmrs.patient p INNER JOIN openmrs.obs o ON p.patient_id = o.person_id 
				   WHERE concept_id=1643
				   AND value_coded=1138
				      ON DUPLICATE KEY UPDATE
					   patient_id = p.patient_id ,
					   concept_id = o.concept_id;
					   value_coded = o.value_coded;

     /* Query for the number of people tested for malaria both Malaria fast test and Malaria microscopic test
	  where answer is undetermined*/
	 SELECT COUNT(p.patient_id)
				   FROM openmrs.patient p INNER JOIN openmrs.obs o ON p.patient_id = o.person_id 
				   WHERE o.concept_id IN (32,1643)
				   AND o.value_coded=1138

	/* Query for the number of people tested for malaria both Malaria fast test and Malaria microscopic test
	  where answer is positive*/
	 SELECT COUNT(p.patient_id)
				   FROM openmrs.patient p INNER JOIN openmrs.obs o ON p.patient_id = o.person_id 
				   WHERE o.concept_id IN (32,1643)
				   AND o.value_coded=703			   
						   
  END$$
	DELIMITER ;  

  DELIMITER $$
	DROP PROCEDURE IF EXISTS call_all_hsis_report_procedures$$
	CREATE PROCEDURE call_all_hsis_report_procedures()
	BEGIN
		call hsis_report_procedure();
	END$$
	DELIMITER ;
	
	
	DROP EVENT if exists hsis_report_event;
	CREATE EVENT if not exists hsis_report_event
	ON SCHEDULE EVERY 10 MINUTE
	 STARTS now()
	  DO
		call call_all_hsis_report_procedures();  
