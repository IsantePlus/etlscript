
/*SET GLOBAL event_scheduler = 1;
DROP DATABASE if exists isanteplus; 
create database if not exists isanteplus;
ALTER DATABASE isanteplus CHARACTER SET utf8 COLLATE utf8_general_ci;  */

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
	ON SCHEDULE EVERY 1 MINUTE
	 STARTS now()
	  DO
		call call_all_hsis_report_procedures();  