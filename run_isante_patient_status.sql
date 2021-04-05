
DELIMITER $$
	DROP PROCEDURE IF EXISTS isanteplus.isantepatientstatus$$
	CREATE PROCEDURE isanteplus.isantepatientstatus()
		BEGIN
		/*Adding patient_status_arv iSante to iSantePlus*/
		INSERT INTO isanteplus.patient_status_arv(patient_id,id_status,start_date,last_updated_date,
		date_started_status)
		SELECT p.patient_id, pst.patientStatus as id_status, pst.insertDate
		AS start_date, pst.insertDate, pst.insertDate
		FROM isanteplus.patient p, itech.patientStatusTemp pst
		WHERE p.isante_id = pst.patientID
		AND DATE(pst.insertDate) >= '2019-09-01'
		AND (pst.patientStatus IS NOT NULL AND pst.patientStatus > 0)
		group by p.patient_id, pst.insertDate
		on duplicate key update
		last_updated_date = values(last_updated_date);
	END$$
	DELIMITER ;
	
	call isanteplus.isantepatientstatus();


DELETE FROM openmrs.role WHERE role = 'Application: View reports' AND uuid = 'b12a19bb-7f36-4176-bd91-c503cf7ce80b';
DELETE FROM openmrs.privilege WHERE privilege = 'App: reportingui.reports' AND uuid = '3a0803b1-72a9-4b15-850a-4cbcdedd8e4f';
DELETE FROM openmrs.role_privilege WHERE role = 'Application: View reports' AND privilege = 'App: reportingui.reports';

INSERT INTO openmrs.role (role, description, uuid)
VALUES ('Application: View reports','Able to view reports','b12a19bb-7f36-4176-bd91-c503cf7ce80b');

INSERT INTO openmrs.privilege (privilege, description, uuid)
VALUES ('App: reportingui.reports', 'Able to access reports', '3a0803b1-72a9-4b15-850a-4cbcdedd8e4f');

INSERT INTO openmrs.role_privilege (role, privilege)
VALUES ('Application: View reports', 'App: reportingui.reports');