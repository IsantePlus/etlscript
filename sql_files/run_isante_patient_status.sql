
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