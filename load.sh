#!/bin/bash
user=$1;
pass=$2;
host=$3;
port=$4;
mysql --protocol=tcp -h ${host} -P ${port} -u ${user} -p${pass} < ./sql_files/isanteplusreportsddlscript.sql
mysql --protocol=tcp -h ${host} -P ${port} -u ${user} -p${pass} < ./sql_files/isanteplusreportsdmlscript.sql
mysql --protocol=tcp -h ${host} -P ${port} -u ${user} -p${pass} < ./sql_files/drug_lookup_isanteplus.sql
mysql --protocol=tcp -h ${host} -P ${port} -u ${user} -p${pass} < ./sql_files/run_isante_patient_status.sql
mysql --protocol=tcp -h ${host} -P ${port} -u ${user} -p${pass} < ./sql_files/insertion_obs_by_day.sql
mysql --protocol=tcp -h ${host} -P ${port} -u ${user} -p${pass} < ./sql_files/patient_status_arv_dml.sql
mysql --protocol=tcp -h ${host} -P ${port} -u ${user} -p${pass} < ./sql_files/indicators_report.sql
mysql --protocol=tcp -h ${host} -P ${port} -u ${user} -p${pass} < ./sql_files/psychoSocialResume.sql