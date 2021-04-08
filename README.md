This is the instructions to use the ETL Script for iSantéPlus reports.


How to execute the ETL script for the reports
1)	Clone this repository in your home directory: https://github.com/IsantePlus/etlscript
Ex: git clone https://github.com/IsantePlus/etlscript
2)	After cloning the etlscript repository, open the terminal and execute these commands
a)	mysql -uroot -pmysql_password < etlscript/isanteplusreportsddlscript.sql
b)	mysql -uroot -pmysql_password isanteplus < etlscript/isanteplusreportsdmlscript.sql
c)	mysql -uroot -pmysql_password isanteplus < etlscript/drug_lookup_isanteplus.sql
d)	mysql -uroot -pmysql_password isanteplus < etlscript/run_isante_patient_status.sql
e)	mysql -uroot -pmysql_password isanteplus < etlscript/insertion_obs_by_day.sql
f)	mysql -uroot -pmysql_password isanteplus < etlscript/patient_status_arv_dml.sql


