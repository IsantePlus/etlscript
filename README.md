This is the instructions to use the ETL Script for iSantéPlus reports.


How to execute the ETL script for the reports
1)	Clone this repository in your home directory: 

          git clone https://github.com/IsantePlus/etlscript
Note: Ensure you have mysql client installed locally        
2)	After cloning the etlscript repository, open the terminal and execute this command in the project root directory.

`./load.sh <mysql_user> <mysql_password> mysql_host> <mysql_port>` ie

        ./load.sh root pdebezium localhost 3306 


 In case the openmrs database user is not `openmrs_user` ie mysqluser, running the report would give error ie    
 `SELECT command denied to user 'mysqluser'@'172.28.0.3' for table 'patient'`

 login mysql as root user 

        mysql -u root -pdebezium --protocol=tcp
 run the command below as mysql root user 

        GRANT ALL PRIVILEGES ON *.* TO 'mysqluser'@'%' IDENTIFIED BY 'mysqlpw';     



