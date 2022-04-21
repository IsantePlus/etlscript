This is the instructions to use the ETL Script for iSantéPlus reports.


How to execute the ETL script for the reports
1)	Clone this repository in your home directory: 

          git clone https://github.com/IsantePlus/etlscript
2)	After cloning the etlscript repository, open the terminal and execute this command in the project root directory.
`./load.sh <mysql_user> <mysql_password> mysql_host> <mysql_port>` ie

        ./load.sh root Admin123 localhost 3306 

 Note: Ensure you have mysql client installed locally  .

 There are cases where running the report would give error like `user denied acces to table ...`
 run the command below as root user 

        GRANT ALL PRIVILEGES ON *.* TO 'mysqluser'@'%' IDENTIFIED BY 'mysqlpw';     



