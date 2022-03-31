Task:
In PySpark implement reconciliation of bank customer transactions from two data sources (a database table and a CSV file).

Description:
The first data source is table FIN_DATA.TRANSACTIONS in MySQL database.
The second data source is a CSV file.
The reconciled data will be saved into HDFS as a Parquet file.

Environment:
- Hortonworks Sandbox HDP 2.6.5 on VirtualBox
- MySQL 5.7.22
- Spark 2.3.0
- PySpark

Step-by-step instructions:
1. Copy the necessary files from Windows to Sandbox:
pscp -P 2222 C:\Users\mlysikov\Downloads\repo\projects\data-reconciliation-using-spark-db\transactions.csv  C:\Users\mlysikov\Downloads\repo\projects\data-reconciliation-using-spark-db\create-table.sql C:\Users\mlysikov\Downloads\repo\projects\data-reconciliation-using-spark-db\constant.py C:\Users\mlysikov\Downloads\repo\projects\data-reconciliation-using-spark-db\db_config.py C:\Users\mlysikov\Downloads\repo\projects\data-reconciliation-using-spark-db\reconciliation.py maria_dev@127.0.0.1:/home/maria_dev/

2. Connect to MySQL, create database FIN_DATA, run a prepared script:
mysql -u root -p
password: hortonworks1
create database fin_data;
set names 'utf8';
set character set utf8;
source /home/maria_dev/create-table.sql

3. Create additional directories in HDFS:
hadoop fs -mkdir input
hadoop fs -mkdir output

4. Copy the CSV file to HDFS:
hadoop fs -copyFromLocal /home/maria_dev/transactions.csv /user/maria_dev/input/

5. Run the PySpark script:
spark-submit reconciliation.py

6. Check the Parquet file:
hadoop fs -ls /user/maria_dev/output

What could be improved:
General:
1. Use classes and objects.

Performance:
1. Avoid UDFs (already done in the script).
2. Read data from two sources in parallel.