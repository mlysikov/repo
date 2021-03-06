Task:
In Python implement reconciliation of bank customer transactions from two data sources (a database table and a CSV file).

Description:
The first data source is table SRC.TRANSACTIONS in Oracle database.
The second data source is a CSV file.
The reconciled data will be saved into table TGT.RECONCILED_TRANSACTIONS in Oracle database.

The Python application fully manages the ETL process but the reconciliation itself takes place on the database side.

Step-by-step instructions:
1. Run SQL script on Windows host machine (IP address can be different):
sqlplus sys/oracle@192.168.56.101:1521/orcl as sysdba @create-users.sql

2. Run Python program on Windows host machine:
python reconciliation.py

Environment:
- Oracle Database 19c Enterprise Edition Release 19.0.0.0.0 on VirtualBox
- Python 3.10.3

What could be improved:
General:
1. Use classes and objects.
2. Read data (from the database and the CSV file) from the same place in case of an error (for example, lost network connection with a data source).
3. Inserts and reads data using stored procedures on the database side, not write SQL statements in Python.

Performance:
1. Use generators to read data from the database and file.
2. Use multithreading (the threading module) to read data from two adapters.