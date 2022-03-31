# Define connection properties.

import os
import getpass

user = os.environ.get("MYSQL_USER", "root")
pw = os.environ.get("MYSQL_PASSWORD")
url = os.environ.get("DB_URL", "jdbc:mysql://localhost:3306/fin_data")
driver = os.environ.get("DB_URL", "com.mysql.jdbc.Driver")
dbtable = os.environ.get("DB_TABLE", "transactions")

if pw is None:
    pw = getpass.getpass("Enter password for %s: " % user)
