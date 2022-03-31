# Define connection properties.

import os
import getpass

user = os.environ.get("PYTHON_USER", "python_user")
pw = os.environ.get("PYTHON_PASSWORD")
dsn = os.environ.get("DB_CONNECT_STRING", "192.168.56.101:1521/orcl")
enc = os.environ.get("DB_ENCODING", "UTF-8")

if pw is None:
    pw = getpass.getpass("Enter password for %s: " % user)
