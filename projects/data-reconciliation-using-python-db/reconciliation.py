import db_config
import constant
import cx_Oracle
import csv
import datetime
import hashlib
import time
import logging


# Get a connection to Oracle Database.
def get_connection():
    logging.debug("Get connection. Begin")

    try:
        return cx_Oracle.connect(user=db_config.user,
                                 password=db_config.pw,
                                 dsn=db_config.dsn,
                                 encoding=db_config.enc)
    except cx_Oracle.Error:
        logging.exception("Error occurred in Get connection")
        raise


# Create the necessary tables in Oracle database.
def create_tables(p_connection):
    logging.debug("Create tables. Begin")

    try:
        cursor = p_connection.cursor()

        cursor.execute("""begin
                            execute immediate 'drop table src.transactions purge';
                            exception
                              when others then
                                if sqlcode <> -942 then
                                  raise;
                                end if;
                            end;""")
        cursor.execute("""create table src.transactions (
                            id               number(10)   not null
                           ,client_id        number(10)   not null
                           ,account_number   varchar2(20) not null
                           ,transaction_date date         not null
                           ,amount           number(10,4) not null)""")

        cursor.execute("""begin
                            execute immediate 'drop table tgt.source_transactions purge';
                            exception
                              when others then
                                if sqlcode <> -942 then
                                  raise;
                                end if;
                            end;""")
        cursor.execute("""create table tgt.source_transactions (
                            id               number(10)   not null
                           ,adapter_id       number(10)   not null
                           ,client_id        number(10)   not null
                           ,account_number   varchar2(20) not null
                           ,transaction_date date         not null
                           ,amount           number(10,4) not null
                           ,hash_val         raw(20)      not null)""")

        cursor.execute("""begin
                            execute immediate 'drop table tgt.reconciled_transactions purge';
                            exception
                              when others then
                                if sqlcode <> -942 then
                                  raise;
                                end if;
                            end;""")
        cursor.execute("""create table tgt.reconciled_transactions (
                            id               number(10)   not null
                           ,client_id        number(10)   not null
                           ,account_number   varchar2(20) not null
                           ,transaction_date date         not null
                           ,amount           number(10,4) not null)""")

        logging.info("Create tables. Tables created successfully")
    except cx_Oracle.Error:
        logging.exception("Error occurred in Create tables")
        raise
    finally:
        if cursor:
            cursor.close()


# Generate test data for table SRC.TRANSACTIONS.
def generate_test_data(p_connection):
    logging.debug("Generate test data. Begin")

    try:
        cursor = p_connection.cursor()

        cursor.execute("""insert into src.transactions
                          select level as id
                                ,level as client_id
                                ,'40817810099910004' || to_char(100 + level) as account_number
                                ,to_date('2020-01-01', 'yyyy-mm-dd') - 1 + level as transaction_date
                                ,100 * level as amount
                          from   dual
                          connect by level <= 100""")

        cursor.execute("""insert into src.transactions
                          select 100 + level as id
                                ,trunc(dbms_random.value(1,100)) as client_id
                                ,'40817810099910004' || to_char(100 + trunc(dbms_random.value(1,100))) as account_number
                                ,to_date('2020-01-01', 'yyyy-mm-dd') - 1 + trunc(dbms_random.value(1,100)) as transaction_date
                                ,trunc((10000 * trunc(dbms_random.value(1,5))) / trunc(dbms_random.value(5,10))) as amount
                          from   dual
                          connect by level <= 99900""")

        logging.info("Generate test data. Test data generated successfully")
    except cx_Oracle.Error:
        logging.exception("Error occurred in Generate test data")
        raise
    finally:
        if cursor:
            cursor.close()


# Function converts data to a single format and insert records into table TGT.SOURCE_TRANSACTIONS.
def process_data(p_connection, p_source_list, p_adapter_id):
    logging.debug("Process data. adapter_id=" + str(p_adapter_id))
    separator = "|"

    try:
        cursor = p_connection.cursor()

        new_list = [(x[0],
                     p_adapter_id,
                     x[1],
                     x[2],
                     x[3],
                     x[4],
                     hashlib.sha1(separator.join([str(x[1]), x[2], x[3].strftime('%Y-%m-%d'), str(x[4])]).encode('utf-8')).hexdigest()) for x in p_source_list]  # Strings must be encoded before hashing.

        cursor.setinputsizes(None, None, None, 20, None, None, None)  # Predefine memory areas.

        cursor.executemany("""INSERT INTO tgt.source_transactions (id, adapter_id, client_id, account_number, transaction_date, amount, hash_val)
                              VALUES (:1, :2, :3, :4, :5, :6, :7)""", new_list)

        logging.debug("Process data. Data processed successfully")
    except cx_Oracle.Error:
        logging.exception("Error occurred in Process data")
        raise
    finally:
        if cursor:
            cursor.close()


# Adapter for a data source in Oracle Database.
# Each data source has its own adapter.
# The adapter reads the data in batches and calls function PROCESS_DATA.
# Batch inserts will reduce the amount of usable memory.
def adapter_db(p_connection):
    logging.debug("Adapter DB. Begin")

    try:
        cursor = p_connection.cursor()

        cursor.execute("""SELECT *
                          FROM   src.transactions""")

        while True:
            rows = cursor.fetchmany(constant.batch_size)

            if not rows:
                break

            process_data(p_connection, rows, constant.adapter_db_id)

        logging.info("Adapter DB. Data processed successfully")
    except cx_Oracle.Error:
        logging.exception("Error occurred in Adapter DB")
        raise
    finally:
        if cursor:
            cursor.close()


# Adapter for a data source in a CSV file.
def adapter_file(p_connection):
    logging.debug("Adapter file. Begin")

    try:
        with open(constant.path_to_csv_file, 'r') as file:
            reader = csv.reader(file,
                                delimiter=',')

            next(reader, None)  # Skip headers.

            rows = []

            for line in reader:
                rows.append((int(line[0]),
                             int(line[1]),
                             line[2],
                             datetime.datetime.strptime(line[3], '%Y-%m-%d'),
                             float(line[4])))

                if len(rows) % constant.batch_size == 0:
                    process_data(p_connection, rows, constant.adapter_file_id)
                    rows = []

            if rows:  # Process remainder.
                process_data(p_connection, rows, constant.adapter_file_id)

        logging.info("Adapter file. CSV file loaded successfully")
    except csv.Error:
        logging.exception("Error occurred in Adapter file")
        raise


# Processor runs multiple adapters.
def processor(p_connection, *adapters):
    logging.debug("Processor. Begin")

    for idx, source in enumerate(adapters):
        source(p_connection)
        logging.debug("Processor. adapter_id=" + str(idx + 1))

    logging.info("Processor. Completed successfully")


# A key part of the reconciliation process.
# Data is considered reconciled if:
#   1. A match on ID + HASH_VAL.
#   2. A match on ID + CLIENT_ID + ACCOUNT_NUMBER + TRANSACTION_DATE + Tolerance.
# Tolerance is computed by the formula ( ( N2 - N1 ) / N2 ) * 100, where:
#   N2 - is the higher value of N2, N1.
#   N1 - is the smaller value of N2, N1.
def reconciliation(p_connection, p_tolerance=None):
    logging.debug("Reconciliation. Begin")

    try:
        cursor = p_connection.cursor()

        cursor.execute("""insert into tgt.reconciled_transactions
                          select x.*
                          from   (select t1.id
                                        ,t1.client_id
                                        ,t1.account_number
                                        ,t1.transaction_date
                                        ,t1.amount
                                  from   tgt.source_transactions t1
                                  join   tgt.source_transactions t2
                                  on     t1.id = t2.id
                                         and t1.adapter_id != t2.adapter_id
                                         and t1.hash_val = t2.hash_val
                                  where  t1.adapter_id = :p1
                                  union
                                  select t1.id
                                        ,t1.client_id
                                        ,t1.account_number
                                        ,t1.transaction_date
                                        ,t1.amount
                                  from   tgt.source_transactions t1
                                  join   tgt.source_transactions t2
                                  on     t1.id = t2.id
                                         and t1.adapter_id != t2.adapter_id
                                         and t1.hash_val != t2.hash_val
                                         and t1.client_id = t2.client_id
                                         and t1.account_number = t2.account_number
                                         and t1.transaction_date = t2.transaction_date
                                         and ( ( greatest(t1.amount, t2.amount) - least(t1.amount, t2.amount)) / greatest(t1.amount, t2.amount ) ) * 100 <= :p2
                                  where  :p3 is not null
                                          and t1.adapter_id = :p4) x
                          where  not exists (select 1
                                             from   tgt.reconciled_transactions t
                                             where  x.id = t.id)""", p1=constant.adapter_db_id, p2=p_tolerance, p3=p_tolerance, p4=constant.adapter_db_id)

        logging.info("Reconciliation. Completed successfully")
    except cx_Oracle.Error:
        logging.exception("Error occurred in Reconciliation")
        raise
    finally:
        if cursor:
            cursor.close()


# Start the reconciliation process.
def start_reconciliation():
    logging.basicConfig(filename='reconciliation.log',
                        format='%(asctime)s:%(levelname)s:%(message)s',
                        filemode='w',
                        level=logging.DEBUG,
                        encoding='utf-8')
    logging.debug("Start reconciliation. Begin")

    try:
        connection = get_connection()
        logging.debug("Start reconciliation. Successfully connected to Oracle Database")
        connection.autocommit = False

        create_tables(connection)
        generate_test_data(connection)
        processor(connection, adapter_db, adapter_file)

        start = time.time()
        reconciliation(connection, constant.tolerance_value)
        elapsed = (time.time() - start)
        logging.info('Reconciliation completed in ' + str(elapsed) + " seconds")

        connection.commit()
    except cx_Oracle.Error:
        logging.exception("Error occurred in Start reconciliation")
        raise
    finally:
        if connection:
            connection.close()

if __name__ == '__main__':
    start_reconciliation()
