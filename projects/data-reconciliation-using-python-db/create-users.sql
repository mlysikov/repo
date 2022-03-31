-- Create users.
-- Run code as user SYS.

-- SRC - source database. We will consider this schema as a source database containing transactional table TRANSACTIONS.
declare
  e_user_does_not_exist exception;
  pragma exception_init(e_user_does_not_exist, -1918);
begin
  execute immediate 'drop user src cascade';
exception
  when e_user_does_not_exist then
    null;
end;
/

create user src identified by "src"
default tablespace users
temporary tablespace temp
quota unlimited on users;

grant create session
     ,create view
     ,create sequence
     ,create procedure
     ,create table
     ,create trigger
     ,create type
     ,create materialized view
to src;

-- TGT - target database. We will consider this schema as a target database containing reconciled data in table RECONCILED_TRANSACTIONS.
declare
  e_user_does_not_exist exception;
  pragma exception_init(e_user_does_not_exist, -1918);
begin
  execute immediate 'drop user tgt cascade';
exception
  when e_user_does_not_exist then
    null;
end;
/

create user tgt identified by "tgt"
default tablespace users
temporary tablespace temp
quota unlimited on users;

grant create session
     ,create view
     ,create sequence
     ,create procedure
     ,create table
     ,create trigger
     ,create type
     ,create materialized view
to tgt;

-- PYTHON_USER - the user under which all operations in Python will be performed.
declare
  e_user_does_not_exist exception;
  pragma exception_init(e_user_does_not_exist, -1918);
begin
  execute immediate 'drop user python_user cascade';
exception
  when e_user_does_not_exist then
    null;
end;
/

create user python_user identified by "python_user"
default tablespace users
temporary tablespace temp
quota unlimited on users;

grant create session
     ,create view
     ,create sequence
     ,create procedure
     ,create trigger
     ,create type
     ,create materialized view
to python_user;

-- It's not a good practice to grant system privileges to an API user.
-- It is better to create procedures in the destination schemas that will perform the required DDL/DML operations.
grant create any table
     ,drop any table
     ,insert any table
     ,read any table
to python_user;
