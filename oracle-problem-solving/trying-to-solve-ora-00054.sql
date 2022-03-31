-- Oracle version:
-- Oracle Database 19c Enterprise Edition Release 19.0.0.0.0 - Production

-- Test 1.

-- Session 1.
create table tab_1
(
  tab1_id number primary key
 ,fname   varchar(50)
);

insert into tab_1 values (1, 'John');

commit;

update tab_1
set    fname = 'Liam'
where  tab1_id = 1;

-- Session 2.
create table tab_2
(
  tab2_id number
 ,tab1_id number
 ,dname   varchar2(50)
 ,constraint fk_tab_1 foreign key (tab1_id) references tab_1 (tab1_id)
);
-- Result:
-- ERROR at line 1:
-- ORA-00054: resource busy and acquire with NOWAIT specified or timeout expired

-- Test 2.
-- The only way I came up with is to create TAB_2 without a FK constraint, load data into it,
-- wait until the lock on TAB_1 is cleared, then add the FK constraint on TAB_2. NOVALIDATE
-- will not help us here.

drop table tab_2 purge;
drop table tab_1 purge;

-- Session 1.
create table tab_1
(
  tab1_id number primary key
 ,fname   varchar(50)
);

insert into tab_1 values (1, 'John');

commit;

update tab_1
set    fname = 'Liam'
where  tab1_id = 1;

-- Session 2.
create table tab_2
(
  tab2_id number
 ,tab1_id number
 ,dname   varchar2(50)
);

-- Load data.

alter table tab_2 add constraint fk_tab_1 foreign key (tab1_id) references tab_1 (tab1_id) novalidate;
-- Result:
-- Waiting for Session 1

-- After COMMIT is issued in Session 1:
-- Table altered.

alter table tab_2 modify constraint fk_tab_1 validate;
-- Table altered.

-- Clean up.
drop table tab_2 purge;
drop table tab_1 purge;
