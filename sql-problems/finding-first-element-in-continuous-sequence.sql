-- Oracle version:
-- Oracle Database 19c Enterprise Edition Release 19.0.0.0.0 - Production

-- Data preparation.
create table tab (id number);

insert into tab values (1);
insert into tab values (2);
insert into tab values (3);
insert into tab values (4);
insert into tab values (6);
insert into tab values (7);
insert into tab values (8);
insert into tab values (10);

commit;

-- Method 1.
-- Simple but a rather elegant solution, in my opinion.

select t.id as first_element
from   tab t
minus
select tt.id + 1
from   tab tt
order  by 1;
-- Result:
-- FIRST_ELEMENT
-- -------------
--             1
--             6
--            10

-- Method 2.
-- The same as the Tabibitosan method mentioned in file FINDING-GAPS-IN-DATA.sql

select min(x.id) as first_element
from   (select t.id
              ,t.id - row_number() over (order by t.id) as gr
        from   tab t) x
group  by x.gr
order  by 1;
-- Result:
-- FIRST_ELEMENT
-- -------------
--             1
--             6
--            10

-- Clean up.
drop table tab purge;
