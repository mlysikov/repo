-- Foreword:
-- You can easily modify these methods to display ranges of gaps.

-- Oracle version:
-- Oracle Database 19c Enterprise Edition Release 19.0.0.0.0 - Production

-- Data preparation.
create table tab (date_created date);

insert into tab values (to_date('2020-01-01', 'yyyy-mm-dd'));
insert into tab values (to_date('2020-01-02', 'yyyy-mm-dd'));
insert into tab values (to_date('2020-01-03', 'yyyy-mm-dd'));
insert into tab values (to_date('2020-01-04', 'yyyy-mm-dd'));
insert into tab values (to_date('2020-01-07', 'yyyy-mm-dd'));
insert into tab values (to_date('2020-01-08', 'yyyy-mm-dd'));
insert into tab values (to_date('2020-01-09', 'yyyy-mm-dd'));
insert into tab values (to_date('2020-01-11', 'yyyy-mm-dd'));
insert into tab values (to_date('2020-01-12', 'yyyy-mm-dd'));
insert into tab values (to_date('2020-01-14', 'yyyy-mm-dd'));

commit;

-- Method 1.
-- The method I invented when I faced a gap detection problem. I called this
-- method the running difference.

with q1 as
(
select t.*
      ,lag(date_created, 1, to_date('1900-01-01', 'yyyy-mm-dd')) over (order by t.date_created) as prev_date
from   tab t
),

q2 as
(
select q1.*
      ,case
         when (q1.date_created - q1.prev_date) > 1 then 1
         else 0
       end as diff
from   q1
),

q3 as
(
select q2.*
      ,sum(q2.diff) over (order by q2.date_created) as running_diff
from   q2
)

select min(q3.date_created) as date_from
      ,max(q3.date_created) as date_to
from   q3
group  by q3.running_diff
order  by 1;
-- Result:
-- DATE_FROM DATE_TO
-- --------- ---------
-- 01-JAN-20 04-JAN-20
-- 07-JAN-20 09-JAN-20
-- 11-JAN-20 12-JAN-20
-- 14-JAN-20 14-JAN-20

-- Method 2.
-- The Tabibitosan method. I met with varieties of this method but did not know
-- that it is called Tabibitosan. :)

select min(x.date_created) as date_from
      ,max(x.date_created) as date_to
from   (select t.date_created
              ,t.date_created - row_number() over (order by t.date_created) as gr
        from   tab t) x
group  by x.gr
order  by 1;
-- Result:
-- DATE_FROM DATE_TO
-- --------- ---------
-- 01-JAN-20 04-JAN-20
-- 07-JAN-20 09-JAN-20
-- 11-JAN-20 12-JAN-20
-- 14-JAN-20 14-JAN-20

-- Clean up.
drop table tab purge;
