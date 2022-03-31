use fin_data;

drop table if exists transactions;

-- Data preparation.
create table transactions (
  id               integer not null
 ,client_id        integer not null
 ,account_number   varchar(20) not null
 ,transaction_date date not null
 ,amount           decimal(10,4) not null
);

create or replace view generator_10 as
select 0 n
union all
select 1
union all
select 2 
union all
select 3
union all
select 4
union all
select 5
union all
select 6
union all
select 7
union all
select 8
union all
select 9;

insert into transactions
select xx.rn as id
      ,xx.rn as client_id
      ,concat('40817810099910004', convert(100 + xx.rn, char)) as account_number
      ,date_add(str_to_date('2020-01-01', '%Y-%m-%d'), interval (- 1 + xx.rn) day) as transaction_date
      ,100 * xx.rn as amount
from   (select x.n 
              ,@rownum := @rownum + 1 as rn
        from   (select t.n
                from   generator_10 t
                      ,generator_10 t2) x
              ,(select @rownum := 0) r) xx;

insert into transactions
select 100 + xx.rn as id
      ,floor(rand() * (100 - 1) + 1) as client_id
      ,concat('40817810099910004', convert(100 + floor(rand() * (100 - 1) + 1), char)) as account_number
      ,date_add(str_to_date('2020-01-01', '%Y-%m-%d'), interval (- 1 + floor(rand() * (100 - 1) + 1)) day) as transaction_date
      ,floor((10000 * floor(rand() * (5 - 1) + 1)) / floor(rand() * (10 - 5) + 5)) as amount
from   (select x.n 
              ,@rownum := @rownum + 1 as rn
        from   (select t.n
                from   generator_10 t
                      ,generator_10 t2
                      ,generator_10 t3
                      ,generator_10 t4
                      ,generator_10 t5
                      ,generator_10 t6) x
              ,(select @rownum := 0) r) xx
where  xx.rn <= 99900;

commit;
