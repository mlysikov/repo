-- Creating tables.
create table film
(
  film_id      number
 ,title        varchar2(100)
 ,description  varchar2(4000)
 ,release_date date
);

create table shop_film
(
  shop_film_id number
 ,shop_id      number
 ,film_id      number
);

create table customer
(
  customer_id number
 ,first_name  varchar2(100)
 ,last_name   varchar2(200)
);

create table rental
(
  rental_id    number
 ,rental_date  date
 ,shop_film_id number
 ,customer_id  number
 ,return_date  date
);

-- Data preparation.
insert into film values (1, 'Toy Story', null, to_date('1995-01-01', 'yyyy-mm-dd'));
insert into film values (2, 'GoldenEye', null, to_date('1995-01-01', 'yyyy-mm-dd'));
insert into film values (3, 'Four Rooms', null, to_date('1995-01-01', 'yyyy-mm-dd'));
insert into film values (4, 'Get Shorty', null, to_date('1995-01-01', 'yyyy-mm-dd'));
insert into film values (5, 'Copycat', null, to_date('1995-01-01', 'yyyy-mm-dd'));
insert into film values (6, 'Shanghai Triad', null, to_date('1995-01-01', 'yyyy-mm-dd'));
insert into film values (7, 'Twelve Monkeys', null, to_date('1995-01-01', 'yyyy-mm-dd'));
insert into film values (8, 'Babe', null, to_date('1995-01-01', 'yyyy-mm-dd'));
insert into film values (9, 'Dead Man Walking', null, to_date('1995-01-01', 'yyyy-mm-dd'));
insert into film values (10, 'Richard III', null, to_date('1996-01-22', 'yyyy-mm-dd'));
insert into film values (11, 'Seven (Se7en)', null, to_date('1995-01-01', 'yyyy-mm-dd'));
insert into film values (12, 'Usual Suspects, The', null, to_date('1995-08-14', 'yyyy-mm-dd'));
insert into film values (13, 'Mighty Aphrodite', null, to_date('1995-10-30', 'yyyy-mm-dd'));
insert into film values (14, 'Postino, Il', null, to_date('1994-01-01', 'yyyy-mm-dd'));
insert into film values (15, 'Mr. Holland''s Opus', null, to_date('1996-01-29', 'yyyy-mm-dd'));
insert into film values (16, 'French Twist (Gazon maudit)', null, to_date('1995-01-01', 'yyyy-mm-dd'));
insert into film values (17, 'From Dusk Till Dawn', null, to_date('1995-02-05', 'yyyy-mm-dd'));
insert into film values (18, 'White Balloon, The', null, to_date('1995-01-01', 'yyyy-mm-dd'));
insert into film values (19, 'Antonia''s Line', null, to_date('1995-01-01', 'yyyy-mm-dd'));
insert into film values (20, 'Angels and Insects', null, to_date('1995-01-01', 'yyyy-mm-dd'));

insert into shop_film values (1, 1, 1);
insert into shop_film values (2, 1, 2);
insert into shop_film values (3, 1, 3);
insert into shop_film values (4, 1, 4);
insert into shop_film values (5, 1, 5);
insert into shop_film values (6, 1, 6);
insert into shop_film values (7, 1, 7);
insert into shop_film values (8, 1, 8);
insert into shop_film values (9, 1, 9);
insert into shop_film values (10, 1, 10);
insert into shop_film values (11, 1, 11);
insert into shop_film values (12, 1, 12);
insert into shop_film values (13, 1, 13);
insert into shop_film values (14, 1, 14);
insert into shop_film values (15, 1, 15);
insert into shop_film values (16, 1, 16);
insert into shop_film values (17, 1, 17);
insert into shop_film values (18, 1, 18);
insert into shop_film values (19, 1, 19);
insert into shop_film values (20, 1, 20);
insert into shop_film values (21, 2, 1);
insert into shop_film values (22, 2, 3);
insert into shop_film values (23, 2, 5);
insert into shop_film values (24, 2, 7);
insert into shop_film values (25, 2, 9);
insert into shop_film values (26, 2, 11);
insert into shop_film values (27, 2, 13);
insert into shop_film values (28, 2, 15);
insert into shop_film values (29, 2, 17);
insert into shop_film values (30, 2, 19);
insert into shop_film values (31, 3, 2);
insert into shop_film values (32, 3, 4);
insert into shop_film values (33, 3, 6);
insert into shop_film values (34, 3, 8);
insert into shop_film values (35, 3, 10);
insert into shop_film values (36, 3, 12);
insert into shop_film values (37, 3, 14);
insert into shop_film values (38, 3, 16);
insert into shop_film values (39, 3, 18);
insert into shop_film values (40, 3, 20);

insert into customer values (1, 'Liam', 'Smith');
insert into customer values (2, 'Noah', 'Johnson');
insert into customer values (3, 'Oliver', 'Williams');
insert into customer values (4, 'Elijah', 'Brown');
insert into customer values (5, 'William', 'Jones');
insert into customer values (6, 'Maria', 'Garcia');
insert into customer values (7, 'James', 'Miller');
insert into customer values (8, 'Benjamin', 'Davis');
insert into customer values (9, 'Lucas', 'Rodriguez');
insert into customer values (10, 'Emma', 'Martinez');

insert into rental values (1, to_date('2020-01-01', 'yyyy-mm-dd'), 1, 1, to_date('2020-01-03', 'yyyy-mm-dd'));
insert into rental values (2, to_date('2020-02-01', 'yyyy-mm-dd'), 2, 1, to_date('2020-02-03', 'yyyy-mm-dd'));
insert into rental values (3, to_date('2020-03-01', 'yyyy-mm-dd'), 3, 1, to_date('2020-03-03', 'yyyy-mm-dd'));

insert into rental values (4, to_date('2020-04-01', 'yyyy-mm-dd'), 21, 2, to_date('2020-04-04', 'yyyy-mm-dd'));
insert into rental values (5, to_date('2020-05-01', 'yyyy-mm-dd'), 22, 2, to_date('2020-05-04', 'yyyy-mm-dd'));
insert into rental values (6, to_date('2020-06-01', 'yyyy-mm-dd'), 23, 2, to_date('2020-06-04', 'yyyy-mm-dd'));
insert into rental values (7, to_date('2020-07-01', 'yyyy-mm-dd'), 24, 2, to_date('2020-07-04', 'yyyy-mm-dd'));
insert into rental values (8, to_date('2020-08-01', 'yyyy-mm-dd'), 25, 2, to_date('2020-08-04', 'yyyy-mm-dd'));
insert into rental values (9, to_date('2021-09-01', 'yyyy-mm-dd'), 26, 2, null);

insert into rental values (10, to_date('2020-04-01', 'yyyy-mm-dd'), 1, 3, to_date('2020-04-05', 'yyyy-mm-dd'));
insert into rental values (11, to_date('2020-05-01', 'yyyy-mm-dd'), 31, 3, to_date('2020-05-05', 'yyyy-mm-dd'));
insert into rental values (12, to_date('2020-06-01', 'yyyy-mm-dd'), 23, 2, to_date('2020-06-05', 'yyyy-mm-dd'));
insert into rental values (13, to_date('2020-07-01', 'yyyy-mm-dd'), 32, 3, to_date('2020-07-05', 'yyyy-mm-dd'));
insert into rental values (14, to_date('2020-08-01', 'yyyy-mm-dd'), 33, 3, to_date('2020-08-05', 'yyyy-mm-dd'));
insert into rental values (15, to_date('2020-09-01', 'yyyy-mm-dd'), 34, 3, to_date('2020-09-05', 'yyyy-mm-dd'));
insert into rental values (16, to_date('2020-10-01', 'yyyy-mm-dd'), 35, 3, to_date('2020-10-05', 'yyyy-mm-dd'));

commit;

-- The first step. Get all films for a given customer.
with q1 as
(
select f.film_id
from   film f
join   shop_film sf
on     f.film_id = sf.film_id
join   rental r
on     sf.shop_film_id = r.shop_film_id
where  r.customer_id = 1
),

-- The second step. Identify all customers (except customer 1) who have watched the same films.
q2 as
(
select r.customer_id
from   shop_film sf
join   rental r
on     sf.shop_film_id = r.shop_film_id
where  sf.film_id in (select q1.film_id
                      from   q1)
       and r.customer_id != 1
)

-- The third and final step. Count the number of rentals by other customers except for films viewed by customer 1.
select sf.film_id
      ,count(r.rental_id) as number_of_rentals
from   shop_film sf
join   rental r
on     sf.shop_film_id = r.shop_film_id
where  r.customer_id in (select q2.customer_id
                         from   q2)
       and sf.film_id not in (select q1.film_id
                              from   q1)
group  by sf.film_id
order  by number_of_rentals DESC
         ,film_id
fetch next 5 rows only; -- Oracle 12+ SQL syntax.

-- Clean up.
drop table film purge;
drop table shop_film purge;
drop table customer purge;
drop table rental purge;
