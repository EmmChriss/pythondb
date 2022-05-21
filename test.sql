create_database asd
use_database asd

create_table a
create_column a a1 int primary-key
create_column a a2 string none
create_column a a3 int unique
create_column a a4 string index

create_table b
create_column b b1 int primary-key
create_column b b2 string foreign-key=a.a2
create_column b b2 int foreign-key=a.a3
create_column b b3 int unique

INSERT

insert into b values 1#1#1

insert into a values 1#asd1#1#a
insert into b values 1#1#1

select * from a
select * from b

insert into a values 2#asd2#2#b
insert into a values 3#asd3#3#c
insert into a values 4#asd4#4#d
insert into a values 5#asd5#5#d

SELECT: show all columns
select * from a
select * from b

SELECT: combine index and normal search
select * from a where a2=asd3 a3=3

SELECT: test uq index
select * from a where a3=3

SELECT: test uq index
select * from b where b3=1

SELECT: test fk index
select * from b where b2=1

SELECT: test nq index
select * from a where a4=d

SELECT: test pk reconstruction
select * from a where a3=1 a1=1 a2=asd1

SELECT: simple join
select * from a,b

SELECT: join with where
select * from a,b a1=&b1

SELECT: join with where
select * from a,b a1>&b1

SELECT: join with aliases
select c.a1,d.b1 from a:c,b:d

SELECT: self-join
select * from b:c,b:d where c.b1=&d.b1

SELECT: reference
select b.b1,a.a1 from a,b where a.a1=1

SELECT: self-reference
select b.b1,b.b2 from b where b.b1=&b.b2

DELETE: delete a couple columns
delete a where a2=asd2
select * from a

PRINT EVERYTHING
select * from a
select * from b

drop_table a

drop_table b
drop_table a
drop_database asd
