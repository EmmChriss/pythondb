drop_database asd
create_database asd
use_database asd

create_table a
create_column a a1 int primary-key-unique
create_column a a2 string primary-key-not-unique
create_column a a3 int unique
create_column a a4 string index

create_table b
create_column b b1 int primary-key-unique
create_column b b2 string foreign-key=a.a2
create_column b b2 int foreign-key=a.a3
create_column b b3 int unique

INSERT

insert into b values 1#1#1

insert into a values 1#asd1#1#a
insert into b values 1#1#1

insert into a values 2#asd1#1#a

select * from a

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
