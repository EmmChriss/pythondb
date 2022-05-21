create_database allatok
use_database allatok

create_table hazi
create_column hazi id int primary-key
create_column hazi faj string none
create_column hazi age int index

create_table vad
create_column vad id int primary-key
create_column vad mutat int foreign-key=hazi.id

insert into hazi values 1#kuty#3
insert into hazi values 2#macs#4
insert into hazi values 3#kecs#5
insert into hazi values 4#fecs#6

insert into vad values 1#1
insert into vad values 2#3

select * from hazi:a,vad:b where a.id=&b.mutat
select * from hazi age>4 faj=kecs

drop_table vad
drop_table hazi

drop_database allatok

