

create table client (
    id INT,
    value INT
    );
    
    
insert into client values (1, 2);
insert into client values (2, 2);
insert into client values (3, 2);
insert into client values (1, 1);
insert into client values (2, 1);

explain select value from client where id = 3 and value =2;


    select value + 10 as value, value as value from client;

select * from client where value = 1 and id = 1 or id = 3 and value = 2;

select toto from client;

create table color (
    id INT,
    color varchar(10)
    );
    
insert into color values (1, 'Blue');    
insert into color values (2, 'Red');

create table city (
    id INT,
    name varchar(10)
    );

insert into city values (0, 'Marseille');    
insert into city values (1, 'Paris');    
insert into city values (2, 'London');
insert into city values (21, 'Lyon');


explain select client.value, color.color
from client, color
;

explain analyse select client.id, client.value, color.color
from client
inner join color on client.id = color.id
inner join city on client.id = city.id
where city.id < 10;

explain select client.id, client.value, color.color, city.name
from client
inner join color on client.id = color.id
inner join city on client.id = city.id + color.id;

select client.id, client.value, c3.id as color_id, c3.color
from client inner join color c3 on client.id = c3.id;



create table relation (
    id INT,
    parent INT,
    name VARCHAR(10)
    );
    
insert into relation values(1, 0, 'Bob');
insert into relation values(2, 1, 'Alice');
insert into relation values(3, 2, 'John');


select * from relation;

select relation.name, parent.name as parent_name
from relation inner join relation parent on relation.parent = parent.id;

set enable_hashjoin=off;
set enable_mergejoin=off;

explain analyse select relation.name, parent.name as parent_name
from relation inner join relation parent on relation.parent = parent.id;
