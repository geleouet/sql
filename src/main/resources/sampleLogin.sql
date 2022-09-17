create table login (l varchar(100), p varchar(100));
		insert into login (l, p) values ('admin', 'a');
        insert into login (l, p) values ('emma', 'e');
		select * from login;

	select * from login where l='admin' and p= 'z' or 1=1 and l='admin';