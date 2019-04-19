create table student (
	id INT NOT NULL PRIMARY KEY,
	name TEXT NOT NULL,
	gender TEXT NOT NULL
);
insert into student (id, name, gender) values (1, 'Gratia', 'F');
insert into student (id, name, gender) values (2, 'Lorain', 'F');
insert into student (id, name, gender) values (3, 'Hally', 'F');
insert into student (id, name, gender) values (4, 'Ryun', 'M');
insert into student (id, name, gender) values (5, 'Catlin', 'F');
insert into student (id, name, gender) values (6, 'Mada', 'F');
insert into student (id, name, gender) values (7, 'Christian', 'M');
insert into student (id, name, gender) values (8, 'Lennard', 'M');
insert into student (id, name, gender) values (9, 'Magnum', 'M');
insert into student (id, name, gender) values (10, 'Renell', 'F');
/*
create table student_sync_admin_0123_3 (
	id INT NOT NULL PRIMARY KEY,
	work_1234_1 INT NOT NULL DEFAULT 0,
	home_2345_2 INT NOT NULL DEFAULT 0
);
insert into student_sync_admin_0123_3(id) select id from student;
*/