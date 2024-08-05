DROP TABLE IF EXISTS students;
CREATE TABLE students (
	first_name  varchar(40) NOT NULL,
	last_name  varchar(40) NOT NULL,
	age        integer NOT NULL,
	PRIMARY KEY(first_name, last_name)

);

