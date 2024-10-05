```
docker pull postges
docker run -d -p 5432:5432 -e POSTGRES_PASSWORD=mysecretpassword -d postgres
psql  -U postgres -h localhost --port 5432 --password  <mysecretpassword>

create user henry password 'password';
create database test;
GRANT ALL PRIVILEGES ON DATABASE test TO henry;
psql  -U henry -h localhost --port 5432 --password -d test
```



