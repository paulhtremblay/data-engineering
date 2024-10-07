set -e
#psql --host=localhost -U postgres -a -f create_database.sql 
#psql --host=localhost -U postgres -a -f create_users.sql 
psql --host=localhost -U postgres --dbname=test -a -f create_tables.sql 

