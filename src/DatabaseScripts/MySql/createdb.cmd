@echo off

if "%1"=="" goto info
if "%2"=="" goto info

mysql -u %1 -p%2 -e "source LOGINUSER.sql"
mysql -u %1 -p%2 HR -e "source TABLES.sql"
mysql -u %1 -p%2 HR -e "source TABLES.sql"
mysql -u %1 -p%2 HR -e "source DATA.sql"
mysql -u %1 -p%2 HR -e "source FUNCTIONS.sql"
mysql -u %1 -p%2 HR -e "source PROCEDURES.sql"
goto skip

:info
echo "createdb adminuser adminpwd"

:skip

