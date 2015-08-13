CREATE USER 'admin'@'localhost' IDENTIFIED BY 'password';
CREATE DATABASE HR;
GRANT ALL ON HR.* TO 'admin'@'localhost';
FLUSH PRIVILEGES;
