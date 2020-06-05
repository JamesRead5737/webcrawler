# webcrawler
This program requires a database named crawl. Run as root user in mysql:
CREATE DATABASE `crawl` /*!40100 DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci */ /*!80016 DEFAULT ENCRYPTION='N' */;

This program also requires a user crawler. Run as root user in mysql:
CREATE USER crawler@localhost IDENTIFIED BY '1q2w3e4r'
GRANT ALL ON crawl.* to crawler@localhost;

Then run the create.mysql script like so:
mysql -u crawler -p < create.mysql

Program should now be ready to run.
Install program with command:
gcc crawler.c -g -I/usr/include/mysql -lssl -lcurl -lmysqlclient

Path to mysql may differ for your system. Use mysql_config --cflags and mysql_config --libs to discover paths for your system.
