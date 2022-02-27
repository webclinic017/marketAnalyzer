docker exec containerStocks  /usr/bin/mysqldump -u root --password=${PASSWORD} stocks > backup.sql
#cat backup.sql |docker exec -i containerStocks  /usr/bin/mysql -u root --password=${PASSWORD} stocks

