docker run -v /media/$(whoami)/$DISKNAME/$DIRNAME/databaseStocks:/var/lib/mysql --user 1000 \
    --name containerStocks \
    --publish 33062:3306 \
    -e  MYSQL_ROOT_PASSWORD=$PASSWORD \
     mysql:5.7
~                                                                               
~                                                                               
~                      
