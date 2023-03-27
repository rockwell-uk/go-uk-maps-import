#!/bin/sh
if ! command -v mysql &> /dev/null
then
    echo "mysql client could not be found"
else
    mysql -h mysql -u root < sql/create-user.sql
fi
