#!/bin/sh
cd /app

go run main.go -v -dbhost mysql -datafolder testdata -cleardown

go run main.go -vv -dbhost mysql -datafolder testdata -cleardown -auto

go run main.go -vvv -dbhost mysql -datafolder testdata -cleardown -usefiles