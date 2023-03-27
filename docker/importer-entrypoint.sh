#!/bin/sh

cd /app/docker

echo "wait for mysql"
./mysql-wait.sh

echo "create mysql user"
./mysql-prep.sh

echo "run importer"
./run-importer.sh

echo "done..."
# uncomment below line to keep container open after the importer run
#/bin/sh
