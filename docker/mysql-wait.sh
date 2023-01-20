#!/bin/sh

while ! nc -z -v -w30 mysql 3306; do
  >&2 echo "MySQL is unavailable - sleeping"
  sleep 1
done