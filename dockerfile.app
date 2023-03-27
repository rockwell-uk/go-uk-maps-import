FROM golang:1.17.12-alpine

RUN apk --update add gcc g++
RUN apk add geos geos-dev
RUN apk add sqlite
RUN apk add libspatialite
RUN apk add mysql-client
RUN apk add git

RUN ln -s /usr/lib/mod_spatialite.so.7 /usr/lib/mod_spatialite.so

ENTRYPOINT /app/docker/importer-entrypoint.sh
