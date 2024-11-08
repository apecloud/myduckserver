#!/bin/bash

# docker-compose down
docker-compose down

# rm myduck-mysql
rm -rf myduck-mysql

# rm myduckserver
rm -rf myduckserver

# rm maxscale
rm -rf maxscale