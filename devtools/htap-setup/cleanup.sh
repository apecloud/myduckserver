#!/bin/bash

# docker-compose down
docker-compose down

# rm data
rm -rf htap

# rm maxscale
rm -rf maxscale