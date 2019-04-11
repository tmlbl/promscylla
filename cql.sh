#!/bin/bash

ID=$(docker ps --filter="label=kind=db" -q | tail -1)
docker exec -it $ID cqlsh

