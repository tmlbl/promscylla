promscylla
==========

This is an experimental Prometheus remote storage integration that uses
ScyllaDB to store metrics.

## Approach

As metrics come in to be written, Promscylla checks against an in-memory cache
whether the schema exists within ScyllaDB to accomodate the write. The tables
are laid out such that the first two elements of the metric name become a
table name, and each tag is added as a column to that table with `ALTER TABLE`.

The first time promscylla is run against an empty ScyllaDB instance, it has to
create _all_ of the table schemas needed as the metrics are coming in. This can
result in a lot of errors and retries, and it can take a while for the whole
system to stabilize. Not sure how to get around it at this time.

## Developing

Run `docker-compose up -d --build` to start a 3-node ScyllaDB cluster, a 
Prometheus server, and a fresh build of promscylla.
