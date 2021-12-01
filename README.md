### RefluxDB timeseries database 

    - Rust
    - GlueDB
    - Chronos
    - Sled

This is a generic UDP and REST server for timeseries data. It is able to understand the most common protocols but the query language is severely limited. Treat it as a simple local timeseries database.

UDP interface test: 
$ echo "test,host=server,region=us-east1 value=0.80 1234567890000000000"| nc -u 127.0.0.1 8089

REST Interface test:
curl -X POST -d "q=SELECT * from test" localhost:8086/query