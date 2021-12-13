### RefluxDB timeseries database 

RefluxDB is a small timeseries database with UDP and REST interface. It uses a simple SQL dialect to query the data and recognizes a simple line protocol. Its objective is to have a set of simple timeseries primitives and pre-calculated metrics for large datasets that can be run locally and benefit of fast ssd storage.

It is built in Rust and depends on a small set of libraries to provide local storage:

    - GlueDB
    - Chronos
    - Sled
  
It still in early phases so internal interfaces may change.

#### Building and running
    $ cargo build
    $ cargo run

#### Data interface

##### UDP interface test: 
```$ echo "test,host=server,region=us-east1 value=0.80 1234567890000000000"| nc -u 127.0.0.1 8089```

##### REST Interface test:
```curl -X POST -d "q=SELECT * from test" localhost:8086/query```


#### Design

The main components for the database are contained within the timeseries [persistence manager](src/persistence.rs).

The basic concepts are the following:
    
    - Measurement: Unit of data tied to a time, within a timeseries, annotated with tags
    - Timeseries: A set of immutable measurements that move forward in time
    - Resultset: a slice of data from a timeseries within T(start) and T(end)
    - Tags: json annotations to measurements used to filter and group resultsets


##### Database structure:

Within the ```databases``` (root directory), a new sled database will be created for each timeseries. This database is abstracted by GlueSQL to provide query language and functions support. 

     pros: isolation, parallelism
     cons: disk space, migration


##### Inner schema:
     id -> UUID
     time -> unix timestamp, ordered - measurement time (expanded 11-30-2021)
     created_at -> unix timestamp, ordered, system time (added 11-30-2021)
     name -> value name (added 11-30-2021)
     value -> float (to be float, int, string and bool)
     tags -> key/value tag map

```"CREATE TABLE <timeseries_name> (id UUID, time TIMESTAMP, created_at TIMESTAMP, name TEXT, value FLOAT, tags MAP);",```

    * TODO: Immutable data: measurements can't be changed
    * TODO: separated tag table: "CREATE TABLE <timeseries_name>_tags (id UUID, key TEXT, value TEXT);",
    * TODO: ensure immutability is enforced through measurement id or fingerprint
    * TODO: Pre-calculated stats for each series