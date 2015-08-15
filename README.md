Crunching Apache Parquet File with Apache Flink
-------------------------------------------------

This repo includes sample code to setup Flink dataflows to process Parquet files.
The CSV datasets under `resources/` are the Restaurant Score datasets downloaded from
[SF OpenData](https://data.sfgov.org). For more information please see []().

###Generating the Avro Model Classes

If you make any changes to the Avro schema files (`*.avsc`) under `resources/` you should re-generate the model classes

```
./compile_schemas.sh
```

###Converting the CSV Data Files to the Parquet Format:

Below command converts and writes the CSV files under `resources/` to `/tmp/business`, `/tmp/violations`, and `/tmp/inspections` directories in Parquet format.

```
mvn clean package exec:java -Dexec.mainClass="yigitbasi.nezih.ConvertToParquet"
```

###Running the Flink Dataflow:


```
mvn clean compile assembly:single
java -jar target/FlinkParquet-0.1-SNAPSHOT-jar-with-dependencies.jar
```