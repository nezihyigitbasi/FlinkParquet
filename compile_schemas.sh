#!/usr/bin/env bash
java -jar tools/avro-tools-1.7.7.jar compile schema src/main/resources/*.avsc src/main/java
