#!/usr/bin/env bash

for i in UNCOMPRESSED SNAPPY GZIP
do
  for j in TRUE FALSE
  do 
    mvn clean 
    mvn install dependency:copy-dependencies
    java -server -cp "target/benchmark-1.0-SNAPSHOT.jar:target/dependency/*" com.ewei.parquet.App $i $j
  done
done 
