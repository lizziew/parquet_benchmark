#!/usr/bin/env bash

for i in `seq 1 10`;
do
  echo $i
  mvn clean 
  mvn install dependency:copy-dependencies
  java -server -cp "target/benchmark-1.0-SNAPSHOT.jar:target/dependency/*" com.ewei.parquet.App
done 
