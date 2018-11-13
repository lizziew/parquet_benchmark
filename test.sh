mvn clean
mvn install dependency:copy-dependencies
java -server -cp "target/benchmark-1.0-SNAPSHOT.jar:target/dependency/*" com.ewei.parquet.App UNCOMPRESSED TRUE > out 2>&1
