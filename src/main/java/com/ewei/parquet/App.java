package com.ewei.parquet;

import java.io.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


public class App {
    private static final String M1 = "/mnt/minwei/";
    private static final String CSV_PATH = M1 + "tpch-dbgen/";
    private static final String PARQUET_PATH = M1 + "parquet_benchmark/src/main/java/com/ewei/parquet/";

    public static void main(String[] args) {
        String compressionSchemeString = args[0];
        String dictionaryOptionString = args[1];

        System.out.println("\nBenchmarking compression scheme: " + compressionSchemeString + ", dict: " + dictionaryOptionString + "...\n");

        SparkSession spark = SparkSession.builder().appName("Small").config("spark.master", "local").getOrCreate();

        String relation = "lineitem";
        try {
            BufferedReader br = new BufferedReader(new FileReader(PARQUET_PATH + relation + "_schema.txt"));

            StringBuilder schemaString = new StringBuilder();

            String line;
            while ((line = br.readLine()) != null) {
                String[] input = line.replaceAll(" ", "").split(",");
                schemaString.append(input[0]).append(" ");
                schemaString.append(input[1].toUpperCase());
                schemaString.append(",");
            }

            schemaString.deleteCharAt(schemaString.length() - 1);

            Dataset<Row> row = spark.read().option("header", "false").schema(schemaString.toString()).csv(CSV_PATH + "lineitem.csv");
        } catch (IOException e) {
            e.printStackTrace();
        }


        // Dataset<Row> dataframe = spark.read().schema(schemaString.toString()).parquet(PARQUET_PATH + relation + ".parquet");
        // dataframe.createOrReplaceTempView(relation);
        Dataset<Row> sqlDF = spark.read().parquet("/mnt/minwei/hmm.parquet");
        sqlDF.show();

        spark.stop();
    }
}
