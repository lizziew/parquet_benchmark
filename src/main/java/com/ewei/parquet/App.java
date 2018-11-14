package com.ewei.parquet;

import java.io.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


public class App {
    private static final String M1 = "/mnt/minwei/";
    private static final String M2 = "/Users/elizabethwei/code/";

    private static final String CSV_PATH = M2 + "tpch-dbgen/";
    private static final String PARQUET_PATH = M2 + "parquet_benchmark/src/main/java/com/ewei/parquet/";

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

            Dataset<Row> csvInput = spark.read().option("header", "false").option("delimiter", "|").schema(schemaString.toString()).csv(CSV_PATH + "lineitem.csv");
            csvInput.show();
            csvInput.write().parquet(PARQUET_PATH + "lineitem.parquet");

            Dataset<Row> parquetOutput = spark.read().schema(schemaString.toString()).parquet(PARQUET_PATH + relation + ".parquet");
            parquetOutput.show();
        } catch (IOException e) {
            e.printStackTrace();
        }

        spark.stop();
    }
}
