/*package com.ewei.parquet;

import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

import static org.apache.parquet.column.ParquetProperties.WriterVersion.PARQUET_2_0;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.SparkSession.Builder;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Days;
import org.joda.time.MutableDateTime;

public class App {
    private static final String M1 = "/mnt/minwei/";
    private static final String M2 = "/Users/elizabethwei/code/";

    private static final String OUTPUT_PATH = M1 + "parquet_benchmark/";
    private static final String CSV_PATH = M1 + "tpch-dbgen/";
    private static final String PARQUET_PATH = M1 + "parquet_benchmark/src/main/java/com/ewei/parquet/";

    private static BufferedWriter outputWriter;

    private static ArrayList<String> relations;

    private static void writeToParquet(String relation, List<GenericData.Record> recordsToWrite, Schema schema, Path fileToWrite, String compressionSchemeString, String dictionaryOptionString) throws IOException {
        CompressionCodecName scheme = CompressionCodecName.UNCOMPRESSED;
        if (compressionSchemeString.toLowerCase().equals("snappy")) {
            scheme = CompressionCodecName.SNAPPY;
        } else if (compressionSchemeString.toLowerCase().equals("gzip")) {
            scheme = CompressionCodecName.GZIP;
        }

        boolean enableDictionary = dictionaryOptionString.toLowerCase().equals("true");

        ParquetWriter<GenericData.Record> writer = AvroParquetWriter
                .<GenericData.Record>builder(fileToWrite)
                .withSchema(schema)
                .withWriterVersion(PARQUET_2_0)
                .withDictionaryEncoding(enableDictionary)
                .withConf(new Configuration())
                .withCompressionCodec(scheme)
                .build();

        final long startTime = System.currentTimeMillis();
        for (GenericData.Record record : recordsToWrite) {
            writer.write(record);
        }
        final long endTime = System.currentTimeMillis();
        outputWriter.write("\n    Total execution time to write " + relation + " to Parquet: " + (endTime - startTime) + " ms");

        writer.close();
    }

    private static void queryParquet(String queryName, String query, SparkSession spark) throws IOException {
        // Load in parquet files
        for (String relation : relations) {
            try {
                BufferedReader br = new BufferedReader(new FileReader(PARQUET_PATH + relation + "_schema.txt"));

                StringBuilder schemaString = new StringBuilder();

                String line;
                while ((line = br.readLine()) != null) {
                    String[] input = line.replaceAll(" ", "").split(",");
                    schemaString.append(input[0] + " ");
                    schemaString.append(input[1].toUpperCase());
                    schemaString.append(",");
                }

                schemaString.deleteCharAt(schemaString.length() - 1);

                Dataset<Row> dataframe = spark.read().schema(schemaString.toString()).parquet(PARQUET_PATH + relation + ".parquet");
                dataframe.createOrReplaceTempView(relation);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        long startTime, endTime;

        System.out.println("Querying " + queryName);
        System.out.println(query + "\n\n");

        // Warm up
        for (int i = 0; i < 3; i++) {
            startTime = System.currentTimeMillis();
            spark.sql(query);
            endTime = System.currentTimeMillis();
            outputWriter.write("\n        Time: " + i + ": " + (endTime - startTime));
        }

        // Actually execute query
        long totalTime = 0;
        for (int i = 0; i < 1000; i++) {
            startTime = System.currentTimeMillis();
            spark.sql(query);
            endTime = System.currentTimeMillis();
            totalTime += (endTime - startTime);
            outputWriter.write("\n        Time: " + i + ": " + (endTime - startTime));
        }

        // Calculate and output average time
        long avgTime = totalTime / 10;
        outputWriter.write(("\n    Average execution time to execute query " + queryName + ": " + avgTime + "\n\n"));
    }

    private static void uncompressedBenchmark() {
        long sum = 0;

        // Read in CSV file
        try {
            for (int i = 0; i < 15; i++) {
                final long startTime = System.currentTimeMillis();
                Reader in = new FileReader(CSV_PATH);
                Iterable<CSVRecord> records = CSVFormat.DEFAULT.withDelimiter('|').parse(in);
                for (CSVRecord record : records) {
                    sum += Integer.parseInt(record.get(0));
                }
                final long endTime = System.currentTimeMillis();
                System.out.println("Sum: " + sum + " in " + (endTime - startTime) + " ms");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws IOException {
        String compressionSchemeString = args[0];
        String dictionaryOptionString = args[1];
        // Set up output file to write benchmarks too
        try {
            outputWriter = new BufferedWriter(new FileWriter(OUTPUT_PATH + "output" + compressionSchemeString + "_" + dictionaryOptionString + ".txt", true));
        } catch (IOException e) {
            System.out.println("Failed to create buffered writer: " + e);
        }

        outputWriter.write("\nBenchmarking compression scheme: " + compressionSchemeString + ", dict: " + dictionaryOptionString + "...\n");


        // Relations
        relations = new ArrayList<String>();
        relations.add("lineitem");
        relations.add("customer");
        relations.add("orders");
        relations.add("supplier");
        relations.add("nation");
        relations.add("region");
        relations.add("part");

        // Create schemas
        Schema.Parser parser = new Schema.Parser().setValidate(true);
        HashMap<String, Schema> schemas = new HashMap<String, Schema>();

        for (String relation : relations) {
            try {
                BufferedReader br = new BufferedReader(new FileReader(PARQUET_PATH + relation + "_schema.txt"));

                StringBuilder schemaString = new StringBuilder("{"
                        + "\"namespace\": \"com.ewei.parquet\","
                        + "\"type\": \"record\","
                        + "\"name\": \"" + relation + "\","
                        + "\"fields\": [");

                String line;
                while ((line = br.readLine()) != null) {
                    String[] input = line.replaceAll(" ", "").split(",");
                    schemaString.append(" {\"name\": \"");
                    schemaString.append(input[0]);
                    if (input[1].equals("date")) {
                        schemaString.append("\", \"type\":");
                        schemaString.append("{\"type\": \"int\", \"logicalType\": \"date\"}");
                        schemaString.append("},");
                    } else {
                        schemaString.append("\", \"type\": \"");
                        schemaString.append(input[1]);
                        schemaString.append("\"},");
                    }
                }

                schemaString.deleteCharAt(schemaString.length() - 1);
                schemaString.append(" ]}");

                schemas.put(relation, parser.parse(schemaString.toString()));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        // Data to be written to Parquet
        List<GenericData.Record> sampleData = new ArrayList<GenericData.Record>();

        // Read in CSV files and write out Parquet files
        MutableDateTime epoch = new MutableDateTime(0L, DateTimeZone.UTC);
        SimpleDateFormat localDateFormat = new SimpleDateFormat("yyyy-MM-dd");

        for (String relation : relations) {
            System.out.println("Reading in " + relation + " CSV file");

            try {
                Schema schema = schemas.get(relation);
                List<Schema.Field> fields = schema.getFields();

                String line;
                BufferedReader br = new BufferedReader(new FileReader(CSV_PATH + relation + ".csv"));
                while ((line = br.readLine()) != null) {
                    String[] input = line.split("\\|");

                    GenericData.Record genericRecord = new GenericData.Record(schemas.get(relation));
                    int i = 0;
                    for (Schema.Field field : fields) {
                        Schema.Type type = field.schema().getType();
                        LogicalType logicalType = field.schema().getLogicalType();

                        if (type.equals(Schema.Type.INT)) {
                            if (logicalType == null) {
                                genericRecord.put(field.name(), Integer.parseInt(input[i]));
                            } else {
                                DateTime currentDate = new DateTime(localDateFormat.parse(input[i]));
                                Days days = Days.daysBetween(epoch, currentDate);
                                genericRecord.put(field.name(), days.getDays());
                            }
                        } else if (type.equals(Schema.Type.DOUBLE)) {
                            genericRecord.put(field.name(), Double.parseDouble(input[i]));
                        } else if (type.equals(Schema.Type.STRING)) {
                            genericRecord.put(field.name(), input[i]);
                        }

                        i++;
                    }
                    sampleData.add(genericRecord);
                }
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (ParseException e) {
                e.printStackTrace();
            }

            File f = new File(PARQUET_PATH + relation + ".parquet");
            f.delete();

            System.out.println("Writing Parquet");
            try {
                File inputFile = new File(CSV_PATH + relation + ".csv");
                outputWriter.write("\n    Before file size of " + relation + ": " + inputFile.length() + " bytes");
                writeToParquet(relation, sampleData, schemas.get(relation), new Path(PARQUET_PATH + relation + ".parquet"), compressionSchemeString, dictionaryOptionString);
                File outputFile = new File(PARQUET_PATH + relation + ".parquet");
                outputWriter.write("\n    After file size of " + relation + ": " + outputFile.length() + " bytes\n");
            } catch (IOException e) {
                e.printStackTrace();
            }

            sampleData.clear();
        }

        // Query Parquet
        ArrayList<String> queries = new ArrayList<String>();
        queries.add("q1");
        queries.add("q3");
        queries.add("q4");
        queries.add("q5");
        queries.add("q6");
        queries.add("q13");
        queries.add("q14");
        queries.add("q19");

        SparkSession spark = SparkSession.builder().appName("BenchmarkParquetSum").config("spark.master", "local").getOrCreate();
        for (String queryName : queries) {
            String query = new Scanner(new File(PARQUET_PATH + queryName + ".txt")).useDelimiter("\\Z").next();
            queryParquet(queryName, query, spark);
        }
        spark.stop();

        outputWriter.close();
    }
}*/
