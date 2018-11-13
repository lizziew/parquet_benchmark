package com.ewei.parquet;

import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

import static org.apache.parquet.column.ParquetProperties.WriterVersion.PARQUET_2_0;
import org.apache.parquet.column.values.ValuesReader; 

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.SparkSession.Builder;
import org.apache.spark.sql.types.StructType;

public class App {
    private static final String M1 = "/mnt/minwei/";
    private static final String M2 = "/Users/elizabethwei/code/";

    private static final String OUTPUT_PATH = M1 + "parquet_benchmark/";
    private static final String CSV_PATH = M1 + "parquet_benchmark/";
    private static final String PARQUET_PATH = M1 + "parquet_benchmark/src/main/java/com/ewei/parquet/";

    private static ArrayList<String> relations;
    private static HashMap<String, Schema> schemas;

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

        for (GenericData.Record record : recordsToWrite) {
            writer.write(record);
        }

        writer.close();
    }

    private static void queryParquet(SparkSession spark) throws IOException {
        // Load in parquet files
        /*for (String relation : relations) {
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
        }*/

        Dataset<Row> sqlDF =
                spark.read().parquet("/mnt/minwei/hmm.parquet");
	// Dataset<Row> people = spark.read().json("/mnt/minwei/people.json");
        sqlDF.show();
    }

    public static void main(String[] args) throws IOException {
        String compressionSchemeString = args[0];
        String dictionaryOptionString = args[1];

        System.out.println("\nBenchmarking compression scheme: " + compressionSchemeString + ", dict: " + dictionaryOptionString + "...\n");

        // Relations
        relations = new ArrayList<String>();
        relations.add("test");

        // Create schemas
        Schema.Parser parser = new Schema.Parser().setValidate(true);
        schemas = new HashMap<String, Schema>();

        for (String relation : relations) {
            try {
                BufferedReader br = new BufferedReader(new FileReader(CSV_PATH + relation + "_schema.txt"));

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
            }

            File f = new File(PARQUET_PATH + relation + ".parquet");
            f.delete();

            System.out.println("Writing Parquet");
            try {
                File inputFile = new File(CSV_PATH + relation + ".csv");
                System.out.println("\n    Before file size of " + relation + ": " + inputFile.length() + " bytes");
                writeToParquet(relation, sampleData, schemas.get(relation), new Path(PARQUET_PATH + relation + ".parquet"), compressionSchemeString, dictionaryOptionString);
                File outputFile = new File(PARQUET_PATH + relation + ".parquet");
                System.out.println("\n    After file size of " + relation + ": " + outputFile.length() + " bytes\n");
            } catch (IOException e) {
                e.printStackTrace();
            }

            sampleData.clear();
        }

        // Query Parquet
        SparkSession spark = SparkSession.builder().appName("Small").config("spark.master", "local").getOrCreate();
        queryParquet(spark);
        spark.stop();
    }
}
