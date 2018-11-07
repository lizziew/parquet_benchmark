package com.ewei.parquet;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import java.io.File;
import java.io.Reader;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import static org.apache.parquet.column.ParquetProperties.WriterVersion.PARQUET_2_0;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.SparkSession.Builder; 
import java.io.BufferedWriter;
import java.io.FileWriter;

public class App
{
    public static final String OUTPUT_PATH = "/mnt/minwei/output.txt";
    public static final String CSV_PATH = "/mnt/minwei/parquet_pipeline/gendata.csv";
    public static final String PARQUET_PATH = "/mnt/minwei/parquet_benchmark/src/main/java/com/ewei/parquet/gendata.parquet";
    public static BufferedWriter outputWriter;

    public static void writeToParquet(List<GenericData.Record> recordsToWrite, Schema schema, Path fileToWrite) throws IOException {
        ParquetWriter<GenericData.Record> writer = AvroParquetWriter
                .<GenericData.Record>builder(fileToWrite)
		.withSchema(schema)
		.withWriterVersion(PARQUET_2_0)
		.withDictionaryEncoding(false)
		.withConf(new Configuration())
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .build();

	final long startTime = System.currentTimeMillis();
	for (GenericData.Record record : recordsToWrite) {
        	writer.write(record);
        }
	final long endTime = System.currentTimeMillis();
	outputWriter.write(("\nTotal write to parquet execution time: " + (endTime - startTime)));

        writer.close();
    }

    public static void queryParquet(SparkSession spark) throws IOException {
	Dataset<Row> parquetFileDF = spark.read().schema("col1 INT").parquet(PARQUET_PATH);
	parquetFileDF.createOrReplaceTempView("parquetFile"); 

	long startTime = 0;
	long endTime = 0;
	Dataset<Row> sqlDF; 

	for (int i = 0; i < 3; i++) {
		startTime = System.currentTimeMillis();
    		sqlDF = spark.sql("SELECT SUM(col1) FROM parquetFile");
		endTime = System.currentTimeMillis();
		outputWriter.write("\nTime: " + i + ": " + (endTime-startTime)); 
	}
     
	long totalTime = 0;
        for (int i = 0; i < 10; i++) {
		startTime = System.currentTimeMillis();
		sqlDF = spark.sql("SELECT SUM(col1) FROM parquetFile"); 
		endTime = System.currentTimeMillis();
		totalTime += (endTime - startTime);
		outputWriter.write("\nTime: " + i + ": " + (endTime-startTime)); 
	}
	long avgTime = totalTime / 10;
	outputWriter.write(("\nTotal query execution time: " + avgTime + "\n\n"));
    }

    public static void uncompressedBenchmark() {
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

    public static void main( String[] args ) throws IOException {
	// uncompressedBenchmark(); 

	// Set up output
	try {
		outputWriter = new BufferedWriter(new FileWriter(OUTPUT_PATH, true));
	} catch (IOException e) {
		System.out.println("Failed to create buffered writer: " + e);
	}

        // Create schema
        String schemaString = "{"
                + "\"namespace\": \"com.ewei.parquet\","
                + "\"type\": \"record\","
                + "\"name\": \"RecordName\","
                + "\"fields\": ["
                + " {\"name\": \"col1\", \"type\": \"int\"}"
                + " ]}";
        Schema.Parser parser = new Schema.Parser().setValidate(true);
        Schema schema = parser.parse(schemaString);

        // Data to be written to Parquet
        List<GenericData.Record> sampleData = new ArrayList<GenericData.Record>();

        System.out.println("Reading in CSV file");
        // Read in CSV file
        try {
            Reader in = new FileReader(CSV_PATH);
            Iterable<CSVRecord> records = CSVFormat.DEFAULT.withDelimiter('|').parse(in);
            for (CSVRecord record : records) {
                GenericData.Record genericRecord = new GenericData.Record(schema);
                genericRecord.put("col1", Integer.parseInt(record.get(0)));
                sampleData.add(genericRecord);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        // Delete old Parquet, if exists
        File f = new File(PARQUET_PATH);
        f.delete();

        System.out.println("Writing Parquet");
        // Write new Parquet
        try {
            writeToParquet(sampleData, schema, new Path(PARQUET_PATH));
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println("Querying Parquet");
    	SparkSession spark = SparkSession.builder().appName("BenchmarkParquetSum").config("spark.master", "local").getOrCreate();

	  queryParquet(spark);
	
	 spark.stop();
	outputWriter.close();
    }
}
