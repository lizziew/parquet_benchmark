package com.ewei.parquet;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import java.io.File;
import java.io.Reader;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;

public class App 
{
    public static final String CSV_PATH = "/Users/elizabethwei/code/benchmark/src/main/java/com/ewei/parquet/gendata.csv";
    public static final String PARQUET_PATH = "/Users/elizabethwei/code/benchmark/src/main/java/com/ewei/parquet/gendata.parquet";

    public static void writeToParquet(List<GenericData.Record> recordsToWrite, Schema schema, Path fileToWrite) throws IOException {
        ParquetWriter<GenericData.Record> writer = AvroParquetWriter
                .<GenericData.Record>builder(fileToWrite)
                .withSchema(schema)
                .withConf(new Configuration())
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .build();

        for (GenericData.Record record : recordsToWrite) {
            writer.write(record);
        }

        writer.close();
    }

    public static void main( String[] args ) {
        // Create schema
        String schemaString = "{"
                + "\"namespace\": \"com.ewei.parquet\","
                + "\"type\": \"record\","
                + "\"name\": \"RecordName\","
                + "\"fields\": ["
                + " {\"name\": \"col1\", \"type\": \"int\"},"
                + " {\"name\": \"col2\", \"type\": \"int\"},"
                + " {\"name\": \"col3\", \"type\": \"int\"}"
                + " ]}";
        Schema.Parser parser = new Schema.Parser().setValidate(true);
        Schema schema = parser.parse(schemaString);

        // Data to be written to Parquet
        List<GenericData.Record> sampleData = new ArrayList<GenericData.Record>();

        // Read in CSV file
        try {
            Reader in = new FileReader(CSV_PATH);
            Iterable<CSVRecord> records = CSVFormat.DEFAULT.withDelimiter('|').parse(in);
            for (CSVRecord record : records) {
                GenericData.Record genericRecord = new GenericData.Record(schema);
                genericRecord.put("col1", Integer.parseInt(record.get(0)));
                genericRecord.put("col2", Integer.parseInt(record.get(1)));
                genericRecord.put("col3", Integer.parseInt(record.get(2)));
                sampleData.add(genericRecord);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        // Delete old Parquet, if exists
        File f = new File(PARQUET_PATH);
        f.delete();

        // Write new Parquet
        try {
            writeToParquet(sampleData, schema, new Path(PARQUET_PATH));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}