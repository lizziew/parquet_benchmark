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
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import java.io.File;

public class App 
{
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
        String csvFile = "/Users/elizabethwei/code/benchmark/src/main/java/com/ewei/parquet/gendata.csv";
        BufferedReader br = null;
        String line;
        String delimiter = "\\|";

        try {
            br = new BufferedReader(new FileReader(csvFile));
            while ((line = br.readLine()) != null) {
                String[] split = line.split(delimiter);
                System.out.println(Arrays.asList(split));
                GenericData.Record record = new GenericData.Record(schema);
                record.put("col1", Integer.parseInt(split[0]));
                record.put("col2", Integer.parseInt(split[1]));
                record.put("col3", Integer.parseInt(split[2]));
                sampleData.add(record);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        System.out.println(sampleData);

        // Delete old Parquet, if exists
        File f = new File("/Users/elizabethwei/code/benchmark/src/main/java/com/ewei/parquet/gendata.parquet");
        f.delete();

        // Write new Parquet
        try {
            writeToParquet(sampleData, schema, new Path("/Users/elizabethwei/code/benchmark/src/main/java/com/ewei/parquet/gendata.parquet"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}