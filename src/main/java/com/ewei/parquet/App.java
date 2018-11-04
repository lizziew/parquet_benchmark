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

public class App
{
    public static final String CSV_PATH = "/mnt/minwei/parquet_pipeline/gendata.csv";
    public static final String PARQUET_PATH = "/mnt/minwei/parquet_benchmark/src/main/java/com/ewei/parquet/gendata.parquet";

    public static void writeToParquet(List<GenericData.Record> recordsToWrite, Schema schema, Path fileToWrite) throws IOException {
        ParquetWriter<GenericData.Record> writer = AvroParquetWriter
                .<GenericData.Record>builder(fileToWrite)
		.withSchema(schema)
		.withWriterVersion(PARQUET_2_0)
		.withDictionaryEncoding(false)
		.withConf(new Configuration())
                .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
                .build();

	for (GenericData.Record record : recordsToWrite) {
        	writer.write(record);
        }

        writer.close();
    }

    public static void queryParquet(SparkSession spark) {
	Dataset<Row> parquetFileDF = spark.read().parquet("/mnt/minwei/parquet_benchmark/src/main/java/com/ewei/parquet/gendata.parquet");
	parquetFileDF.createOrReplaceTempView("parquetFile"); 
    	Dataset<Row> sqlDF = spark.sql("SELECT * FROM parquetFile");
	sqlDF.show(); 
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
    	SparkSession spark = SparkSession
		    .builder()
		    .appName("BenchmarkParquetSum")
		    .getOrCreate();

	    queryParquet(spark);
	
	    spark.stop();
    }
}
