package com.ewei.parquet;

import java.io.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.bytes.*;
import org.apache.parquet.column.values.delta.*;
import org.apache.parquet.hadoop.CodecFactory;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;


public class App {
    private static final CodecFactory CODEC_FACTORY = new CodecFactory(new Configuration(), 1024);
    private static final CodecFactory.BytesInputCompressor GZIP_COMPRESSOR =  CODEC_FACTORY.getCompressor(CompressionCodecName.GZIP);
    private static final CodecFactory.BytesInputCompressor SNAPPY_COMPRESSOR = CODEC_FACTORY.getCompressor(CompressionCodecName.SNAPPY);

    public static void main(String[] args) {
        // Set up delta binary packing encoder
        int slabSize = 256;
        int pageSize = 1000;
        DirectByteBufferAllocator bytesAllocator = DirectByteBufferAllocator.getInstance();
        DeltaBinaryPackingValuesWriterForInteger deltaBinaryPackingValuesWriterForInteger = new DeltaBinaryPackingValuesWriterForInteger(slabSize, pageSize, bytesAllocator);

        File columnFile = new File("/Users/elizabethwei/code/parquet_benchmark/src/main/java/com/ewei/parquet/l_shipdate");

        try {
            BufferedReader br = new BufferedReader(new FileReader(columnFile));
            try {
                String line;
                while ((line = br.readLine()) != null) {
                    line = line.replaceAll("\\D", "");
                    deltaBinaryPackingValuesWriterForInteger.writeInteger(Integer.parseInt(line));
                }
            } finally {
                br.close();
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            // Get encoded size
            byte[] encodedBytes = deltaBinaryPackingValuesWriterForInteger.getBytes().toByteArray();
            System.out.println("encoded length: " + encodedBytes.length);

            BytesInput input = BytesInput.from(encodedBytes);

            // Get snappy size
            BytesInput snappy = SNAPPY_COMPRESSOR.compress(input);
            System.out.println("snappy size: " + snappy.size());

            // Get gzip size
            BytesInput gzip = GZIP_COMPRESSOR.compress(input);
            System.out.println("gzip size: " + gzip.size());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
