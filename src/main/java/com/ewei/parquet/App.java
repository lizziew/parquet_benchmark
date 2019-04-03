package com.ewei.parquet;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.bytes.*;
import org.apache.parquet.column.values.delta.*;
import org.apache.parquet.hadoop.CodecFactory;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;


public class App {
    private static final CodecFactory CODEC_FACTORY = new CodecFactory(new Configuration(), 1024);
    private static final CodecFactory.BytesInputCompressor GZIP_COMPRESSOR =
            CODEC_FACTORY.getCompressor(CompressionCodecName.GZIP);
    private static final CodecFactory.BytesInputCompressor SNAPPY_COMPRESSOR =
            CODEC_FACTORY.getCompressor(CompressionCodecName.SNAPPY);

    public static void main(String[] args) {
        // Original data
//        File initialFile = new File("/Users/elizabethwei/code/parquet_benchmark/src/main/java/com/ewei/parquet/sample.txt");
//        try {
//            // Encoded, snappy setup
//            System.out.println(initialFile.length());
//            InputStream targetStream = new FileInputStream(initialFile);
//            BytesInput input = BytesInput.from(targetStream, (int) initialFile.length());
//
//
//
//        } catch (FileNotFoundException e) {
//            e.printStackTrace();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }


        // Encoded, un-compressed setup
        DirectByteBufferAllocator bytesAllocator = DirectByteBufferAllocator.getInstance();
        int slabSize = 256;
        int pageSize = 1000;
        DeltaBinaryPackingValuesWriterForInteger deltaBinaryPackingValuesWriterForInteger =
                new DeltaBinaryPackingValuesWriterForInteger(slabSize, pageSize, bytesAllocator);

        for (int i = 1; i <= 1e6; i++) {
            deltaBinaryPackingValuesWriterForInteger.writeInteger(i);
        }

        try {
            byte[] encodedBytes = deltaBinaryPackingValuesWriterForInteger.getBytes().toByteArray();
            System.out.println("encoded length: " + encodedBytes.length);

            BytesInput input = BytesInput.from(encodedBytes);

            BytesInput snappy = SNAPPY_COMPRESSOR.compress(input);
            System.out.println("snappy size: " + snappy.size());

            BytesInput gzip = GZIP_COMPRESSOR.compress(input);
            System.out.println("gzip size: " + snappy.size());
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
