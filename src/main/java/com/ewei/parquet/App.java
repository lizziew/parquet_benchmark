package com.ewei.parquet;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

import org.apache.parquet.bytes.*;
import org.apache.parquet.column.values.delta.*;


public class App {
    public static void main(String[] args) {
        DirectByteBufferAllocator bytesAllocator = DirectByteBufferAllocator.getInstance();
        int slabSize = 256;
        int pageSize = 1000;

        DeltaBinaryPackingValuesWriterForInteger deltaBinaryPackingValuesWriterForInteger =
                new DeltaBinaryPackingValuesWriterForInteger(slabSize, pageSize, bytesAllocator);
        List<Byte> plainNumberBytes = new ArrayList<Byte>();

        for (int i = 0; i < 100; i++) {
            deltaBinaryPackingValuesWriterForInteger.writeInteger(i);
            plainNumberBytes.add(new Integer(i).byteValue());
        }

        try {
            byte[] encodedBytes = deltaBinaryPackingValuesWriterForInteger.getBytes().toByteArray();
            System.out.println("encoded length: " + encodedBytes.length);
            System.out.println("previous length: " + plainNumberBytes.size());
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
