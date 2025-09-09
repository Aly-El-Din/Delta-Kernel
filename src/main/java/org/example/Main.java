package org.example;
import io.delta.kernel.*;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.exceptions.TableNotFoundException;
import io.delta.kernel.internal.InternalScanFileUtils;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.MessageType;

import java.io.*;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;


public class Main {
    public static Configuration hadoopConfig;
    public static String tablePath;
    public static String outputLogFilePath;
    public static AtomicInteger totalNumberOfRowsRead = new AtomicInteger(0);
    public static MessageType physicalSchemaForAllParquetFiles;
    private static StringBuilder getTableName(String tablePath) {
        int pathLength = tablePath.length();
        int idx = pathLength-1;
        StringBuilder tableName = new StringBuilder();
        while(idx>=0 && tablePath.charAt(idx)!='\\'){
            tableName.append(tablePath.charAt(idx));
            idx--;
        }
        tableName.reverse();
        return tableName;
    }
    public static List<WrapperObject> getFilesStatuses(CloseableIterator<FilteredColumnarBatch> scanFiles) throws IOException {

        List<WrapperObject> objects = new ArrayList<>();

        while (scanFiles.hasNext()) {
            FilteredColumnarBatch scanFileColumnarBatch = scanFiles.next();

            //Get physical read schema of columns to read the parquet files
            try(CloseableIterator<Row> scanFileRows = scanFileColumnarBatch.getRows()){
                while(scanFileRows.hasNext()) {
                    Row scanFileRow = scanFileRows.next();
                    //extracting all needed info about file (path, size, time metadata)
                    FileStatus fileStatus = InternalScanFileUtils.getAddFileStatus(scanFileRow);
                    objects.add(new WrapperObject(scanFileRow, fileStatus));
                }
            }
        }
        return objects;
    }
    private static ParquetFileReader createParquetFileReader(String filePath) throws IOException {
        return ParquetFileReader.open(
                HadoopInputFile.fromPath(new Path(filePath), hadoopConfig));
    }
    public static void readParquetFilesInMemory(List<WrapperObject> statusesAndScanFiles, Engine engine) throws IOException {

        if(statusesAndScanFiles.size()>0){
            ParquetFileReader parquetFileReader = createParquetFileReader(statusesAndScanFiles.get(0).getFileStatus().getPath());
            physicalSchemaForAllParquetFiles = parquetFileReader.getFooter().getFileMetaData().getSchema();
        }
        int numThreads = Math.min(statusesAndScanFiles.size(), Runtime.getRuntime().availableProcessors());
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);

        if(statusesAndScanFiles.size()>0){
            try (ParquetFileReader parquetFileReader = createParquetFileReader(statusesAndScanFiles.get(0).
                    getFileStatus().getPath())) {
                physicalSchemaForAllParquetFiles = parquetFileReader.getFooter().getFileMetaData().getSchema();
            }
        }

        for(WrapperObject obj:statusesAndScanFiles){
            Thread fileReader = new Actor3(obj.getFileStatus(), engine, obj.getScanFileRow());
            executor.submit(fileReader);
        }
        executor.shutdown();
        try {
            if (!executor.awaitTermination(1, TimeUnit.HOURS)) {
                System.err.println("File processing threads did not terminate in the specified time.");
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            System.err.println("Main thread interrupted while waiting for file processors to finish.");
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
    public static void main(String[] args) {

        //Get args
        if(args.length < 2){
            System.out.println("Usage: java -jar MyApp.jar <tablePath> <outputLogFilePath>");
            System.exit(1);
        }
        hadoopConfig = new Configuration();
        Engine engine = DefaultEngine.create(hadoopConfig);
        tablePath = args[0];
        outputLogFilePath = args[1];

        //1.Table initialization
        try{
            long multiThreadedStartTime = System.nanoTime();
            Table table = Table.forPath(engine, tablePath);
            System.out.println("Delta table initialized=> Table class: "+table.getClass().getSimpleName());

            //2.snapshot creation
            Snapshot snapshot = table.getLatestSnapshot(engine);

            //3.Scan planning
            try {
                ScanBuilder scanBuilder = snapshot.getScanBuilder(engine);
                Scan scan = scanBuilder.build();
                System.out.println("Scanner created");

                //scanFiles iterator -> file-inventory having parquet files data to be read (path, size, dv, stats, physical schema)
                CloseableIterator<FilteredColumnarBatch> scanFiles = scan.getScanFiles(engine);

                //Collecting physical data iter (columnar batches) with its corresponding scan file row
                List<WrapperObject> fileStatusesAndScanFilesRows = getFilesStatuses(scanFiles);

                readParquetFilesInMemory(fileStatusesAndScanFilesRows, engine);
                long multiThreadedEndTime = System.nanoTime();
                System.out.println("Total number of rows read =====> " + totalNumberOfRowsRead);
                long elapsedTime = (multiThreadedEndTime - multiThreadedStartTime) / 1_000_000;
                FileWriter fileWriter = new FileWriter(outputLogFilePath, true);
                fileWriter.write("ACTOR 3 V4 READS | "+getTableName(tablePath)+" | IN "+elapsedTime+" MILLI SECONDS");
                fileWriter.write("\n");
                fileWriter.close();
                System.out.println("Actor 3 V4 reading Time: "+elapsedTime);
            }
            catch (Exception e) {
                System.err.println("Error creating scanner");
                e.printStackTrace();
            }
        }
        catch (TableNotFoundException e){
            System.err.println("Delta table is not found at this path");
            e.printStackTrace();
        }
        catch (Exception e){
            System.err.println("Error: "+e);
            e.printStackTrace();
        }
    }
}
