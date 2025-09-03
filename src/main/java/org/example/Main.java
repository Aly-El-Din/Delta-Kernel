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
import java.util.concurrent.atomic.AtomicInteger;


public class Main {
    public static Configuration hadoopConfig;
    public static String tablePath;
    public static AtomicInteger totalNumberOfRowsRead = new AtomicInteger(0);
    public static MessageType physicalSchemaForAllParquetFiles;

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
    public static void readParquetFilesInMemory(List<WrapperObject> statusesAndScanFiles, Engine engine, Row scanStateRow) throws IOException {
        List<Thread> threads = new ArrayList<>();

        if(statusesAndScanFiles.size()>0){
            ParquetFileReader parquetFileReader = createParquetFileReader(statusesAndScanFiles.get(0).getFileStatus().getPath());
            physicalSchemaForAllParquetFiles = parquetFileReader.getFooter().getFileMetaData().getSchema();
        }

        for(WrapperObject obj:statusesAndScanFiles){
            Thread fileReader = new Actor3(obj.getFileStatus(), engine, obj.getScanFileRow(), Optional.empty());
            threads.add(fileReader);
            fileReader.start();
        }

        for(Thread fr:threads) {
            try{
                fr.join();
            }
            catch (InterruptedException e) {
                System.err.println("Thread interrupted "+e.getMessage());
                Thread.currentThread().interrupt();
            }
        }
    }
    public static void main(String[] args) {

        //Get args
        /*if(args.length < 2){
            System.out.println("Usage: java -jar MyApp.jar <tablePath> <outputLogTxtFile>");
            System.exit(1);
        }*/
        hadoopConfig = new Configuration();
        Engine engine = DefaultEngine.create(hadoopConfig);
        tablePath = "C:\\Users\\Cyber\\Downloads\\smallTable_5000_10_50";

        //1.Table initialization
        try{
            Table table = Table.forPath(engine, tablePath);
            System.out.println("Delta table initialized=> Table class: "+table.getClass().getSimpleName());

            //2.snapshot creation
            Snapshot snapshot = table.getLatestSnapshot(engine);

            //3.Scan planning
            try {
                ScanBuilder scanBuilder = snapshot.getScanBuilder(engine);
                Scan scan = scanBuilder.build();
                System.out.println("Scanner created");

                //scanStateRow -> snapshot-wide metadata && info for transforming physical schema to logical schema
                Row scantStateRow = scan.getScanState(engine);

                //scanFiles iterator -> file-inventory having parquet files data to be read (path, size, dv, stats, physical schema)
                CloseableIterator<FilteredColumnarBatch> scanFiles = scan.getScanFiles(engine);

                //Collecting physical data iter (columnar batches) with its corresponding scan file row
                List<WrapperObject> fileStatusesAndScanFilesRows = getFilesStatuses(scanFiles);

                readParquetFilesInMemory(fileStatusesAndScanFilesRows, engine, scantStateRow);
                System.out.println("Total number of rows read =====> " + totalNumberOfRowsRead);
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
