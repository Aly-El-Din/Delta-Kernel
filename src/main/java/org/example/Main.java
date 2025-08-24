package org.example;
import io.delta.kernel.*;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.exceptions.TableNotFoundException;
import io.delta.kernel.internal.InternalScanFileUtils;
import io.delta.kernel.internal.data.ScanStateRow;
import io.delta.kernel.types.*;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;
import org.apache.hadoop.conf.Configuration;
import static io.delta.kernel.internal.util.Utils.singletonCloseableIterator;

import java.io.*;
import java.util.*;

public class Main {
    public static String outputDirectoryPath;

    public static List<PhysicalWrapperObject> getLogicalDataAttributes(CloseableIterator<FilteredColumnarBatch> scanFiles,
                                                                       Engine engine, Row scantStateRow) throws IOException {

        List<PhysicalWrapperObject> globalLogicalDataAtt = new ArrayList<>();
        while (scanFiles.hasNext()) {
            FilteredColumnarBatch scanFileColumnarBatch = scanFiles.next();

            //Get physical read schema of columns to read the parquet files
            StructType physicalReadSchema = ScanStateRow.getPhysicalDataReadSchema(engine, scantStateRow);

            try(CloseableIterator<Row> scanFileRows = scanFileColumnarBatch.getRows()){
                while(scanFileRows.hasNext()) {
                    Row scanFileRow = scanFileRows.next();
                    //extracting all needed info about file (path, size, time metadata)
                    FileStatus fileStatus = InternalScanFileUtils.getAddFileStatus(scanFileRow);
                    CloseableIterator<ColumnarBatch> physicalDataIter = engine.getParquetHandler().readParquetFiles(
                            singletonCloseableIterator(fileStatus),
                            physicalReadSchema,
                            Optional.empty()
                    );
                    globalLogicalDataAtt.add(new PhysicalWrapperObject(scanFileRow, physicalDataIter));
                }
            }
        }
        return globalLogicalDataAtt;
    }
    public static void main(String[] args) {

        //Get args
       if(args.length < 2){
            System.out.println("Usage: java -jar MyApp.jar <tablePath> <outputDir>");
            System.exit(1);
        }

        Configuration hadoopConfig = new Configuration();
        Engine engine = DefaultEngine.create(hadoopConfig);

        String tablePath = args[0];
        outputDirectoryPath = args[1];

        //1.Table initialization
        try{
            long multiThreadStartTime = System.nanoTime();
            Table table = Table.forPath(engine, tablePath);
            System.out.println("Delta table initialized=> Table class: "+table.getClass().getSimpleName());

            //2.snapshot creation
            Snapshot snapshot = table.getLatestSnapshot(engine);

            //3.Scan planning
            try {
                ScanBuilder scanBuilder = snapshot.getScanBuilder(engine);
                Scan scan = scanBuilder.build();
                System.out.println("Scanner created");
                Row scantStateRow = scan.getScanState(engine);
                CloseableIterator<FilteredColumnarBatch> scanFiles = scan.getScanFiles(engine);
                List<PhysicalWrapperObject> globalLogicalDataAtt = getLogicalDataAttributes(scanFiles, engine, scantStateRow);

                List<Thread> threads = new ArrayList<>();
                int threadIndex = 1;
                for(PhysicalWrapperObject obj:globalLogicalDataAtt){
                    try {
                        Thread t = new Actor3(obj, engine, scantStateRow, threadIndex);
                        threads.add(t);
                        t.start();
                        threadIndex++;
                    } catch (Exception e) {
                        System.out.println("Error===>"+e);
                    }
                }

                //Wait for threads to complete
                //TODO: Specify number of threads dynamically
                for(Thread t:threads) {
                    try{
                        t.join();
                    }
                    catch (InterruptedException e){
                        System.err.println("Thread interrupted "+e.getMessage());
                        Thread.currentThread().interrupt();
                    }
                }
                long multiThreadEndTime = System.nanoTime();
                long elapsedTime = (multiThreadEndTime - multiThreadStartTime) / 1_000_000;



                System.out.println("Number of executed threads: "+threads.size());
                System.out.println("Actor-3 reading time: " + elapsedTime);
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