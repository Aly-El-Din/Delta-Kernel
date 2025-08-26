package org.example;
import io.delta.kernel.*;
import io.delta.kernel.data.ColumnVector;
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
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.*;


public class Main {
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
    public static Object getColumnValue(ColumnVector column, int rowIndex) {
        if (column.isNullAt(rowIndex)) {
            return null;
        }
        DataType dataType = column.getDataType();

        if (dataType instanceof StringType) {
            return column.getString(rowIndex);
        } else if (dataType instanceof IntegerType) {
            return column.getInt(rowIndex);
        } else if (dataType instanceof LongType) {
            return column.getLong(rowIndex);
        } else if (dataType instanceof DoubleType) {
            return column.getDouble(rowIndex);
        } else if(dataType instanceof DecimalType) {
            BigDecimal decimalValue = column.getDecimal(rowIndex);
            return decimalValue.doubleValue();
        } else if (dataType instanceof BooleanType) {
            return column.getBoolean(rowIndex);
        }else if (dataType instanceof TimestampType) {
            // microseconds since epoch -> convert to Instant
            long micros = column.getLong(rowIndex);
            Instant instant = Instant.ofEpochSecond(
                    micros / 1_000_000,
                    (micros % 1_000_000) * 1000
            );

            // Use system default zone offset (or ZoneOffset.of("+03:00") if you want fixed)
            return DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSXXX")
                    .withZone(ZoneOffset.systemDefault())  // <--- adjust here
                    .format(instant);
        } else if (dataType instanceof TimestampType) {
            long micros = column.getLong(rowIndex);
            Instant instant = Instant.ofEpochSecond(
                    micros / 1_000_000,
                    (micros % 1_000_000) * 1000
            );
            return DateTimeFormatter.ISO_OFFSET_DATE_TIME
                    .withZone(ZoneOffset.UTC)
                    .format(instant);
        } else if (dataType instanceof DateType) {
            int days = column.getInt(rowIndex);
            LocalDate date = LocalDate.ofEpochDay(days);
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("M/d/yyyy");
            return date.format(formatter);
        }
        return column.toString();
    }
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
            System.out.println("Usage: java -jar MyApp.jar <tablePath> <outputLogTxtFile>");
            System.exit(1);
        }
        Configuration hadoopConfig = new Configuration();
        Engine engine = DefaultEngine.create(hadoopConfig);
        String tablePath = args[0];
        String outputLogFilePath = args[1];
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
                //scanStateRow -> snapshot-wide metadata && info for transforming physical schema to logical schema
                Row scantStateRow = scan.getScanState(engine);
                //scanFiles iterator -> file-inventory having parquet files data to be read (path, size, dv, stats, physical schema)
                CloseableIterator<FilteredColumnarBatch> scanFiles = scan.getScanFiles(engine);
                //Collecting physical data iter (columnar batches) with its corresponding scan file row
                List<PhysicalWrapperObject> globalLogicalDataAtt = getLogicalDataAttributes(scanFiles, engine, scantStateRow);

                List<Thread> threads = new ArrayList<>();
                for(PhysicalWrapperObject obj:globalLogicalDataAtt){
                    try {
                        Thread t = new Actor3(obj, engine, scantStateRow);
                        threads.add(t);
                        t.start();
                    } catch (Exception e) {
                        System.out.println("Error while executing thread ===> "+e);
                    }
                }

                //Wait for threads to complete
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
                FileWriter fileWriter = new FileWriter(outputLogFilePath, true);
                fileWriter.write("\n");
                fileWriter.write("ACTOR 3 READS | "+getTableName(tablePath)+" | IN "+elapsedTime+" SECONDS");
                fileWriter.close();
                System.out.println("Actor 3 reading Time: "+elapsedTime);
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