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

import java.util.*;


public class Main {
    public static void exploreSnapshot(Snapshot snapshot, Engine engine){
        try {
            long version = snapshot.getVersion(engine);
            StructType schema = snapshot.getSchema(engine);
            System.out.println("Schema json:" + schema.toJson());
            List<StructField> fieldList = schema.fields();
            System.out.println("Schema fields:\n");
            for(StructField field:fieldList) {
                System.out.println("Field name: " + field.getName()
                        + "\ntype: " + field.getDataType().toString());
            }
        }
        catch (Exception e){
            System.err.println("Error exploring snapshot: " + e.getMessage());
        }
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
        } else if (dataType instanceof BooleanType) {
            return column.getBoolean(rowIndex);
        } else if (dataType instanceof DateType) {
            return column.getInt(rowIndex);
        } else if (dataType instanceof TimestampType) {
            return column.getLong(rowIndex);
        }
        return column.toString();
    }

    public static void main(String[] args) {


        Configuration hadoopConfig = new Configuration();
        Engine engine = DefaultEngine.create(hadoopConfig);
        String tablePath = "C:\\Users\\Cyber\\Desktop\\deltalake_project\\Delta-Lake-Mini-Project\\delta-table-test_table";

        System.out.println("Engine components:");
        System.out.println("FileSystemClient: " + engine.getFileSystemClient().getClass().getSimpleName());
        System.out.println("JsonHandler: " + engine.getJsonHandler().getClass().getSimpleName());
        System.out.println("ParquetHandler: " + engine.getParquetHandler().getClass().getSimpleName());
        System.out.println("ExpressionHandler: " + engine.getExpressionHandler().getClass().getSimpleName());

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
                Row scantStateRow = scan.getScanState(engine);
                CloseableIterator<FilteredColumnarBatch> scanFiles = scan.getScanFiles(engine);

                List<PhysicalWrapperObject> globalLogicalDataAtt = new ArrayList<>();

                int scanFileNum = 1;
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
                    scanFileNum++;
                }

                //TODO: system log for each thread
                long startTime = System.nanoTime();
                for(PhysicalWrapperObject obj:globalLogicalDataAtt){
                    Thread t = new MultiThreading(obj, engine, scantStateRow);
                    t.start();
                }
                long endTime = System.nanoTime();

                long elapsedTime = (endTime - startTime) / 1_000_000;
                System.out.println("Execution Time: "+elapsedTime+" ms");
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