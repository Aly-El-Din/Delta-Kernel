package org.example;

import io.delta.kernel.data.Row;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.internal.data.ScanStateRow;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.FileStatus;

import java.io.IOException;
import java.util.Optional;

import static org.example.Main.*;

public class Actor3 extends Thread {
    private FileStatus fileStatus;
    private final Engine engine;
    private final Row scanStateRow;
    Optional<Predicate> predicate;
    public Actor3(FileStatus fileStatus, Engine engine, Row scanStateRow, Optional<Predicate> predicate) {
        this.fileStatus = fileStatus;
        this.engine = engine;
        this.scanStateRow = scanStateRow;
        this.predicate = predicate;
    }

    public void run(){
        System.out.println("Thread: "+currentThread().getName() + "started");
        StructType physicalReadSchema =
                ScanStateRow.getPhysicalDataReadSchema(engine, scanStateRow);
        /*StructType logicalReadSchema =
                ScanStateRow.getLogicalSchema(engine, scanStateRow);
        try {
            CloseableIterator<ColumnarBatch> physicalDataIter = engine.getParquetHandler().
                    readParquetFiles(singletonCloseableIterator(fileStatus), physicalReadSchema, Optional.empty());
            if(physicalDataIter.hasNext()){

            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }*/

        MyParquetReader parquetReader = new MyParquetReader(hadoopConfig, physicalReadSchema, predicate);
        if(fileStatus!=null){
            String filePath = fileStatus.getPath();
            try {
                parquetReader.readParquetFile(filePath);
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
            catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
