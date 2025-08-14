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

import java.util.Optional;

import static io.delta.kernel.internal.util.Utils.singletonCloseableIterator;

public class Reader {
    private Scan scan;
    private Engine engine;
    public Reader(Scan scan, Engine engine){
        this.scan = scan;
        this.engine = engine;
    }
    public void getScanFiles(){
        //File inventory: What files to read from (path, size, modification time, partition values, stata, DV)
        CloseableIterator<FilteredColumnarBatch> scanFiles = scan.getScanFiles(engine);
        //Snapshot level information needed to read any file (Contains info needed for transforming physical data to logical data)
        //Contains partition info, filter(predicate) info
        Row scanState = scan.getScanState(engine);
        //Data transformation
        while(scanFiles.hasNext()){
            FilteredColumnarBatch batch = scanFiles.next();
            StructType physicalReadSchema = ScanStateRow.getPhysicalDataReadSchema(engine, scanState);

            try(CloseableIterator<Row> batchRows = batch.getRows()){
                while(batchRows.hasNext()) {
                    Row row = batchRows.next();
                    FileStatus fileStatus = InternalScanFileUtils.getAddFileStatus(row);
                    CloseableIterator<ColumnarBatch> physicalDataItr = engine.getParquetHandler().readParquetFiles(
                            singletonCloseableIterator(fileStatus),
                            physicalReadSchema,
                            Optional.empty()
                    );
                }
            }
            catch (Exception e){
                System.out.println("Error in scanning batch file rows");
                e.printStackTrace();
            }
        }



    }
}
