package org.example;

import io.delta.kernel.Scan;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.utils.CloseableIterator;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.example.Main.*;

public class Actor3 extends Thread {
    private final PhysicalWrapperObject globalPhysicalDataItr;
    private final Engine engine;
    private final Row scanStateRow;
    private final int threadId;

    public Actor3(PhysicalWrapperObject globalPhysicalDataItr, Engine engine, Row scanStateRow, int threadId) {
        this.globalPhysicalDataItr = globalPhysicalDataItr;
        this.engine = engine;
        this.scanStateRow = scanStateRow;
        this.threadId = threadId;
    }
    public void run(){
        try{
            CloseableIterator<FilteredColumnarBatch> transformedData =
                    Scan.transformPhysicalData(
                            engine, scanStateRow,
                            globalPhysicalDataItr.getScanFileRow(),
                            globalPhysicalDataItr.getPhysicalDataIter()
                    );

            if (transformedData.hasNext()){

                List<FilteredColumnarBatch> allBatches = new ArrayList<>();
                while (transformedData.hasNext()) {
                    allBatches.add(transformedData.next());
                }
                FilteredColumnarBatch firstBatch = allBatches.get(0);
                // Extract metadata from first batch
                ColumnarBatch dataBatch = firstBatch.getData();
                Optional<ColumnVector> selectionVector = firstBatch.getSelectionVector();

                int numCols = dataBatch.getSchema().length();
                int numRows = dataBatch.getSize();
                boolean dvExist = selectionVector.isPresent();

                // Write parquet files
                Actor3ParquetWriter parquetWriter = new Actor3ParquetWriter(dataBatch.getSchema(), threadId);

                parquetWriter.writeParquet(allBatches);

                boolean rowSelected;
                for(int rowIndex = 0; rowIndex < numRows; rowIndex++) {
                    rowSelected = true;
                    if(dvExist){
                        rowSelected = (!selectionVector.get().isNullAt(rowIndex) &&
                                selectionVector.get().getBoolean(rowIndex));
                    }
                   if(rowSelected) {
                        for(int colIdx = 0; colIdx<numCols; colIdx++){
                            ColumnVector columnVector = dataBatch.getColumnVector(colIdx);
                        }
                   }
                }
            }
        }
        catch (Exception e){
            System.out.print("Exception is caught:");
            e.printStackTrace();
            System.out.println();
        }
    }
}
