package org.example;

import io.delta.kernel.Scan;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.utils.CloseableIterator;

import java.util.Optional;

import static org.example.Main.*;

public class Actor3 extends Thread {
    private PhysicalWrapperObject globalPhysicalDataItr;
    private final Engine engine;
    private final Row scanStateRow;

    public Actor3(PhysicalWrapperObject globalPhysicalDataItr, Engine engine, Row scanStateRow) {
        this.globalPhysicalDataItr = globalPhysicalDataItr;
        this.engine = engine;
        this.scanStateRow = scanStateRow;
    }

    public void run(){
        long threadStart = System.nanoTime();
        try{
            CloseableIterator<FilteredColumnarBatch> transformedData =
                    Scan.transformPhysicalData(
                            engine, scanStateRow,
                            globalPhysicalDataItr.getScanFileRow(),
                            globalPhysicalDataItr.getPhysicalDataIter()
                    );
            if (transformedData.hasNext()){
                FilteredColumnarBatch logicalData = transformedData.next();
                ColumnarBatch dataBatch = logicalData.getData(); //Returns unfiltered cols
                Optional<ColumnVector> selectionVector = logicalData.getSelectionVector();
                int numCols = dataBatch.getSchema().length();
                int numRows = dataBatch.getSize();

                boolean dvExist = false;
                if(selectionVector.isPresent()){
                    dvExist = true;
                }
                boolean rowSelected;
                for(int rowIndex = 0; rowIndex < numRows; rowIndex++){
                    rowSelected = true;
                    if(dvExist){
                        rowSelected = (!selectionVector.get().isNullAt(rowIndex) &&
                                selectionVector.get().getBoolean(rowIndex));
                    }
                   if(rowSelected) {
                        for(int colIdx = 0; colIdx<numCols; colIdx++){
                            ColumnVector columnVector = dataBatch.getColumnVector(colIdx);
                            getColumnValue(columnVector, rowIndex);
                        }
                   }
                }
            }
            /*Thread logs*/
            long threadEnd = System.nanoTime();
            long elapsedMs = (threadEnd - threadStart) / 1_000_000;
            System.out.println("Thread " + this.getName() + " finished in " + elapsedMs + " ms");
        }
        catch (Exception e){
            System.out.print("Exception is caught:");
            e.printStackTrace();
            System.out.println();
        }
    }
}
