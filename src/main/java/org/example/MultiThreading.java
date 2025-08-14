package org.example;

import io.delta.kernel.Scan;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.utils.CloseableIterator;

import java.util.Optional;


import static org.example.Main.getColumnValue;

public class MultiThreading extends Thread {
    private PhysicalWrapperObject globalPhysicalDataItr;
    private final Engine engine;
    private final Row scanStateRow;
    public MultiThreading(PhysicalWrapperObject globalPhysicalDataItr, Engine engine, Row scanStateRow) {
        this.globalPhysicalDataItr = globalPhysicalDataItr;
        this.engine = engine;
        this.scanStateRow = scanStateRow;
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
                FilteredColumnarBatch logicalData = transformedData.next();
                ColumnarBatch dataBatch = logicalData.getData(); //Returns unfiltered cols
                Optional<ColumnVector> selectionVector = logicalData.getSelectionVector();
                int numCols = dataBatch.getSchema().length();
                int numRows = dataBatch.getSize();

                for(int rowIndex = 0; rowIndex < numRows; rowIndex++){
                    boolean rowSelected = (!selectionVector.get().isNullAt(rowIndex) &&
                                    selectionVector.get().getBoolean(rowIndex));
                    if(rowSelected) {
                        for(int colIdx = 0; colIdx<numCols; colIdx++){
                            ColumnVector columnVector = dataBatch.getColumnVector(colIdx);
                            Object value = getColumnValue(columnVector, rowIndex);
                            System.out.print(value);
                            if (colIdx < numCols - 1) {
                                System.out.print(", ");
                            }
                        }
                        System.out.println();
                    }
                }
            }
        }
        catch (Exception e){
            System.out.println("Exception is caught");
        }
    }
}
