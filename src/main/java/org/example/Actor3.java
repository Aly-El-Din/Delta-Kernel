package org.example;

import io.delta.kernel.Scan;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.utils.CloseableIterator;

import java.io.PrintWriter;
import java.util.Optional;

import static org.example.Main.*;

public class Actor3 extends Thread {
    private PhysicalWrapperObject globalPhysicalDataItr;
    private final Engine engine;
    private final Row scanStateRow;
    //private PrintWriter csvWriter;

    public Actor3(PhysicalWrapperObject globalPhysicalDataItr, Engine engine, Row scanStateRow /*, PrintWriter csvWriter*/) {
        this.globalPhysicalDataItr = globalPhysicalDataItr;
        this.engine = engine;
        this.scanStateRow = scanStateRow;
        //this.csvWriter = csvWriter;
    }
    /*public synchronized void writeRowToCsv(String row) {
        csvWriter.println(row);
    }*/

    /*public static String escapeCsvValue(Object value) {
        if (value == null) {
            return "";
        }

        String stringValue = value.toString().trim();

        if (stringValue.contains(",") ||
                stringValue.contains("\"") ||
                stringValue.contains("\n") ||
                stringValue.contains("\r")) {
            stringValue = stringValue.replace("\"", "\"\"");
            return "\"" + stringValue + "\"";
        }

        return stringValue;
    }*/
    public void run(){
        /*long threadStart = System.nanoTime();
        long rowsWriteTime = 0;*/

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

                boolean flag = false;
                if(selectionVector.isPresent()){
                    flag = true;
                }
                boolean rowSelected;
                for(int rowIndex = 0; rowIndex < numRows; rowIndex++){
                    rowSelected = true;
                    if(flag){
                        rowSelected = (!selectionVector.get().isNullAt(rowIndex) &&
                                selectionVector.get().getBoolean(rowIndex));
                    }
                   if(rowSelected) {
                       /*
                       StringBuilder rowBuilder = new StringBuilder();
                       long rowsBuildTime = 0;*/
                        for(int colIdx = 0; colIdx<numCols; colIdx++){
                            ColumnVector columnVector = dataBatch.getColumnVector(colIdx);
                            Object value = getColumnValue(columnVector, rowIndex);
                            /*
                            long rowBuildTime = System.nanoTime();
                            rowBuilder.append(escapeCsvValue(value));
                            if (colIdx < numCols - 1) {
                                rowBuilder.append(",");
                            }
                            rowsBuildTime += (System.nanoTime()-rowBuildTime);*/
                        }
                        /*
                        long rowWriteStart = System.nanoTime();
                        writeRowToCsv(rowBuilder.toString());
                        long rowWriteEnd = System.nanoTime();
                        rowsWriteTime +=(rowWriteEnd - rowWriteStart) + rowsBuildTime;*/
                   }
                }
            }
            /*Thread logs*/
            /*
            long threadEnd = System.nanoTime();
            long totalThreadTime = threadEnd - threadStart;
            long elapsedMs = (totalThreadTime-rowsWriteTime) / 1_000_000;


            //System.out.println("Thread " + this.getName() + " finished in " + elapsedMs + " ms");
            StringBuilder threadRow = new StringBuilder();
            threadRow.append(escapeCsvValue(this.getName()));
            threadRow.append(",");
            threadRow.append(escapeCsvValue(elapsedMs));
            writeRowToCsv(threadRow.toString());*/
        }
        catch (Exception e){
            System.out.print("Exception is caught:");
            e.printStackTrace();
            System.out.println();
        }
        /*finally {
            if(csvWriter != null) {
                csvWriter.close();
            }
        }*/
    }
}
