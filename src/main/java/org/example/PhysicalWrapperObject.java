package org.example;

import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.utils.CloseableIterator;

public class PhysicalWrapperObject {
    private Row scanFileRow;
    private CloseableIterator<ColumnarBatch> physicalDataIter;
    public PhysicalWrapperObject(Row scanFileRow,
                                 CloseableIterator<ColumnarBatch> physicalDataIter) {
        this.scanFileRow = scanFileRow;
        this.physicalDataIter = physicalDataIter;
    }

    public Row getScanFileRow() {
        return scanFileRow;
    }

    public CloseableIterator<ColumnarBatch> getPhysicalDataIter() {
        return physicalDataIter;
    }
}
