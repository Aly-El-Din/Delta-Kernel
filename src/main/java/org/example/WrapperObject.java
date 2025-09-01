package org.example;

import io.delta.kernel.data.Row;
import io.delta.kernel.utils.FileStatus;

public class WrapperObject {
    private Row scanFileRow;
    private FileStatus fileStatus;
    public WrapperObject(Row scanFileRow,
                         FileStatus fileStatus) {
        this.scanFileRow = scanFileRow;
        this.fileStatus = fileStatus;
    }

    public Row getScanFileRow() {
        return scanFileRow;
    }

    public FileStatus getFileStatus() {
        return fileStatus;
    }
}
