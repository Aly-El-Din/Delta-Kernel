package org.example;

import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.utils.CloseableIterator;
import org.apache.hadoop.conf.Configuration;

import javax.xml.validation.Schema;

public class MyParquetFileReader {
    private Configuration hadoopConfig;
    public MyParquetFileReader(Configuration hadoopConfig) {
        this.hadoopConfig = hadoopConfig;
    }
    /*public CloseableIterator<ColumnarBatch> read(
            String path,
            Schema schema
    )*/
}
