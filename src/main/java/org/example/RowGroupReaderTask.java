package org.example;

import io.delta.kernel.internal.deletionvectors.RoaringBitmapArray;
import io.delta.kernel.types.StructType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;


public class RowGroupReaderTask implements Callable<List<Object>> {
    private final String filePath;
    private final Configuration conf;
    private final StructType physicalSchema;
    private final int rowGroupIndex;
    private final long startRowIndex;
    private final long rowCount;
    private final RoaringBitmapArray deletionVector;

    public RowGroupReaderTask(String filePath, Configuration conf, StructType physicalSchema, int rowGroupIndex, long startRowIndex, long rowCount, RoaringBitmapArray deletionVector) {
        this.filePath = filePath;
        this.conf = conf;
        this.physicalSchema = physicalSchema;
        this.rowGroupIndex = rowGroupIndex;
        this.startRowIndex = startRowIndex;
        this.rowCount = rowCount;
        this.deletionVector = deletionVector;
    }
    @Override
    public List<Object> call() throws Exception {
        List<Object> rows = new ArrayList<>();
        // Each thread opens its own reader to be thread-safe
        try (ParquetFileReader reader = ParquetFileReader.open(HadoopInputFile.fromPath(new Path(filePath), conf))) {
            MessageType parquetSchema = reader.getFooter().getFileMetaData().getSchema();
            PageReadStore pages = reader.readRowGroup(rowGroupIndex);

            int maxBatchSize = conf.getInt("delta.kernel.default.parquet.reader.batch-size", 1024);
            var readSupport = new io.delta.kernel.defaults.internal.parquet.ParquetFileReader.BatchReadSupport(maxBatchSize, physicalSchema);

            InitContext initContext = new InitContext(conf, Collections.emptyMap(), parquetSchema);
            ReadSupport.ReadContext readContext = readSupport.init(initContext);

            RecordMaterializer<Object> recordMaterializer = readSupport.prepareForRead(
                    conf, Collections.emptyMap(), parquetSchema, readContext);

            MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(parquetSchema);
            RecordReader<Object> recordReader = columnIO.getRecordReader(pages, recordMaterializer);

            for (int i = 0; i < this.rowCount; i++) {
                Object row = recordReader.read();
                if (deletionVector != null) {
                    long globalRowIndex = this.startRowIndex + i;
                    if (deletionVector.contains(globalRowIndex)) {
                        continue; // Skip deleted row
                    }
                }
                rows.add(row);
            }
        }
        System.out.printf("Nested thread %s finished row group %d, read %d valid rows.\n", Thread.currentThread().getName(), rowGroupIndex, rows.size());
        return rows;
    }
}
