package org.example;

import io.delta.kernel.internal.deletionvectors.RoaringBitmapArray;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.example.GroupReadSupport;
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

import static org.example.Main.*;


public class RowGroupReaderTask implements Callable<List<Object>> {
    private final String filePath;
    private final int rowGroupIndex;
    private final long startRowIndex;
    private final long rowCount;
    private final RoaringBitmapArray deletionVector;
    private final Configuration conf;
    public RowGroupReaderTask(String filePath, int rowGroupIndex, long startRowIndex, long rowCount,
                              RoaringBitmapArray deletionVector, Configuration conf) {
        this.filePath = filePath;
        this.rowGroupIndex = rowGroupIndex;
        this.startRowIndex = startRowIndex;
        this.rowCount = rowCount;
        this.deletionVector = deletionVector;
        this.conf = conf;
    }

    @Override
    public List<Object> call() throws Exception {
        return read();
    }
    private List<Object> read() throws IOException {
        List<Object> rows = new ArrayList<>();
        try (ParquetFileReader reader = ParquetFileReader.open(HadoopInputFile.fromPath(new Path(filePath), conf))) {
            MessageType parquetSchema = physicalSchemaForAllParquetFiles;

            PageReadStore pages = reader.readRowGroup(rowGroupIndex);

            GroupReadSupport readSupport = new GroupReadSupport();
            ReadSupport.ReadContext readContext = readSupport.init(
                    new InitContext(conf, Collections.emptyMap(), parquetSchema)
            );

            RecordMaterializer<Group> recordMaterializer = readSupport.prepareForRead(
                    conf, Collections.emptyMap(), parquetSchema, readContext);

            MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(parquetSchema);
            RecordReader<Group> recordReader = columnIO.getRecordReader(pages, recordMaterializer);

            for (int i = 0; i < this.rowCount; i++) {
                Group row = recordReader.read();
                if (deletionVector != null) {
                    long globalRowIndex = this.startRowIndex + i;
                    if (deletionVector.contains(globalRowIndex)) {
                        continue;
                    }
                }
                /*System.out.println("Thread " + Thread.currentThread().getId() +
                        " read valid row: " + row.toString().replace("\n", " | "));*/
                rows.add(row);
            }
        }
        /*System.out.printf("Nested thread %s finished row group %d, read %d valid rows.\n", Thread.currentThread().getName(),
                rowGroupIndex, rows.size());*/
        return rows;
    }
}
