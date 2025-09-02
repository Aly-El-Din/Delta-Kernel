package org.example;

import io.delta.kernel.defaults.internal.parquet.ParquetFileReader;
import io.delta.kernel.internal.deletionvectors.RoaringBitmapArray;
import io.delta.kernel.types.StructType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.parquet.hadoop.ParquetRecordReader;
import org.apache.parquet.hadoop.ParquetRecordReaderWrapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

import static io.delta.kernel.internal.util.Preconditions.checkArgument;

public class ParquetSplitReaderTask implements Callable<List<Object>> {
    private final Configuration hadoopConfig;
    private final InputSplit split;
    private final RoaringBitmapArray deletionVector;
    private final boolean hasRowIndexCol;
    private final int maxBatchSize;
    private ParquetFileReader.BatchReadSupport readSupport;

    public ParquetSplitReaderTask(
            Configuration hadoopConfig,
            InputSplit split,
            RoaringBitmapArray deletionVector,
            boolean hasRowIndexCol) {
        this.hadoopConfig = hadoopConfig;
        this.split = split;
        this.deletionVector = deletionVector;
        this.hasRowIndexCol = hasRowIndexCol;
        this.maxBatchSize =
                hadoopConfig.getInt("delta.kernel.default.parquet.reader.batch-size", 1024);
        checkArgument(maxBatchSize > 0, "invalid Parquet reader batch size: " + maxBatchSize);
    }
    @Override
    public List<Object> call() throws Exception {
        System.out.println("Task started on thread: " + Thread.currentThread().getName() + " for split: " + split);
        List<Object> rowGroup = new ArrayList<>();

        TaskAttemptContext context = new TaskAttemptContextImpl(hadoopConfig, new TaskAttemptID());
        ParquetInputFormat<Object> parquetInputFormat = new ParquetInputFormat<>();
        RecordReader<Void, Object> reader = parquetInputFormat.createRecordReader(split, context);
        ParquetRecordReader<Object> parquetRecordReader = (ParquetRecordReader<Object>) reader;

        try {
            parquetRecordReader.initialize(split, context);
            while(parquetRecordReader.nextKeyValue()){
                Object row = parquetRecordReader.getCurrentValue();
                if(row == null) {
                    continue;
                }
                if(deletionVector != null) {
                    boolean rowDeleted = deletionVector.contains(parquetRecordReader.getCurrentRowIndex());
                    if(!rowDeleted) {
                        rowGroup.add(row);
                    }
                }
                else{
                    rowGroup.add(row);
                }
            }
        } catch (IOException | InterruptedException e) {
            System.err.println("Error in ParquetSplitReaderTask on thread " + Thread.currentThread().getName());
            Thread.currentThread().interrupt();
            throw new Exception("Failed to read Parquet split.", e);
        } finally {
            if (parquetRecordReader != null) {
                try {
                    parquetRecordReader.close();
                } catch (IOException e) {
                    System.err.println("Failed to close Parquet reader: " + e.getMessage());
                }
            }
        }
        System.out.println("Task finished on thread: " + Thread.currentThread().getName() + ", rows read in row goup: " + rowGroup.size());
        return rowGroup;
    }
}
