package org.example;

import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.internal.parquet.ParquetFileReader;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.internal.InternalScanFileUtils;
import io.delta.kernel.internal.actions.DeletionVectorDescriptor;
import io.delta.kernel.internal.deletionvectors.DeletionVectorUtils;
import io.delta.kernel.internal.deletionvectors.RoaringBitmapArray;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.*;

import static io.delta.kernel.defaults.internal.parquet.ParquetFilterUtils.toParquetFilter;
import static io.delta.kernel.internal.util.Preconditions.checkArgument;
import static org.apache.parquet.hadoop.ParquetInputFormat.*;
import static org.apache.parquet.hadoop.ParquetInputFormat.COLUMN_INDEX_FILTERING_ENABLED;

public class MyParquetReader {
    private final Configuration hadoopConf;
    private final StructType physicalSchema;
    private final int maxBatchSize;
    private Optional<Predicate> predicate;
    public MyParquetReader(Configuration hadoopConf, StructType physicalSchema, Optional<Predicate> predicate) {
        this.hadoopConf = hadoopConf;
        this.physicalSchema = physicalSchema;
        this.maxBatchSize =
                hadoopConf.getInt("delta.kernel.default.parquet.reader.batch-size", 1024);
        checkArgument(maxBatchSize > 0, "invalid Parquet reader batch size: " + maxBatchSize);
        this.predicate = predicate;
    }
    public void readParquetFile(
        String path,
        Engine engine,
        Row scanFile,
        String tablePath
    ) throws IOException, InterruptedException {

        final boolean hasRowIndexCol =
                physicalSchema.indexOf(StructField.METADATA_ROW_INDEX_COLUMN_NAME) >= 0 &&
                        physicalSchema.get(StructField.METADATA_ROW_INDEX_COLUMN_NAME).isMetadataColumn();

        DeletionVectorDescriptor dv =
                InternalScanFileUtils.getDeletionVectorDescriptorFromRow(scanFile);

        RoaringBitmapArray deletionVector = null;

        if(dv != null){
            if (!hasRowIndexCol) {
                throw new IllegalArgumentException("Row index column is not " +
                        "present in the data read from the Parquet file.");
            }
             deletionVector = DeletionVectorUtils.loadNewDvAndBitmap(engine, tablePath, dv)._2;
        }

        // Setup Hadoop Job and ParquetInputFormat to get splits (row groups)
        Job job = Job.getInstance(hadoopConf);
        Configuration jobConf = job.getConfiguration();

        ParquetInputFormat.setReadSupportClass(job, ParquetFileReader.BatchReadSupport.class);
        String schemaJson = physicalSchema.toJson();
        jobConf.set("delta.kernel.default.parquet.read.schema", schemaJson);

        FileInputFormat.addInputPath(job, new Path(URI.create(path)));
        // Configure predicate pushdown if a predicate exists
        configurePredicatePushdown(job, path);

        ParquetInputFormat<Object> parquetInputFormat = new ParquetInputFormat<>();
        List<InputSplit> splits = parquetInputFormat.getSplits(job);
        int numThreads = Math.min(splits.size(), Runtime.getRuntime().availableProcessors());
        if (numThreads <= 0) {
            numThreads = 1;
        }
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        List<Future<List<Object>>> futures = new ArrayList<>();
        System.out.println("Reading file " + path + " with " + splits.size() + " splits (row groups) using " + numThreads + " threads.");

        for (InputSplit split : splits) {
            Callable<List<Object>> task = new ParquetSplitReaderTask(
                    jobConf,
                    split,
                    deletionVector,
                    hasRowIndexCol
            );
            futures.add(executor.submit(task));
        }

        List<Object> memory = new ArrayList<>();
        try {
            for (Future<List<Object>> future : futures) {
                memory.addAll(future.get());
            }
        } catch (InterruptedException | ExecutionException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Error reading Parquet file concurrently", e);
        } finally {
            executor.shutdown();
        }
        System.out.println("Thread: " + Thread.currentThread().getName() + " finished reading " + path + ". Total rows read: " + memory.size());
    }

    private void configurePredicatePushdown(Job job, String path) throws IOException {
        Configuration conf = job.getConfiguration();
        Path filePath = new Path(URI.create(path));

        ParquetMetadata footer = org.apache.parquet.hadoop.ParquetFileReader.readFooter(conf, filePath);
        MessageType parquetSchema = footer.getFileMetaData().getSchema();
        Optional<FilterPredicate> parquetPredicate = predicate.flatMap(
                p -> toParquetFilter(parquetSchema, p));

        if (parquetPredicate.isPresent()) {
            setFilterPredicate(conf, parquetPredicate.get());
            conf.set(RECORD_FILTERING_ENABLED, "false");
            conf.set(DICTIONARY_FILTERING_ENABLED, "false");
            conf.set(COLUMN_INDEX_FILTERING_ENABLED, "false");
        }
    }

}


