package org.example;

import io.delta.kernel.data.Row;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.internal.InternalScanFileUtils;
import io.delta.kernel.internal.actions.DeletionVectorDescriptor;
import io.delta.kernel.internal.deletionvectors.DeletionVectorUtils;
import io.delta.kernel.internal.deletionvectors.RoaringBitmapArray;
import io.delta.kernel.utils.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.util.HadoopInputFile;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.*;

import static org.example.Main.hadoopConfig;
import static org.example.Main.totalNumberOfRowsRead;

public class Actor3 extends Thread {
    private final FileStatus fileStatus;
    private final Engine engine;
    private final Row scanFile;
    private final Optional<Predicate> predicate;
    public Actor3(FileStatus fileStatus, Engine engine, Row scanFile,
                  Optional<Predicate> predicate) {
        this.fileStatus = fileStatus;
        this.engine = engine;
        this.scanFile = scanFile;
        this.predicate = predicate;
    }

    @Override
    public void run() {
        System.out.println("Thread: " + currentThread().getName() + " started processing file: " + fileStatus.getPath());

        String filePath = fileStatus.getPath();

        try {
            RoaringBitmapArray deletionVector = null;
            DeletionVectorDescriptor dv = InternalScanFileUtils.getDeletionVectorDescriptorFromRow(scanFile);
            if (dv != null) {
                System.out.println("  - Deletion Vector found in " + currentThread().getName() + ", loading it.");
                deletionVector = DeletionVectorUtils.loadNewDvAndBitmap(engine, Main.tablePath, dv)._2;
            }

            List<BlockMetaData> rowGroups;
            try (ParquetFileReader reader = ParquetFileReader.open(HadoopInputFile.fromPath(new Path(filePath), hadoopConfig))) {
                rowGroups = reader.getRowGroups();
            }
            System.out.printf("File %s has %d row groups. Spawning nested threads.\n", fileStatus.getPath(), rowGroups.size());
            if (rowGroups.isEmpty()) return;

            int numThreads = Math.min(rowGroups.size(), Runtime.getRuntime().availableProcessors());
            ExecutorService executor = Executors.newFixedThreadPool(numThreads);//TODO: queue of size 10 (TBD)
            List<Future<List<Object>>> futures = new ArrayList<>();
            long startingRowIndex = 0;

            for (int i = 0; i < rowGroups.size(); i++) {
                BlockMetaData group = rowGroups.get(i);
                long rowCountInGroup = group.getRowCount();
                Callable<List<Object>> task = new RowGroupReaderTask(
                        filePath,
                        i,
                        startingRowIndex,
                        rowCountInGroup,
                        deletionVector
                );
                futures.add(executor.submit(task));
                startingRowIndex += rowCountInGroup;
            }

            List<Object> memory = new ArrayList<>();
            for (Future<List<Object>> future : futures) {
                List<Object> rowGroup = future.get();
                totalNumberOfRowsRead.addAndGet(rowGroup.size());
                memory.addAll(rowGroup);
            }
            executor.shutdown();

            System.out.printf("Thread: %s FINISHED. Total valid rows read from file %s: %d\n",
                    currentThread().getName(), fileStatus.getPath(), memory.size());

        } catch (IOException | ExecutionException | InterruptedException e) {
            System.err.println("Error processing file in thread " + currentThread().getName());
            e.printStackTrace();
            Thread.currentThread().interrupt();
        }
    }
}