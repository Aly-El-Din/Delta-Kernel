package org.example;

import io.delta.kernel.data.Row;
import io.delta.kernel.engine.Engine;
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
import java.util.concurrent.*;

import static org.example.Main.hadoopConfig;
import static org.example.Main.totalNumberOfRowsRead;

public class Actor3 extends Thread {
    private final FileStatus fileStatus;
    private final Engine engine;
    private final Row scanFile;
    public Actor3(FileStatus fileStatus, Engine engine, Row scanFile) {
        this.fileStatus = fileStatus;
        this.engine = engine;
        this.scanFile = scanFile;
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
            ExecutorService executor = Executors.newFixedThreadPool(numThreads);
            //List<Future<List<Object>>> futures = new ArrayList<>();
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
                //futures.add(executor.submit(task));
                executor.submit(task);
                startingRowIndex += rowCountInGroup;
            }

            //List<Object> memory = new ArrayList<>();
           /*for (Future<List<Object>> future : futures) {
                List<Object> rowGroup = future.get();
                totalNumberOfRowsRead.addAndGet(rowGroup.size());
                memory.addAll(rowGroup);
            }*/
            executor.shutdown();
            try {
                // CRITICAL FIX: Block this Actor3 thread until all its RowGroupReaderTasks
                // have completed, or until a timeout is reached.
                if (!executor.awaitTermination(1, TimeUnit.HOURS)) {
                    System.err.println("Executor for file " + filePath + " did not terminate in the specified time.");
                    executor.shutdownNow();
                }
            } catch (InterruptedException e) {
                System.err.println("Thread was interrupted while waiting for executor to terminate.");
                executor.shutdownNow();
                Thread.currentThread().interrupt();
            }

            System.out.printf("Thread: %s FINISHED. Rows read from file %s: \n",
                    currentThread().getName(), fileStatus.getPath());

        } catch (IOException e) {
            System.err.println("Error processing file in thread " + currentThread().getName());
            e.printStackTrace();
            Thread.currentThread().interrupt();
        }
    }
}