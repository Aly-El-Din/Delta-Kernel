package org.example;

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.internal.parquet.ParquetFileReader;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.internal.InternalScanFileUtils;
import io.delta.kernel.internal.actions.DeletionVectorDescriptor;
import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.hadoop.ParquetRecordReaderWrapper;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;

import static io.delta.kernel.defaults.internal.parquet.ParquetFilterUtils.toParquetFilter;
import static io.delta.kernel.internal.util.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static org.apache.parquet.hadoop.ParquetInputFormat.*;
import static org.apache.parquet.hadoop.ParquetInputFormat.COLUMN_INDEX_FILTERING_ENABLED;

public class MyParquetReader {
    private final Configuration hadoopConf;
    private final StructType physicalSchema;
    private final int maxBatchSize;
    private boolean hasNotConsumedNextElement;
    private ParquetRecordReaderWrapper<Object> reader;
    private Optional<Predicate> predicate;
    private ParquetFileReader.BatchReadSupport readSupport;
    public MyParquetReader(Configuration hadoopConf, StructType physicalSchema, Optional<Predicate> predicate) {
        this.hadoopConf = hadoopConf;
        this.physicalSchema = physicalSchema;
        this.maxBatchSize =
                hadoopConf.getInt("delta.kernel.default.parquet.reader.batch-size", 1024);
        checkArgument(maxBatchSize > 0, "invalid Parquet reader batch size: " + maxBatchSize);
        this.predicate = predicate;
    }
    /*private Object extractValue(Group group, String fieldName, DataType dataType) {
    }
    private Row convertParquetGroupToRow(Group parquetGroup) {
        Object[] values = new Object[physicalSchema.fields().size()];
        for(int i=0; i<physicalSchema.length(); i++) {
            StructField field = physicalSchema.fields().get(i);
            String fieldName = field.getName();
            DataType fieldType = field.getDataType();

            if(parquetGroup.getFieldRepetitionCount(fieldName) > 0) {
                values[i] =
            }
        }
    }
    private Row convertToRow(Object currentValue) {
        if (currentValue instanceof Group) {

        }
    }*/
    private boolean checkNextElementConsumed(String path){
        initParquetReaderIfRequired(path);
        try {
            if (hasNotConsumedNextElement) {
                return true;
            }

            hasNotConsumedNextElement = reader.nextKeyValue() &&
                    reader.getCurrentValue() != null;
            return hasNotConsumedNextElement;
        } catch (IOException | InterruptedException ie) {
            throw new RuntimeException(ie);
        }
    }
    public void readParquetFile(
        String path
    ) throws IOException, InterruptedException {
        readSupport = new ParquetFileReader.BatchReadSupport(maxBatchSize, physicalSchema);

        final boolean hasRowIndexCol =
                physicalSchema.indexOf(StructField.METADATA_ROW_INDEX_COLUMN_NAME) >= 0 &&
                        physicalSchema.get(StructField.METADATA_ROW_INDEX_COLUMN_NAME).isMetadataColumn();

        List<Object> rows = new ArrayList<>();

        checkNextElementConsumed(path);

        if (!hasNotConsumedNextElement) {
            throw new NoSuchElementException();
        }
        int batchSize = 0;
        do {
            hasNotConsumedNextElement = false;
            // hasNext reads to row to confirm there is a next element.
            // get the row index only if required by the read schema
            long rowIndex = hasRowIndexCol ? reader.getCurrentRowIndex() : -1;
            Object row = reader.getCurrentValue();
            //System.out.println("Row: "+row);
            rows.add(row);
            /*if(rowIndex != -1){
                Object row = reader.getCurrentValue();
                System.out.println("Row: "+row);

                rows.add(row);
            }*/
            batchSize++;
        } while (batchSize < maxBatchSize && checkNextElementConsumed(path));
        System.out.println("Batch rows:");
        int c = 0;

        System.out.println("number of rows read: "+rows.size());
    }
    private void initParquetReaderIfRequired(String path) {
        if (reader == null) {
            org.apache.parquet.hadoop.ParquetFileReader fileReader = null;
            try {
                Configuration confCopy = hadoopConf;
                Path filePath = new Path(URI.create(path));

                // We need physical schema in order to construct a filter that can be
                // pushed into the `parquet-mr` reader. For that reason read the footer
                // in advance.
                ParquetMetadata footer =
                        org.apache.parquet.hadoop.ParquetFileReader.readFooter(
                                confCopy,
                                filePath);

                MessageType parquetSchema = footer.getFileMetaData().getSchema();
                Optional<FilterPredicate> parquetPredicate = predicate.flatMap(
                        predicate -> toParquetFilter(parquetSchema, predicate));

                if (parquetPredicate.isPresent()) {
                    // clone the configuration to avoid modifying the original one
                    confCopy = new Configuration(confCopy);

                    setFilterPredicate(confCopy, parquetPredicate.get());
                    // Disable the record level filtering as the `parquet-mr` evaluates
                    // the filter once the entire record has been materialized. Instead,
                    // we use the predicate to prune the row groups which is more efficient.
                    // In the future, we can consider using the record level filtering if a
                    // native Parquet reader is implemented in Kernel default module.
                    confCopy.set(RECORD_FILTERING_ENABLED, "false");
                    confCopy.set(DICTIONARY_FILTERING_ENABLED, "false");
                    confCopy.set(COLUMN_INDEX_FILTERING_ENABLED, "false");
                }

                // Pass the already read footer to the reader to avoid reading it again.
                fileReader = new MyParquetReader.ParquetFileReaderWithFooter(filePath, confCopy, footer);
                reader = new ParquetRecordReaderWrapper<>(readSupport);
                reader.initialize(fileReader, confCopy);
            } catch (IOException e) {
                Utils.closeCloseablesSilently(fileReader, reader);
                throw new UncheckedIOException(e);
            }
        }
    }
    private static class ParquetFileReaderWithFooter
            extends org.apache.parquet.hadoop.ParquetFileReader {
        private final ParquetMetadata footer;

        ParquetFileReaderWithFooter(
                Path filePath,
                Configuration configuration,
                ParquetMetadata footer) throws IOException {
            super(configuration, filePath, footer);
            this.footer = requireNonNull(footer, "footer is null");
        }

        @Override
        public ParquetMetadata getFooter() {
            return footer;  // return the footer passed in the constructor
        }
    }
}


