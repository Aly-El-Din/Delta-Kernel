package org.example;

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.internal.parquet.ParquetFileReader;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.internal.InternalScanFileUtils;
import io.delta.kernel.internal.actions.DeletionVectorDescriptor;
import io.delta.kernel.internal.deletionvectors.DeletionVectorUtils;
import io.delta.kernel.internal.deletionvectors.RoaringBitmapArray;
import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.types.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.hadoop.ParquetRecordReaderWrapper;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.*;


import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URI;
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
    private  int totalRowsRead = 0;
    private long threadId;
    public MyParquetReader(Configuration hadoopConf, StructType physicalSchema,
                           Optional<Predicate> predicate, long threadId) {
        this.hadoopConf = hadoopConf;
        this.physicalSchema = physicalSchema;
        this.maxBatchSize =
                hadoopConf.getInt("delta.kernel.default.parquet.reader.batch-size", 1024);
        checkArgument(maxBatchSize > 0, "invalid Parquet reader batch size: " + maxBatchSize);
        this.predicate = predicate;
        this.threadId = threadId;
    }
    private void readRowByReadSupport() {
        hasNotConsumedNextElement = false;
        long rowIndex = reader.getCurrentRowIndex();
        readSupport.finalizeCurrentRow(rowIndex);
        totalRowsRead++;
    }
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
        String path,
        Engine engine,
        Row scanFile,
        String tablePath
    ) throws IOException, InterruptedException {

        File outputDir = new File(Main.outputDirectoryPath);
        if(!outputDir.exists()) {
            outputDir.mkdirs();
        }
        File outputFile = new File(outputDir, "file-" + threadId + ".parquet");
        Path outputPath = new Path(outputFile.getAbsolutePath());

        readSupport = new ParquetFileReader.BatchReadSupport(maxBatchSize, physicalSchema);

        final boolean hasRowIndexCol =
                physicalSchema.indexOf(StructField.METADATA_ROW_INDEX_COLUMN_NAME) >= 0 &&
                        physicalSchema.get(StructField.METADATA_ROW_INDEX_COLUMN_NAME).isMetadataColumn();

        checkNextElementConsumed(path);

        if (!hasNotConsumedNextElement) {
            throw new NoSuchElementException();
        }

        DeletionVectorDescriptor dv =
                InternalScanFileUtils.getDeletionVectorDescriptorFromRow(scanFile);

        if (dv == null) {
            do {
                readRowByReadSupport();
            } while (checkNextElementConsumed(path));
        } else {
            if (!hasRowIndexCol) {
                throw new IllegalArgumentException("Row index column is not " +
                        "present in the data read from the Parquet file.");
            }
            RoaringBitmapArray actualDeletionVector = DeletionVectorUtils.loadNewDvAndBitmap(engine, tablePath, dv)._2;
            do {
                hasNotConsumedNextElement = false;
                boolean rowDeleted = actualDeletionVector.contains(reader.getCurrentRowIndex());
                if (!rowDeleted) {
                    readRowByReadSupport();
                }
            } while (checkNextElementConsumed(path));
        }

        MessageType parquetSchema = convertPhysicalSchemaToParquetSchema(physicalSchema);

        GroupWriteSupport.setSchema(parquetSchema, hadoopConf);

        try (ParquetWriter<Group> writer = org.apache.parquet.hadoop.example.ExampleParquetWriter.builder(outputPath)
                .withConf(hadoopConf)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .withPageSize(1024 * 1024)
                .withRowGroupSize(128 * 1024 * 1024)
                .build()) {

            SimpleGroupFactory groupFactory = new SimpleGroupFactory(parquetSchema);

            ColumnarBatch batch = readSupport.getDataAsColumnarBatch(totalRowsRead);
            StructType deltaSchema = batch.getSchema();
            for (int i = 0; i < batch.getSize(); i++) {
                Group group = groupFactory.newGroup();
                for (int j = 0; j < deltaSchema.length(); j++) {
                    StructField field = deltaSchema.at(j);
                    if (field.isMetadataColumn()) {
                        continue;
                    }
                    String fieldName = field.getName();
                    DataType dataType = field.getDataType();
                    ColumnVector cv = batch.getColumnVector(j);
                    writeValueToGroup(group, fieldName, cv, i, dataType);
                }
                writer.write(group);
            }
        } catch (Exception e){
            System.out.println("Error while initializing parquet writer");
        }
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
    private Binary encodeDecimal(BigDecimal decimal, int scale, int byteLength) {
        BigDecimal scaledDecimal = decimal.setScale(scale, BigDecimal.ROUND_HALF_UP);
        BigInteger unscaledValue = scaledDecimal.unscaledValue();

        byte[] bytes = unscaledValue.toByteArray();
        byte[] result = new byte[byteLength];
        boolean isNegative = unscaledValue.signum() < 0;
        byte fillByte = isNegative ? (byte) 0xFF : (byte) 0x00;
        for (int i = 0; i < byteLength; i++) {
            result[i] = fillByte;
        }
        System.arraycopy(bytes, 0,result,byteLength-bytes.length, bytes.length);
        return Binary.fromConstantByteArray(result);
    }
    private void writeValueToGroup(Group group, String fieldName, ColumnVector column, int rowIndex, DataType dataType) {
        if (column.isNullAt(rowIndex)) {
            return;
        }
        try {
            if (dataType instanceof StringType) {
                group.add(fieldName, column.getString(rowIndex));
            } else if (dataType instanceof IntegerType) {
                group.add(fieldName, column.getInt(rowIndex));
            } else if (dataType instanceof LongType) {
                group.add(fieldName, column.getLong(rowIndex));
            } else if (dataType instanceof DoubleType) {
                group.add(fieldName, column.getDouble(rowIndex));
            }  else if (dataType instanceof DecimalType) {
                DecimalType decType = (DecimalType) dataType;
                BigDecimal decimal = column.getDecimal(rowIndex);
                int byteLength = decType.getPrecision() <= 18 ? 8 : 16;
                Binary encodedDecimal = encodeDecimal(decimal, decType.getScale(), byteLength);
                group.add(fieldName, encodedDecimal);
            } else if (dataType instanceof BooleanType) {
                group.add(fieldName, column.getBoolean(rowIndex));
            } else if (dataType instanceof TimestampType) {
                long micros = column.getLong(rowIndex);
                group.add(fieldName, micros);
            } else if (dataType instanceof DateType) {
                int days = column.getInt(rowIndex);
                group.add(fieldName, days);
            } else {
                group.add(fieldName, column.toString());
            }
        } catch (Exception e) {
            System.err.println("Thread " + threadId + " Error writing field " + fieldName + ": " + e.getMessage());
        }
    }
    private MessageType convertPhysicalSchemaToParquetSchema(StructType physicalSchema) {
        Types.MessageTypeBuilder builder = Types.buildMessage();
        for (StructField field : physicalSchema.fields()) {
            if (field.isMetadataColumn()) {
                continue;
            }

            String fieldName = field.getName();
            DataType dataType = field.getDataType();
            Type.Repetition repetition = field.isNullable() ? Type.Repetition.OPTIONAL : Type.Repetition.REQUIRED;

            if (dataType instanceof StringType) {
                builder.addField(Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, repetition)
                        .as(org.apache.parquet.schema.LogicalTypeAnnotation.stringType())
                        .named(fieldName));
            } else if (dataType instanceof IntegerType) {
                builder.addField(Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, repetition)
                        .named(fieldName));
            } else if (dataType instanceof LongType) {
                builder.addField(Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, repetition)
                        .named(fieldName));
            } else if (dataType instanceof DoubleType) {
                builder.addField(Types.primitive(PrimitiveType.PrimitiveTypeName.DOUBLE, repetition)
                        .named(fieldName));
            } else if (dataType instanceof DecimalType) {
                DecimalType decType = (DecimalType) dataType;
                int byteLength = decType.getPrecision() <= 18 ? 8 : 16;
                builder.addField(Types.primitive(PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY, repetition)
                        .as(org.apache.parquet.schema.LogicalTypeAnnotation.decimalType(decType.getScale(), decType.getPrecision()))
                        .length(byteLength)
                        .named(fieldName));
            } else if (dataType instanceof BooleanType) {
                builder.addField(Types.primitive(PrimitiveType.PrimitiveTypeName.BOOLEAN, repetition)
                        .named(fieldName));
            } else if (dataType instanceof TimestampType) {
                builder.addField(Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, repetition)
                        .as(org.apache.parquet.schema.LogicalTypeAnnotation.timestampType(true, org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit.MICROS))
                        .named(fieldName));
            } else if (dataType instanceof DateType) {
                builder.addField(Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, repetition)
                        .as(org.apache.parquet.schema.LogicalTypeAnnotation.dateType())
                        .named(fieldName));
            } else {
                builder.addField(Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, repetition)
                        .as(org.apache.parquet.schema.LogicalTypeAnnotation.stringType())
                        .named(fieldName));
            }
        }
        return builder.named("delta_record");

    }
}
