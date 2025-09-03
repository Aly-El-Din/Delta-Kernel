package org.example;

import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.defaults.internal.parquet.ParquetFileReader.BatchReadSupport;
import io.delta.kernel.internal.deletionvectors.RoaringBitmapArray;
import io.delta.kernel.types.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;


import java.io.File;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;
import java.util.stream.IntStream;


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
        try (ParquetFileReader reader = ParquetFileReader.open(HadoopInputFile.fromPath(new Path(filePath), conf))) {
            MessageType parquetSchema = reader.getFooter().getFileMetaData().getSchema();

            BatchReadSupport readSupport = new BatchReadSupport(
                    (int) this.rowCount,
                    physicalSchema
            );

            PageReadStore pages = reader.readRowGroup(rowGroupIndex);
            RecordMaterializer<Object> recordMaterializer = readSupport.prepareForRead(
                    conf, Collections.emptyMap(), parquetSchema, readSupport.init(
                            new InitContext(conf, Collections.emptyMap(), parquetSchema)
                    ));

            MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(parquetSchema);
            RecordReader<Object> recordReader = columnIO.getRecordReader(pages, recordMaterializer);

            for (int i = 0; i < this.rowCount; i++) {
                recordReader.read();
                long globalRowIndex = this.startRowIndex + i;
                readSupport.finalizeCurrentRow(globalRowIndex);
            }

            ColumnarBatch batch = readSupport.getDataAsColumnarBatch((int) this.rowCount);
            System.out.printf("Thread %s read %d rows from row group %d into memory.\n",
                    Thread.currentThread().getName(), batch.getSize(), rowGroupIndex);

            File outputFile = new File(Main.outputPath, "file-rowgroup-" + rowGroupIndex + Thread.currentThread().getId() +".parquet");
            Path outputPath = new Path(outputFile.getAbsolutePath());

            int validRowsWritten = 0;
            MessageType outputSchema = convertPhysicalSchemaToParquetSchema(physicalSchema);

            try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(outputPath)
                    .withType(outputSchema)
                    .withConf(conf)
                    .withCompressionCodec(CompressionCodecName.SNAPPY)
                    .withPageSize(ParquetWriter.DEFAULT_PAGE_SIZE)
                    .withRowGroupSize(ParquetWriter.DEFAULT_BLOCK_SIZE)
                    .build()) {

                for (int i = 0; i < batch.getSize(); i++) {
                    long globalRowIndex = this.startRowIndex + i;
                    if (deletionVector != null && deletionVector.contains(globalRowIndex)) {
                        continue;
                    }

                    Group group = new SimpleGroup(outputSchema);
                    for (int j = 0; j < physicalSchema.length(); j++) {
                        StructField field = physicalSchema.at(j);

                        if (field.isMetadataColumn()) {
                            continue;
                        }

                        ColumnVector cv = batch.getColumnVector(j);
                        writeValueToGroup(group, field.getName(), cv, i, field.getDataType());
                    }

                    writer.write(group);
                    validRowsWritten++;
                }
            }
            System.out.printf("Thread %s finished row group %d, wrote %d valid rows to %s.\n",
                    Thread.currentThread().getName(), rowGroupIndex, validRowsWritten, outputFile.getName());
        }
        return Collections.emptyList();
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
            throw new RuntimeException("Thread " + Thread.currentThread().getId() + " Error writing field " + fieldName + ": " + e.getMessage(), e);
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

    private String buildRowString(int rowIndex, ColumnarBatch batch) {
        return IntStream.range(0, physicalSchema.length())
                .mapToObj(i -> {
                    StructField field = physicalSchema.at(i);
                    ColumnVector vector = batch.getColumnVector(i);
                    if (vector.isNullAt(rowIndex)) {
                        return field.getName() + ": NULL";
                    }
                    Object value;
                    DataType dataType = field.getDataType();
                    if (dataType instanceof StringType) value = vector.getString(rowIndex);
                    else if (dataType instanceof IntegerType) value = vector.getInt(rowIndex);
                    else if (dataType instanceof LongType) value = vector.getLong(rowIndex);
                    else if (dataType instanceof DoubleType) value = vector.getDouble(rowIndex);
                    else if (dataType instanceof DecimalType) value = vector.getDecimal(rowIndex);
                    else if (dataType instanceof BooleanType) value = vector.getBoolean(rowIndex);
                    else if (dataType instanceof DateType) value = vector.getInt(rowIndex);
                    else if (dataType instanceof TimestampType) value = vector.getLong(rowIndex);
                    else value = "UNSUPPORTED_TYPE";
                    return field.getName() + ": " + value;
                })
                .collect(Collectors.joining(", "));
    }
}