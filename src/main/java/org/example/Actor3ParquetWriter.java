package org.example;

import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.ColumnVector;
import io.delta.kernel.types.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import java.util.Optional;

public class Actor3ParquetWriter {
    private final StructType schema;
    private final int threadId;

    public Actor3ParquetWriter(StructType schema, int threadId) {
        this.schema = schema;
        this.threadId = threadId;
    }

    private MessageType convertDeltaSchemaToParquetSchema(StructType deltaSchema) {
        Types.MessageTypeBuilder builder = Types.buildMessage();

        for (int i = 0; i < deltaSchema.length(); i++) {
            StructField field = deltaSchema.at(i);
            String fieldName = field.getName();
            DataType dataType = field.getDataType();
            boolean isOptional = field.isNullable();

            org.apache.parquet.schema.Type.Repetition repetition =
                    isOptional ? org.apache.parquet.schema.Type.Repetition.OPTIONAL :
                            org.apache.parquet.schema.Type.Repetition.REQUIRED;

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
        return builder.named("DeltaRecord");
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
    public void writeParquet(List<FilteredColumnarBatch> batches) throws IOException {
        if (batches.isEmpty()) {
            System.out.println("Thread " + threadId + ": No batches to write");
            return;
        }
        File outputDir = new File(Main.outputDirectoryPath);
        if (!outputDir.exists()) {
            boolean created = outputDir.mkdirs();
            System.out.println("Thread " + threadId + ": Directory created: " + created);
        }
        File outputFile = new File(outputDir, "file-" + threadId + ".parquet");
        Path outputPath = new Path(outputFile.getAbsolutePath());
        ColumnarBatch firstBatch = batches.get(0).getData();
        StructType deltaSchema = firstBatch.getSchema();
        MessageType parquetSchema = convertDeltaSchemaToParquetSchema(deltaSchema);
        //Setting up hadoop configurations
        Configuration conf = new Configuration();

        // Configure write support
        GroupWriteSupport.setSchema(parquetSchema, conf);

        // Create ParquetWriter with configs
        try (ParquetWriter<Group> writer = org.apache.parquet.hadoop.example.ExampleParquetWriter.builder(outputPath)
                .withConf(conf)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .withPageSize(1024 * 1024)
                .withRowGroupSize(128 * 1024 * 1024)
                .build()) {

            SimpleGroupFactory groupFactory = new SimpleGroupFactory(parquetSchema);

            int totalRowsWritten = 0;

            // Process each batch
            for (FilteredColumnarBatch filteredBatch : batches) {
                ColumnarBatch batch = filteredBatch.getData();
                Optional<ColumnVector> selectionVector = filteredBatch.getSelectionVector();
                //Same logic as reading delta files
                boolean dvExist = selectionVector.isPresent();
                int numRows = batch.getSize();
                int numCols = batch.getSchema().length();


                System.out.println("Thread " + threadId + ": Processing batch with " + numRows + " rows, " + numCols + " columns");

                // Write each row
                for (int rowIndex = 0; rowIndex < numRows; rowIndex++) {
                    // Check if row is selected (if deletion vector exists)
                    boolean rowSelected = true;
                    if (dvExist) {
                        ColumnVector dv = selectionVector.get();
                        rowSelected = (!dv.isNullAt(rowIndex) && dv.getBoolean(rowIndex));
                    }
                    if (rowSelected) {
                        Group group = groupFactory.newGroup();
                        for (int colIndex = 0; colIndex < numCols; colIndex++) {
                            //Writing value to row group
                            StructField field = deltaSchema.at(colIndex);
                            String fieldName = field.getName();
                            DataType dataType = field.getDataType();
                            ColumnVector column = batch.getColumnVector(colIndex);

                            writeValueToGroup(group, fieldName, column, rowIndex, dataType);
                        }

                        writer.write(group);
                        totalRowsWritten++;
                    }
                }
            }

            System.out.println("Thread " + threadId + ": Finished writing " + totalRowsWritten + " rows");
        }
    }

}
