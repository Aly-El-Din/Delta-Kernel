package org.example;

import io.delta.kernel.data.ColumnarBatch;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.internal.util.Utils;
import io.delta.kernel.types.FieldMetadata;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

import java.io.IOException;
import java.util.*;

public class MyParquetHandler {
    private Configuration hadoopConfig;
    private Engine engine;
    public MyParquetHandler(Configuration hadoopConfig, Engine engine){
        this.hadoopConfig = hadoopConfig;
        this.engine = engine;
    }

    private static class SchemaMapping{
        private final Map<StructField, Type> mappings = new HashMap<>();

        public void addMapping(StructField deltaField, Type parquetField) {
            mappings.put(deltaField, parquetField);
        }

        public Type getMapping(StructField deltaField) {
            return mappings.get(deltaField);
        }

        public Map<StructField, Type> getMappings() {
            return mappings;
        }
    }
    /*
    * Open parquet file stream
    * Read schema and metadata
    * Prune unnecessary columns (Per physical read schema)
    * Predicate -> filter row groups/pages
    * Load columnar chunks into memory
    * Wrap in column vectors
    * Yield columnar batches
    * */
    public CloseableIterator<ColumnarBatch> readParquetFiles(
            CloseableIterator<FileStatus> filesStatusItr,
            StructType physicalSchema,
            Optional<Predicate> predicate
    ) throws IOException {
        return new CloseableIterator<ColumnarBatch>() {
            private final MyParquetFileReader batchReader = new MyParquetFileReader(hadoopConfig);
            private CloseableIterator<ColumnarBatch> currentFileReader;
            @Override
            public void close() throws IOException {

            }

            @Override
            public boolean hasNext() {
                if(currentFileReader != null || currentFileReader.hasNext()){
                    return true;
                }
                else{
                    Utils.closeCloseables(currentFileReader);//Close currentFileReader because there may be no data to be loaded
                    currentFileReader = null;
                    if(filesStatusItr.hasNext()){
                        //Get next file and define its file reader
                        String nextFilePath = filesStatusItr.next().getPath();
                        //currentFileReader =
                    }
                }
                return false;
            }

            @Override
            public ColumnarBatch next() {
                return null;
            }
        };


    }
    private SchemaMapping createSchemaMapping (StructType logicalSchema, MessageType parquetSchema) {
        SchemaMapping mapping = new SchemaMapping();
        for(StructField deltaField:logicalSchema.fields()) {
            Type parquetField = mapLogicalFieldToParquetField(deltaField, parquetSchema);
            if(parquetField!=null){
                mapping.addMapping(deltaField, parquetField);
            }
        }
        return mapping;
    }
    //Try finding physical parquet field from the delta logical field using mapping strategy
    //Finding by id if failed move to name matching
    private Type mapLogicalFieldToParquetField(StructField deltaField, MessageType parquetSchema) {
        String fieldName = deltaField.getName();
        FieldMetadata metadata = deltaField.getMetadata();

        //If column mapping is by id
        if(metadata.contains("parquet.field.id")){
            int fieldId = Integer.parseInt(metadata.get("parquet.field.id").toString());
            Type field = findFieldById(parquetSchema, fieldId);
            if(field!=null){
                return field;
            }
        }
        //If column mapping is by name
        if(parquetSchema.containsField(fieldName)){
            return parquetSchema.getType(fieldName);
        }
        //TODO:Case sesnsitive handling
        return null;
    }

    private Type findFieldById(MessageType schema, int fieldId) {
        for(Type field:schema.getFields()){
            if(field.getId()!=null && field.getId().intValue() == fieldId){
                return field;
            }
        }
        return null;
    }

    private MessageType projectSchema(MessageType originalSchema, SchemaMapping mapping) {
        List<Type> projectedFields = new ArrayList<>();
        for(Map.Entry<StructField, Type> entry:mapping.getMappings().entrySet()){
            projectedFields.add(entry.getValue());
        }
        return new MessageType(originalSchema.getName(), projectedFields);
    }
}
