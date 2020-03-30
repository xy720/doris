// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.load.loadv2.dpp;

import org.apache.doris.load.loadv2.etl.EtlJobConfig;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.apache.spark.Partitioner;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.RelationalGroupedDataset;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import scala.Tuple2;
import scala.collection.Seq;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Date;
import java.util.Queue;
import java.util.LinkedList;
import java.util.Iterator;
import java.util.zip.CRC32;

import org.apache.hadoop.fs.FileSystem;
import scala.collection.JavaConverters;

// This class is a Spark-based data preprocessing program,
// which will make use of the distributed compute framework of spark to
// do ETL job/sort/preaggregate/convert data format job in spark job
// to boost the process of large amount of data load.
public final class SparkDpp implements java.io.Serializable {
    private static final String BUCKET_ID = "__bucketId__";
    private EtlJobConfig etlJobConfig = null;

    public SparkDpp(EtlJobConfig etlJobConfig) {
        this.etlJobConfig = etlJobConfig;
    }

    private Class dataTypeToClass(DataType dataType) {
        if (dataType.equals(DataTypes.BooleanType)) {
            return Boolean.class;
        } else if (dataType.equals(DataTypes.ShortType)) {
            return Short.class;
        } else if (dataType.equals(DataTypes.IntegerType)) {
            return Integer.class;
        } else if (dataType.equals(DataTypes.LongType)) {
            return Long.class;
        } else if (dataType.equals(DataTypes.FloatType)) {
            return Float.class;
        } else if (dataType.equals(DataTypes.DoubleType)) {
            return Double.class;
        } else if (dataType.equals(DataTypes.DateType)) {
            return Date.class;
        } else if (dataType.equals(DataTypes.StringType)) {
            return String.class;
        } else if (dataType.equals(DataTypes.ShortType)) {
            return Short.class;
        }
        return null;
    }

    private Class columnTypeToClass(String columnType) {
        switch (columnType) {
            case "BOOL":
                return Boolean.class;
            case "TINYINT":
            case "SMALLINT":
                return Short.class;
            case "INT":
                return Integer.class;
            case "DATETIME":
            case "BIGINT":
            case "LARGEINT":
                return Long.class;
            case "FLOAT":
                return Float.class;
            case "DOUBLE":
                return Double.class;
            case "DATE":
                return Date.class;
            case "HLL":
            case "CHAR":
            case "VARCHAR":
            case "BITMAP":
            case "OBJECT":
                return String.class;
            default:
                return String.class;
        }
    }

    private DataType columnTypeToDataType(String columnType) {
        DataType structColumnType = DataTypes.StringType;
        switch (columnType) {
            case "BOOL":
                structColumnType = DataTypes.BooleanType;
                break;
            case "TINYINT":
            case "SMALLINT":
                structColumnType = DataTypes.ShortType;
                break;
            case "INT":
                structColumnType = DataTypes.IntegerType;
                break;
            case "DATETIME":
            case "BIGINT":
            case "LARGEINT":
                // todo: special treat LARGEINT because spark not support int128
                structColumnType = DataTypes.LongType;
                break;
            case "FLOAT":
                structColumnType = DataTypes.FloatType;
                break;
            case "DOUBLE":
                structColumnType = DataTypes.DoubleType;
                break;
            case "DATE":
                structColumnType = DataTypes.DateType;
                break;
            case "HLL":
            case "CHAR":
            case "VARCHAR":
            case "BITMAP":
            case "OBJECT":
                structColumnType = DataTypes.StringType;
                break;
            default:
                System.out.println("invalid column type:" + columnType);
                break;
        }
        return structColumnType;
    }

    private ByteBuffer getHashValue(Object o, DataType type) {
        ByteBuffer buffer = ByteBuffer.allocate(8);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        if (type.equals(DataTypes.ShortType)) {
            buffer.putShort((Short)o);
        } else if (type.equals(DataTypes.IntegerType)) {
            buffer.putInt((Integer) o);
        } else if (type.equals(DataTypes.LongType)) {
            buffer.putLong((Long)o);
        } else if (type.equals(DataTypes.StringType)) {
            try {
                String str = (String)o;
                buffer = ByteBuffer.wrap(str.getBytes("UTF-8"));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        return buffer;
    }

    private StructType createDstTableSchema(List<EtlJobConfig.EtlColumn> columns, boolean addBucketIdColumn) {
        List<StructField> fields = new ArrayList<>();
        if (addBucketIdColumn) {
            StructField bucketIdField = DataTypes.createStructField(BUCKET_ID, DataTypes.StringType, true);
            fields.add(bucketIdField);
        }
        for (EtlJobConfig.EtlColumn column : columns) {
            String columnName = column.columnName;
            String columnType = column.columnType;
            DataType structColumnType = columnTypeToDataType(columnType);
            StructField field = DataTypes.createStructField(columnName, structColumnType, column.isAllowNull);
            fields.add(field);
        }
        StructType dstSchema = DataTypes.createStructType(fields);
        return dstSchema;
    }

    private Dataset<Row> processDataframeAgg(Dataset<Row> dataframe, EtlJobConfig.EtlIndex indexMeta) {
        List<Column> keyColumns = new ArrayList<>();
        // add BUCKET_ID as a Key
        keyColumns.add(new Column(BUCKET_ID));
        Map<String, String> aggFieldOperations = new HashMap<>();
        Map<String, String> renameMap = new HashMap<>();

        for (EtlJobConfig.EtlColumn column : indexMeta.columns) {
            if (column.isKey) {
                keyColumns.add(new Column(column.columnName));
            }
        }
        Seq<Column> keyColumnSeq = JavaConverters.asScalaIteratorConverter(keyColumns.iterator()).asScala().toSeq();

        Dataset<Row> aggDataFrame = null;
        for (EtlJobConfig.EtlColumn column : indexMeta.columns) {
            if (column.isKey) {
                continue;
            }
            // agg will modify the result column type, so here add cast for agg result column
            // the case meet is sum(column) will change int to long
            DataType originalType = dataframe.schema().apply(column.columnName).dataType();
            if (column.aggregationType.equals("MAX")) {
                if (aggDataFrame == null) {
                    aggDataFrame = dataframe.groupBy(keyColumnSeq).agg(functions.max(column.columnName).cast(originalType).alias(column.columnName));
                } else {
                    aggDataFrame = aggDataFrame.agg(functions.max(column.columnName).cast(originalType).alias(column.columnName));
                }
            } else if (column.aggregationType.equals("MIN")) {
                if (aggDataFrame == null) {
                    aggDataFrame = dataframe.groupBy(keyColumnSeq).agg(functions.min(column.columnName).cast(originalType).alias(column.columnName));
                } else {
                    aggDataFrame = aggDataFrame.agg(functions.min(column.columnName).cast(originalType).alias(column.columnName));
                }
            } else if (column.aggregationType.equals("SUM")) {
                if (aggDataFrame == null) {
                    aggDataFrame = dataframe.groupBy(keyColumnSeq).agg(functions.sum(column.columnName).cast(originalType).alias(column.columnName));
                } else {
                    aggDataFrame = aggDataFrame.agg(functions.sum(column.columnName).cast(originalType).alias(column.columnName));
                }
            } else if (column.aggregationType.equals("HLL_UNION")) {
                // TODO: add HLL_UNION
            } else if (column.aggregationType.equals("BITMAP_UNION")) {
                // TODO: add BITMAP_UNION
            } else if (column.aggregationType.equals("REPLACE")) {
                // TODO: add REPLACE
            } else if (column.aggregationType.equals("REPLACE_IF_NOT_NULL")) {
                // TODO: add REPLACE_IF_NOT_NULL
            } else {
                System.out.println("INVALID aggregation type:" + column.aggregationType);
            }
        }
        return aggDataFrame;
    }

    private void writePartitionedAndSortedDataframeToParquet(Dataset<Row> dataframe, String pathPattern, long tableId,
                                                             EtlJobConfig.EtlIndex indexMeta) {
        dataframe.foreachPartition(new ForeachPartitionFunction<Row>() {
            @Override
            public void call(Iterator<Row> t) throws Exception {
                // write the data to dst file
                Configuration conf = new Configuration();
                FileSystem fs = FileSystem.get(URI.create(etlJobConfig.outputPath), conf);
                String lastBucketKey = null;
                ParquetWriter<Group> writer = null;
                Types.MessageTypeBuilder builder = Types.buildMessage();
                for (EtlJobConfig.EtlColumn column : indexMeta.columns) {
                    if (column.isAllowNull) {
                        if (column.columnType.equals("SMALLINT") ||column.columnType.equals("INT")) {
                            builder.optional(PrimitiveType.PrimitiveTypeName.INT32).named(column.columnName);
                        } else if (column.columnType.equals("BIGINT")) {
                            builder.optional(PrimitiveType.PrimitiveTypeName.INT64).named(column.columnName);
                        } else if (column.columnType.equals("BOOL")) {
                            builder.optional(PrimitiveType.PrimitiveTypeName.BOOLEAN).named(column.columnName);
                        } else if (column.columnType.equals("VARCHAR")) {
                            // should use as(OriginalType.UTF8), or result will be binary
                            builder.optional(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named(column.columnName);
                        } else if (column.columnType.equals("FLOAT")) {
                            builder.optional(PrimitiveType.PrimitiveTypeName.FLOAT).named(column.columnName);
                        } else if (column.columnType.equals("DOUBLE")) {
                            builder.optional(PrimitiveType.PrimitiveTypeName.DOUBLE).named(column.columnName);
                        } else {
                            // TODO: exception handle
                            System.err.println("invalid column type:" + column);
                        }
                    } else {
                        if (column.columnType.equals("SMALLINT") ||column.columnType.equals("INT")) {
                            builder.required(PrimitiveType.PrimitiveTypeName.INT32).named(column.columnName);
                        } else if (column.columnType.equals("BIGINT")) {
                            builder.required(PrimitiveType.PrimitiveTypeName.INT64).named(column.columnName);
                        } else if (column.columnType.equals("BOOL")) {
                            builder.required(PrimitiveType.PrimitiveTypeName.BOOLEAN).named(column.columnName);
                        }  else if (column.columnType.equals("VARCHAR")) {
                            // should use as(OriginalType.UTF8), or result will be binary
                            builder.required(PrimitiveType.PrimitiveTypeName.BINARY).as(OriginalType.UTF8).named(column.columnName);
                        } else if (column.columnType.equals("FLOAT")) {
                            builder.required(PrimitiveType.PrimitiveTypeName.FLOAT).named(column.columnName);
                        } else if (column.columnType.equals("DOUBLE")) {
                            builder.required(PrimitiveType.PrimitiveTypeName.DOUBLE).named(column.columnName);
                        } else {
                            // TODO: exception handle
                            System.err.println("invalid column type:" + column);
                        }
                    }
                }
                MessageType index_schema = builder.named("index" + indexMeta.indexId);
                while (t.hasNext()) {
                    Row row = t.next();
                    if (row.length() <= 1) {
                        System.err.println("invalid row:" + row);
                        continue;
                    }
                    String curBucketKey = row.getString(0);
                    if (lastBucketKey == null || !curBucketKey.equals(lastBucketKey)) {
                        if (writer != null) {
                            System.out.println("close writer");
                            writer.close();
                        }
                        // flush current writer and create a new writer
                        String[] bucketKey = curBucketKey.split("_");
                        if (bucketKey.length != 2) {
                            System.err.println("invalid bucket key:" + curBucketKey);
                            continue;
                        }
                        int partitionId = Integer.parseInt(bucketKey[0]);
                        int bucketId = Integer.parseInt(bucketKey[1]);
                        String path = String.format(pathPattern, tableId, partitionId, indexMeta.indexId,
                                bucketId, indexMeta.schemaHash);
                        path = path + ".parquet";
                        GroupWriteSupport.setSchema(index_schema, conf);
                        writer = new ParquetWriter<Group>(new Path(path), new GroupWriteSupport(),
                                CompressionCodecName.SNAPPY, 1024, 1024, 512,
                                true, false,
                                ParquetProperties.WriterVersion.PARQUET_2_0, conf);
                        if(writer != null){
                            System.out.println("[HdfsOperate]>> initialize writer succeed! path:" + path);
                        }
                        lastBucketKey = curBucketKey;
                    }
                    SimpleGroupFactory groupFactory = new SimpleGroupFactory(index_schema);
                    Group group = groupFactory.newGroup();
                    for (int i = 1; i < row.length(); i++) {
                        Object columnObject = row.get(i);
                        if (columnObject instanceof Short) {
                            group.add(indexMeta.columns.get(i - 1).columnName, row.getShort(i));
                        } else if (columnObject instanceof Integer) {
                            group.add(indexMeta.columns.get(i - 1).columnName, row.getInt(i));
                        } else if (columnObject instanceof String) {
                            group.add(indexMeta.columns.get(i - 1).columnName, row.getString(i));
                        } else if (columnObject instanceof Long) {
                            group.add(indexMeta.columns.get(i - 1).columnName, row.getLong(i));
                        }
                    }
                    try {
                        writer.write(group);
                    } catch (Exception e) {
                        System.err.println("exception caught:" + e);
                        e.printStackTrace();
                    }
                }
                if (writer != null) {
                    writer.close();
                }
            }
        });
    }

    private void processRollupTree(RollupTreeNode rootNode,
                                   Dataset<Row> rootDataframe,
                                   long tableId, EtlJobConfig.EtlTable tableMeta, EtlJobConfig.EtlIndex baseIndex) {
        Queue<RollupTreeNode> nodeQueue = new LinkedList<>();
        nodeQueue.offer(rootNode);
        int currentLevel = 0;
        // level travel the tree
        Map<Long, Dataset<Row>> parentDataframeMap = new HashMap<>();
        parentDataframeMap.put(baseIndex.indexId, rootDataframe);
        Map<Long, Dataset<Row>> childrenDataframeMap = new HashMap<>();
        String pathPattern = etlJobConfig.outputPath + "/" + etlJobConfig.outputFilePattern;
        while (!nodeQueue.isEmpty()) {
            RollupTreeNode curNode = nodeQueue.poll();
            System.err.println("start to process index:" + curNode.indexId);
            if (curNode.children != null) {
                for (RollupTreeNode child : curNode.children) {
                    nodeQueue.offer(child);
                }
            }
            Dataset<Row> curDataFrame = null;
            // column select for rollup
            if (curNode.level != currentLevel) {
                // need to unpersist parent dataframe
                currentLevel = curNode.level;
                parentDataframeMap.clear();
                parentDataframeMap = childrenDataframeMap;
                childrenDataframeMap = new HashMap<>();
            }
            // TODO: check here
            long parentIndexId = baseIndex.indexId;
            if (curNode.parent != null) {
                parentIndexId = curNode.parent.indexId;
            }

            Dataset<Row> parentDataframe = parentDataframeMap.get(parentIndexId);
            List<Column> columns = new ArrayList<>();
            List<Column> keyColumns = new ArrayList<>();
            Column bucketIdColumn = new Column(BUCKET_ID);
            keyColumns.add(bucketIdColumn);
            columns.add(bucketIdColumn);
            for (String keyName : curNode.keyColumnNames) {
                columns.add(new Column(keyName));
                keyColumns.add(new Column(keyName));
            }
            for (String valueName : curNode.valueColumnNames) {
                columns.add(new Column(valueName));
            }
            Seq<Column> columnSeq = JavaConverters.asScalaIteratorConverter(columns.iterator()).asScala().toSeq();
            curDataFrame = parentDataframe.select(columnSeq);
            if (curNode.indexMeta.indexType.equals("AGGREGATE")) {
                // do aggregation
                curDataFrame = processDataframeAgg(curDataFrame, curNode.indexMeta);
            }
            Seq<Column> keyColumnSeq = JavaConverters.asScalaIteratorConverter(keyColumns.iterator()).asScala().toSeq();
            // should use sortWithinPartitions, not sort
            // because sort will modify the partition number which will lead to bug
            curDataFrame = curDataFrame.sortWithinPartitions(keyColumnSeq);
            childrenDataframeMap.put(curNode.indexId, curDataFrame);

            if (curNode.children != null && curNode.children.size() > 1) {
                // if the children number larger than 1, persist the dataframe for performance
                curDataFrame.persist();
            }
            long curIndexId = curNode.indexId;
            // writePartitionedAndSortedDataframeToFile(curDataFrame, pathPattern, tableId, curIndexId, curNode.indexMeta.schemaHash);
            writePartitionedAndSortedDataframeToParquet(curDataFrame, pathPattern, tableId, curNode.indexMeta);
        }
    }

    private long getHashValue(Row row, List<String> distributeColumns, StructType dstTableSchema) {
        CRC32 hashValue = new CRC32();
        for (String distColumn : distributeColumns) {
            Object columnObject = row.get(row.fieldIndex(distColumn));
            ByteBuffer buffer = getHashValue(columnObject, dstTableSchema.apply(distColumn).dataType());
            hashValue.update(buffer.array(), 0, buffer.limit());
        }
        return hashValue.getValue();
    }

    private Dataset<Row> repartitionDataframeByBucketId(SparkSession spark, Dataset<Row> dataframe,
                                                        EtlJobConfig.EtlPartitionInfo partitionInfo,
                                                        List<Integer> partitionKeyIndex,
                                                        List<Class> partitionKeySchema,
                                                        List<DorisRangePartitioner.PartitionRangeKey> partitionRangeKeys,
                                                        List<String> keyColumnNames,
                                                        List<String> valueColumnNames,
                                                        StructType dstTableSchema,
                                                        EtlJobConfig.EtlIndex baseIndex) {
        List<String> distributeColumns = partitionInfo.distributionColumnRefs;
        Partitioner partitioner = new DorisRangePartitioner(partitionInfo, partitionKeyIndex, partitionRangeKeys);

        JavaPairRDD<String, DppColumns> pairRDD = dataframe.javaRDD().mapToPair(new PairFunction<Row, String, DppColumns>() {
            @Override
            public Tuple2<String, DppColumns> call(Row row) throws Exception {
                List<Object> columns = new ArrayList<>();
                List<Class> classes = new ArrayList<>();
                List<Object> keyColumns = new ArrayList<>();
                List<Class> keyClasses = new ArrayList<>();
                for (String columnName : keyColumnNames) {
                    Object columnObject = row.get(row.fieldIndex(columnName));
                    columns.add(columnObject);
                    classes.add(dataTypeToClass(dstTableSchema.apply(dstTableSchema.fieldIndex(columnName)).dataType()));
                    keyColumns.add(columnObject);
                    keyClasses.add(dataTypeToClass(dstTableSchema.apply(dstTableSchema.fieldIndex(columnName)).dataType()));
                }

                for (String columnName : valueColumnNames) {
                    columns.add(row.get(row.fieldIndex(columnName)));
                    classes.add(dataTypeToClass(dstTableSchema.apply(dstTableSchema.fieldIndex(columnName)).dataType()));
                }
                DppColumns dppColumns = new DppColumns(columns, classes);
                // TODO: judge invalid partitionId
                DppColumns key = new DppColumns(keyColumns, keyClasses);
                int pid = partitioner.getPartition(key);
                if (pid < 0) {
                    // TODO: add log for invalid partition id
                }
                long hashValue = getHashValue(row, distributeColumns, dstTableSchema);
                int bucketId = (int) ((hashValue & 0xffffffff) % partitionInfo.partitions.get(pid).bucketNum);
                long partitionId = partitionInfo.partitions.get(pid).partitionId;
                // bucketKey is partitionId_bucketId
                String bucketKey = Long.toString(partitionId) + "_" + Integer.toString(bucketId);
                return new Tuple2<String, DppColumns>(bucketKey, dppColumns);
            }}
        );

        JavaRDD<Row> resultRdd = pairRDD.map( record -> {
                    String bucketKey = record._1;
                    List<Object> row = new ArrayList<>();
                    // bucketKey as the first key
                    row.add(bucketKey);
                    row.addAll(record._2.columns);
                    return RowFactory.create(row.toArray());
                }
        );

        StructType tableSchemaWithBucketId = createDstTableSchema(baseIndex.columns, true);
        dataframe = spark.createDataFrame(resultRdd, tableSchemaWithBucketId);
        // use bucket number as the parallel number
        int reduceNum = 0;
        for (EtlJobConfig.EtlPartition partition : partitionInfo.partitions) {
            reduceNum += partition.bucketNum;
        }
        dataframe = dataframe.repartition(reduceNum, new Column(BUCKET_ID));
        return dataframe;
    }

    private Dataset<Row> convertSrcDataframeToDstDataframe(Dataset<Row> srcDataframe,
                                                           StructType srcSchema,
                                                           StructType dstTableSchema) {
        // TODO: add mapping columns and default value columns and negative load
        // assume that all columns is simple mapping
        // TODO: calculate the mapping columns
        // TODO: exception handling
        Dataset<Row> dataframe = srcDataframe;
        for (StructField field : srcSchema.fields()) {
            StructField dstField = dstTableSchema.apply(field.name());
            if (!dstField.dataType().equals(field.dataType())) {
                dataframe = dataframe.withColumn(field.name(), dataframe.col(field.name()).cast(dstField.dataType()));
            }
        }
        return dataframe;
    }

    private Dataset<Row> loadDataFromPath(SparkSession spark,
                                          EtlJobConfig.EtlFileGroup fileGroup,
                                          String fileUrl,
                                          StructType srcSchema,
                                          List<String> dataSrcColumns) {
        JavaRDD<String> sourceDataRdd = spark.read().textFile(fileUrl).toJavaRDD();
        JavaRDD<Row> rowRDD = sourceDataRdd.map((Function<String, Row>) record -> {
            // TODO: process null value
            String[] attributes = record.split(fileGroup.columnSeparator);
            if (attributes.length != dataSrcColumns.size()) {
                // update invalid row counter statistics
                // this counter will be record in result.json
            }
            return RowFactory.create(attributes);
        });

        Dataset<Row> dataframe = spark.createDataFrame(rowRDD, srcSchema);
        return dataframe;
    }

    private StructType createScrSchema(List<String> srcColumns) {
        List<StructField> fields = new ArrayList<>();
        for (String srcColumn : srcColumns) {
            // user StringType to load source data
            StructField field = DataTypes.createStructField(srcColumn, DataTypes.StringType, true);
            fields.add(field);
        }
        StructType srcSchema = DataTypes.createStructType(fields);
        return srcSchema;
    }

    // partition keys will be parsed into double from json
    // so need to convert it to partition columns' type
    Object convertPartitionKey(Object srcValue, Class dstClass) {
        if (dstClass.equals(Float.class) || dstClass.equals(Double.class)) {
            return null;
        }
        if (srcValue instanceof Double) {
            if (dstClass.equals(Short.class)) {
                System.out.println("partitionKeySchema schema is short");
                return ((Double)srcValue).shortValue();
            } else if (dstClass.equals(Integer.class)) {
                System.out.println("partitionKeySchema schema is int");
                return ((Double)srcValue).intValue();
            } else if (dstClass.equals(Long.class)) {
                System.out.println("partitionKeySchema schema is int");
                return ((Double)srcValue).longValue();
            } else {
                // dst type is string
                return srcValue.toString();
            }
        } else if (srcValue instanceof String) {
            // TODO: support src value is string type
            return null;
        } else {
            System.out.println("unsupport partition key:" + srcValue);
            return null;
        }
    }

    private List<DorisRangePartitioner.PartitionRangeKey> createPartitionRangeKeys(
            EtlJobConfig.EtlPartitionInfo partitionInfo, List<Class> partitionKeySchema) {
        List<DorisRangePartitioner.PartitionRangeKey> partitionRangeKeys = new ArrayList<>();
        for (EtlJobConfig.EtlPartition partition : partitionInfo.partitions) {
            DorisRangePartitioner.PartitionRangeKey partitionRangeKey = new DorisRangePartitioner.PartitionRangeKey();
            List<Object> startKeyColumns = new ArrayList<>();
            for (int i = 0; i < partition.startKeys.size(); i++) {
                Object value = partition.startKeys.get(i);
                startKeyColumns.add(convertPartitionKey(value, partitionKeySchema.get(i)));
            }
            partitionRangeKey.startKeys = new DppColumns(startKeyColumns, partitionKeySchema);;
            if (!partition.isMaxPartition) {
                partitionRangeKey.isMaxPartition = false;
                List<Object> endKeyColumns = new ArrayList<>();
                for (int i = 0; i < partition.endKeys.size(); i++) {
                    Object value = partition.endKeys.get(i);
                    endKeyColumns.add(convertPartitionKey(value, partitionKeySchema.get(i)));
                }
                partitionRangeKey.endKeys = new DppColumns(endKeyColumns, partitionKeySchema);
            } else {
                partitionRangeKey.isMaxPartition = false;
            }
            partitionRangeKeys.add(partitionRangeKey);
        }
        System.out.println("partitionRangeKeys:" + partitionRangeKeys);
        return partitionRangeKeys;
    }

    public void doDpp() {
        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .getOrCreate();
        for (Map.Entry<Long, EtlJobConfig.EtlTable> entry : etlJobConfig.tables.entrySet()) {
            Long tableId = entry.getKey();
            EtlJobConfig.EtlTable etlTable = entry.getValue();

            // get the base index meta
            //IndexMeta baseIndex = null;
            EtlJobConfig.EtlIndex baseIndex = null;
            for (EtlJobConfig.EtlIndex indexMeta : etlTable.indexes) {
                if (indexMeta.isBaseIndex) {
                    baseIndex = indexMeta;
                    break;
                }
            }

            // get key column names and value column names seperately
            List<String> keyColumnNames = new ArrayList<>();
            List<String> valueColumnNames = new ArrayList<>();
            for (EtlJobConfig.EtlColumn etlColumn : baseIndex.columns) {
                if (etlColumn.isKey) {
                    keyColumnNames.add(etlColumn.columnName);
                } else {
                    valueColumnNames.add(etlColumn.columnName);
                }
            }

            EtlJobConfig.EtlPartitionInfo partitionInfo = etlTable.partitionInfo;
            List<Integer> partitionKeyIndex = new ArrayList<Integer>();
            List<Class> partitionKeySchema = new ArrayList<>();
            for (String key : partitionInfo.partitionColumnRefs) {
                for (int i = 0; i < baseIndex.columns.size(); ++i) {
                    EtlJobConfig.EtlColumn column = baseIndex.columns.get(i);
                    if (column.columnName.equals(key)) {
                        partitionKeyIndex.add(i);
                        partitionKeySchema.add(columnTypeToClass(column.columnType));
                        break;
                    }
                }
            }
            List<DorisRangePartitioner.PartitionRangeKey> partitionRangeKeys = createPartitionRangeKeys(partitionInfo, partitionKeySchema);
            StructType dstTableSchema = createDstTableSchema(baseIndex.columns, false);
            RollupTreeBuilder rollupTreeParser = new MinimalCoverRollupTreeBuilder();
            RollupTreeNode rootNode = rollupTreeParser.build(etlTable);

            for (EtlJobConfig.EtlFileGroup fileGroup : etlTable.fileGroups) {
                // TODO: process the case the user do not provide the source schema
                List<String> dataSrcColumns = fileGroup.fileFieldNames;
                StructType srcSchema = createScrSchema(dataSrcColumns);
                List<String> filePaths = fileGroup.filePaths;
                Dataset<Row> fileGroupDataframe = null;
                for (String filePath : filePaths) {
                    if (fileGroup.columnSeparator == null) {
                        System.out.println("invalid null column separator!");
                        System.exit(-1);
                    }
                    Dataset<Row> dataframe = loadDataFromPath(spark, fileGroup, filePath, srcSchema, dataSrcColumns);
                    dataframe = convertSrcDataframeToDstDataframe(dataframe, srcSchema,dstTableSchema);
                    if (fileGroupDataframe == null) {
                        fileGroupDataframe = dataframe;
                    } else {
                        fileGroupDataframe.union(dataframe);
                    }
                }
                if (fileGroupDataframe == null) {
                    System.err.println("no data for file paths:" + filePaths);
                    continue;
                }
                if (!fileGroup.where.isEmpty()) {
                    fileGroupDataframe = fileGroupDataframe.filter(fileGroup.where);
                }

                fileGroupDataframe = repartitionDataframeByBucketId(spark, fileGroupDataframe,
                        partitionInfo, partitionKeyIndex,
                        partitionKeySchema, partitionRangeKeys,
                        keyColumnNames, valueColumnNames,
                        dstTableSchema, baseIndex);

                processRollupTree(rootNode, fileGroupDataframe, tableId, etlTable, baseIndex);
            }
        }
        spark.stop();
    }
}
