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

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import org.apache.doris.common.UserException;
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
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction;
import org.apache.spark.util.LongAccumulator;
import scala.Tuple2;
import scala.collection.Seq;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Map;
import java.util.Set;
import java.util.List;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Queue;
import java.util.LinkedList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Arrays;
import java.util.zip.CRC32;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;
import scala.collection.JavaConverters;

// This class is a Spark-based data preprocessing program,
// which will make use of the distributed compute framework of spark to
// do ETL job/sort/preaggregate/convert data format job in spark job
// to boost the process of large amount of data load.
public final class SparkDpp implements java.io.Serializable {
    private static final String BUCKET_ID = "__bucketId__";
    private static final String NULL_FLAG = "\\N";
    private static final String DPP_RESULT_FILE = "dpp_result.json";
    private static final String BITMAP_TYPE = "bitmap";
    private EtlJobConfig etlJobConfig = null;
    private LongAccumulator invalidRowAcc = null;
    private DppResult dppResult;
    private SparkSession spark = null;
    private UserDefinedAggregateFunction bitmap_union = null;

    public SparkDpp(SparkSession spark, EtlJobConfig etlJobConfig) {
        this.spark = spark;
        this.etlJobConfig = etlJobConfig;
    }

    public void init() {
        bitmap_union = spark.udf().register("bitmap_union", new BitmapUnion());
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
        if (o == null) {
            buffer.putInt(0);
            return buffer;
        }
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
        dataframe.createOrReplaceTempView("base_table");
        StringBuilder sb = new StringBuilder();
        sb.append("select ");
        sb.append(BUCKET_ID + ",");

        // assume that keys are all before values
        StringBuilder groupBySb = new StringBuilder();
        groupBySb.append(BUCKET_ID + ",");
        Map<String, DataType> valueColumnsOriginalType = new HashMap<>();
        for (EtlJobConfig.EtlColumn column : indexMeta.columns) {
            if (column.isKey) {
                sb.append(column.columnName + ",");
                groupBySb.append(column.columnName + ",");
            } else {
                // get the value columns's original type
                DataType originalType = dataframe.schema().apply(column.columnName).dataType();
                valueColumnsOriginalType.put(column.columnName, originalType);
                if (column.aggregationType.equalsIgnoreCase("MAX")) {
                    sb.append("max(" + column.columnName + ") as " + column.columnName);
                    sb.append(",");
                } else if (column.aggregationType.equalsIgnoreCase("MIN")) {
                    sb.append("min(" + column.columnName + ") as " + column.columnName);
                    sb.append(",");
                } else if (column.aggregationType.equalsIgnoreCase("SUM")) {
                    sb.append("sum(" + column.columnName + ") as " + column.columnName);
                    sb.append(",");
                }  else if (column.aggregationType.equalsIgnoreCase("BITMAP_UNION")) {
                    sb.append("bitmap_union(" + column.columnName + ") as " + column.columnName);
                    sb.append(",");
                }
            }
        }
        sb.deleteCharAt(sb.length() - 1);
        groupBySb.deleteCharAt(groupBySb.length() - 1);
        sb.append(" from base_table ");
        sb.append(" group by ");
        sb.append(groupBySb.toString());
        String aggSql = sb.toString();
        Dataset<Row> aggDataFrame = spark.sql(aggSql);
        // after agg, the type of sum column maybe be changed, so should add type cast for value column
        for (Map.Entry<String, DataType> entry : valueColumnsOriginalType.entrySet()) {
            DataType currentType = aggDataFrame.schema().apply(entry.getKey()).dataType();
            if (!currentType.equals(entry.getValue())) {
                aggDataFrame = aggDataFrame.withColumn(entry.getKey(), aggDataFrame.col(entry.getKey()).cast(entry.getValue()));
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
                MessageType index_schema = builder.named("index_" + indexMeta.indexId);
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
                        GroupWriteSupport.setSchema(index_schema, conf);
                        writer = new ParquetWriter<Group>(new Path(path), new GroupWriteSupport(),
                                CompressionCodecName.SNAPPY, 1024, 1024, 512,
                                true, false,
                                ParquetProperties.WriterVersion.PARQUET_1_0, conf);
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

    private List<String> parseColumnsFromPath(String filePath, List<String> columnsFromPath) throws UserException {
        if (columnsFromPath == null || columnsFromPath.isEmpty()) {
            return Collections.emptyList();
        }
        String[] strings = filePath.split("/");
        if (strings.length < 2) {
            System.err.println("Fail to parse columnsFromPath, expected: " + columnsFromPath + ", filePath: " + filePath);
            throw new UserException("Reason: Fail to parse columnsFromPath, expected: " + columnsFromPath + ", filePath: " + filePath);
        }
        String[] columns = new String[columnsFromPath.size()];
        int size = 0;
        for (int i = strings.length - 2; i >= 0; i--) {
            String str = strings[i];
            if (str != null && str.isEmpty()) {
                continue;
            }
            if (str == null || !str.contains("=")) {
                System.err.println("Fail to parse columnsFromPath, expected: " + columnsFromPath + ", filePath: " + filePath);
                throw new UserException("Reason: Fail to parse columnsFromPath, expected: " + columnsFromPath + ", filePath: " + filePath);
            }
            String[] pair = str.split("=", 2);
            if (pair.length != 2) {
                System.err.println("Fail to parse columnsFromPath, expected: " + columnsFromPath + ", filePath: " + filePath);
                throw new UserException("Reason: Fail to parse columnsFromPath, expected: " + columnsFromPath + ", filePath: " + filePath);
            }
            int index = columnsFromPath.indexOf(pair[0]);
            if (index == -1) {
                continue;
            }
            columns[index] = pair[1];
            size++;
            if (size >= columnsFromPath.size()) {
                break;
            }
        }
        if (size != columnsFromPath.size()) {
            System.err.println("Fail to parse columnsFromPath, expected: " + columnsFromPath + ", filePath: " + filePath);
            throw new UserException("Reason: Fail to parse columnsFromPath, expected: " + columnsFromPath + ", filePath: " + filePath);
        }
        return Lists.newArrayList(columns);
    }

    private Dataset<Row> repartitionDataframeByBucketId(SparkSession spark, Dataset<Row> dataframe,
                                                        EtlJobConfig.EtlPartitionInfo partitionInfo,
                                                        List<Integer> partitionKeyIndex,
                                                        List<Class> partitionKeySchema,
                                                        List<DorisRangePartitioner.PartitionRangeKey> partitionRangeKeys,
                                                        List<String> keyColumnNames,
                                                        List<String> valueColumnNames,
                                                        StructType dstTableSchema,
                                                        EtlJobConfig.EtlIndex baseIndex,
                                                        List<Long> validPartitionId) {
        List<String> distributeColumns = partitionInfo.distributionColumnRefs;
        Partitioner partitioner = new DorisRangePartitioner(partitionInfo, partitionKeyIndex, partitionRangeKeys);
        Set<Integer> validPartitionIndex = new HashSet<>();
        if (validPartitionId == null) {
            for (int i = 0; i < partitionInfo.partitions.size(); ++i) {
                validPartitionIndex.add(i);
            }
        } else {
            for (int i = 0; i < partitionInfo.partitions.size(); ++i) {
                if (validPartitionId.contains(partitionInfo.partitions.get(i).partitionId)) {
                    validPartitionIndex.add(i);
                }
            }
        }
        JavaPairRDD<String, DppColumns> pairRDD = dataframe.javaRDD().flatMapToPair(new PairFlatMapFunction<Row, String, DppColumns>() {
            @Override
            public Iterator<Tuple2<String, DppColumns>> call(Row row) {
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
                DppColumns key = new DppColumns(keyColumns, keyClasses);
                List<Tuple2<String, DppColumns>> result = new ArrayList<>();
                int pid = partitioner.getPartition(key);
                if (!validPartitionIndex.contains(pid)) {
                    System.err.println("invalid partition for row:" + row + ", pid:" + pid);
                    invalidRowAcc.add(1);
                } else {
                    long hashValue = getHashValue(row, distributeColumns, dstTableSchema);
                    int bucketId = (int) ((hashValue & 0xffffffff) % partitionInfo.partitions.get(pid).bucketNum);
                    long partitionId = partitionInfo.partitions.get(pid).partitionId;
                    // bucketKey is partitionId_bucketId
                    String bucketKey = Long.toString(partitionId) + "_" + Integer.toString(bucketId);
                    Tuple2<String, DppColumns> newTuple = new Tuple2<String, DppColumns>(bucketKey, dppColumns);
                    result.add(newTuple);
                }
                return result.iterator();
            }
        });

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

    private Dataset<Row> convertSrcDataframeToDstDataframe(EtlJobConfig.EtlIndex baseIndex,
                                                           Dataset<Row> srcDataframe,
                                                           StructType dstTableSchema,
                                                           EtlJobConfig.EtlFileGroup fileGroup) throws UserException {
        System.out.println("convertSrcDataframeToDstDataframe for index:" + baseIndex);
        Dataset<Row> dataframe = srcDataframe;
        StructType srcSchema = dataframe.schema();
        Set<String> srcColumnNames = new HashSet<>();
        for (StructField field : srcSchema.fields()) {
            srcColumnNames.add(field.name());
        }
        Map<String, EtlJobConfig.EtlColumnMapping> columnMappings = fileGroup.columnMappings;
        // 1. process simple columns
        Set<String> mappingColumns = null;
        if (columnMappings != null) {
            mappingColumns = columnMappings.keySet();
        }
        List<String> dstColumnNames = new ArrayList<>();
        for (StructField dstField : dstTableSchema.fields()) {
            dstColumnNames.add(dstField.name());
            EtlJobConfig.EtlColumn column = baseIndex.getColumn(dstField.name());
            if (!srcColumnNames.contains(dstField.name())) {
                if (mappingColumns != null && mappingColumns.contains(dstField.name())) {
                    // mapping columns will be processed in next step
                    continue;
                }
                if (column.defaultValue != null) {
                    if (column.defaultValue.equals(NULL_FLAG)) {
                        dataframe = dataframe.withColumn(dstField.name(), functions.lit(null));
                    } else {
                        dataframe = dataframe.withColumn(dstField.name(), functions.lit(column.defaultValue));
                    }
                } else if (column.isAllowNull) {
                    dataframe = dataframe.withColumn(dstField.name(), functions.lit(null));
                } else {
                    throw new UserException("Reason: no data for column:" + dstField.name());
                }
            }
            if (!column.columnType.equalsIgnoreCase(BITMAP_TYPE) && !dstField.dataType().equals(DataTypes.StringType)) {
                dataframe = dataframe.withColumn(dstField.name(), dataframe.col(dstField.name()).cast(dstField.dataType()));
                if (fileGroup.isNegative && !column.isKey) {
                    // negative load
                    // value will be convert te -1 * value
                    dataframe = dataframe.withColumn(dstField.name(), functions.expr("-1 *" + dstField.name()));
                }
            }
        }
        // 2. process the mapping columns
        for (String mappingColumn : mappingColumns) {
            String mappingDescription = columnMappings.get(mappingColumn).toDescription();
            // here should cast data type to dst column type
            dataframe = dataframe.withColumn(mappingColumn,
                    functions.expr(mappingDescription).cast(dstTableSchema.apply(mappingColumn).dataType()));
        }
        // projection and reorder the columns
        dataframe.createOrReplaceTempView("src_table");
        StringBuilder selectSqlBuilder = new StringBuilder();
        selectSqlBuilder.append("select ");
        for (String name : dstColumnNames) {
            selectSqlBuilder.append(name + ",");
        }
        selectSqlBuilder.deleteCharAt(selectSqlBuilder.length() - 1);
        selectSqlBuilder.append(" from src_table");
        String selectSql = selectSqlBuilder.toString();
        dataframe = spark.sql(selectSql);
        return dataframe;
    }

    private Dataset<Row> loadDataFromPath(SparkSession spark,
                                          EtlJobConfig.EtlFileGroup fileGroup,
                                          String fileUrl,
                                          EtlJobConfig.EtlIndex baseIndex) throws UserException {
        List<String> columnValueFromPath = parseColumnsFromPath(fileUrl, fileGroup.columnsFromPath);
        List<String> dataSrcColumns = fileGroup.fileFieldNames;
        if (dataSrcColumns == null) {
            // if there is no source columns info
            // use base index columns as source columns
            dataSrcColumns = new ArrayList<>();
            for (EtlJobConfig.EtlColumn column : baseIndex.columns) {
                dataSrcColumns.add(column.columnName);
            }
        }
        List<String> srcColumnsWithColumnsFromPath = new ArrayList<>();
        srcColumnsWithColumnsFromPath.addAll(dataSrcColumns);
        if (fileGroup.columnsFromPath != null) {
            srcColumnsWithColumnsFromPath.addAll(fileGroup.columnsFromPath);
        }
        StructType srcSchema = createScrSchema(srcColumnsWithColumnsFromPath);
        JavaRDD<String> sourceDataRdd = spark.read().textFile(fileUrl).toJavaRDD();
        int columnSize = dataSrcColumns.size();
        // now we first support csv file
        // TODO: support parquet file and orc file
        JavaRDD<Row> rowRDD = sourceDataRdd.flatMap(
                record -> {
                    String[] attributes = record.split(fileGroup.columnSeparator);
                    List<Row> result = new ArrayList<>();
                    if (attributes.length != columnSize) {
                        invalidRowAcc.add(1);
                        System.err.println("invalid src schema, data columns:"
                                + attributes.length + ", file group columns:"
                                + columnSize + ", row:" + record);
                    } else {
                        for (int i = 0; i < attributes.length; ++i) {
                            if (attributes[i].equals(NULL_FLAG)) {
                                if (baseIndex.columns.get(i).isAllowNull) {
                                    attributes[i] = null;
                                } else {
                                    invalidRowAcc.add(1);
                                    System.err.println("colunm:" + i + " can not be null. row:" + record);
                                    return null;
                                }
                            }
                        }
                        Row row = null;
                        if (fileGroup.columnsFromPath == null) {
                            row = RowFactory.create(attributes);
                        } else {
                            // process columns from path
                            // append columns from path to the tail
                            List<String> columnAttributes = new ArrayList<>();
                            columnAttributes.addAll(Arrays.asList(attributes));
                            columnAttributes.addAll(columnValueFromPath);
                            row = RowFactory.create(columnAttributes.toArray());
                        }
                        result.add(row);
                    }
                    return result.iterator();
                }
        );

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
                return ((Double)srcValue).shortValue();
            } else if (dstClass.equals(Integer.class)) {
                return ((Double)srcValue).intValue();
            } else if (dstClass.equals(Long.class)) {
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

    private Dataset<Row> loadDataFromFilePaths(SparkSession spark,
                                               EtlJobConfig.EtlIndex baseIndex,
                                               List<String> filePaths,
                                               EtlJobConfig.EtlFileGroup fileGroup,
                                               StructType dstTableSchema) throws UserException {
        Dataset<Row> fileGroupDataframe = null;
        for (String filePath : filePaths) {
            if (fileGroup.columnSeparator == null) {
                System.err.println("invalid null column separator!");
                throw new UserException("Reason: invalid null column separator!");
            }
            Dataset<Row> dataframe = null;

            dataframe = loadDataFromPath(spark, fileGroup, filePath, baseIndex);
            dataframe = convertSrcDataframeToDstDataframe(baseIndex, dataframe, dstTableSchema, fileGroup);
            if (fileGroupDataframe == null) {
                fileGroupDataframe = dataframe;
            } else {
                fileGroupDataframe.union(dataframe);
            }
        }
        return fileGroupDataframe;
    }

    private Dataset<Row> loadDataFromHiveTable(SparkSession spark, String hiveTableName) {
        Dataset<Row> fileGroupDataframe = spark.sql("select * from " + hiveTableName);
        return fileGroupDataframe;
    }

    private DppResult process() throws Exception {
        dppResult = new DppResult();
        try {
            invalidRowAcc = spark.sparkContext().longAccumulator();
            for (Map.Entry<Long, EtlJobConfig.EtlTable> entry : etlJobConfig.tables.entrySet()) {
                Long tableId = entry.getKey();
                EtlJobConfig.EtlTable etlTable = entry.getValue();

                // get the base index meta
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
                System.out.println("Rollup Tree:" + rootNode);

                for (EtlJobConfig.EtlFileGroup fileGroup : etlTable.fileGroups) {
                    List<String> filePaths = fileGroup.filePaths;
                    Dataset<Row> fileGroupDataframe = null;
                    if (Strings.isNullOrEmpty(fileGroup.hiveTableName)) {
                        fileGroupDataframe = loadDataFromFilePaths(spark, baseIndex, filePaths, fileGroup, dstTableSchema);
                    } else {
                        String taskId = etlJobConfig.outputPath.substring(etlJobConfig.outputPath.lastIndexOf("/") + 1);
                        String dorisIntermediateHiveTable = String.format(EtlJobConfig.DORIS_INTERMEDIATE_HIVE_TABLE_NAME,
                                tableId, taskId);
                        fileGroupDataframe = loadDataFromHiveTable(spark, dorisIntermediateHiveTable);
                    }
                    if (fileGroupDataframe == null) {
                        System.err.println("no data for file file group:" + fileGroup);
                        continue;
                    }
                    fileGroupDataframe.show();
                    if (!Strings.isNullOrEmpty(fileGroup.where)) {
                        fileGroupDataframe = fileGroupDataframe.filter(fileGroup.where);
                    }
                    fileGroupDataframe.show();

                    fileGroupDataframe = repartitionDataframeByBucketId(spark, fileGroupDataframe,
                            partitionInfo, partitionKeyIndex,
                            partitionKeySchema, partitionRangeKeys,
                            keyColumnNames, valueColumnNames,
                            dstTableSchema, baseIndex, fileGroup.partitions);
                    processRollupTree(rootNode, fileGroupDataframe, tableId, etlTable, baseIndex);
                }
            }
            spark.stop();
        } catch (Exception exception) {
            exception.printStackTrace();
            System.err.println("spark dpp failed for exception:" + exception);
            dppResult.isSuccess = false;
            dppResult.failedReason = exception.getMessage();
            dppResult.invalidRowNumber = invalidRowAcc.value();
            throw exception;
        }
        dppResult.isSuccess = true;
        dppResult.failedReason = "";
        dppResult.invalidRowNumber = invalidRowAcc.value();
        return dppResult;
    }

    public void doDpp() {
        // write dpp result to output
        try {
            DppResult dppResult = process();
            String outputPath = etlJobConfig.getOutputPath();
            String resultFilePath = outputPath + "/" + DPP_RESULT_FILE;
            Configuration conf = new Configuration();
            URI uri = new URI(outputPath);
            FileSystem fs = FileSystem.get(uri, conf);
            Path filePath = new Path(resultFilePath);
            FSDataOutputStream outputStream= fs.create(filePath);
            Gson gson = new Gson();
            outputStream.write(gson.toJson(dppResult).getBytes());
            outputStream.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
