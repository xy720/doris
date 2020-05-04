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

package org.apache.doris.load.loadv2;

import org.apache.doris.analysis.BrokerDesc;
import org.apache.doris.analysis.DataDescription;
import org.apache.doris.analysis.DataProcessorDesc;
import org.apache.doris.analysis.EtlClusterDesc;
import org.apache.doris.analysis.LabelName;
import org.apache.doris.analysis.LoadStmt;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.EtlClusterMgr;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PartitionInfo;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.RangePartitionInfo;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.SparkEtlCluster;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.LoadException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.load.EtlJobType;
import org.apache.doris.load.EtlStatus;
import org.apache.doris.load.FailMsg;
import org.apache.doris.load.loadv2.etl.EtlJobConfig;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.task.AgentBatchTask;
import org.apache.doris.task.AgentTaskExecutor;
import org.apache.doris.task.MasterTaskExecutor;
import org.apache.doris.task.PushTask;
import org.apache.doris.thrift.TEtlState;
import org.apache.doris.transaction.GlobalTransactionMgr;
import org.apache.doris.transaction.TabletCommitInfo;
import org.apache.doris.transaction.TransactionState;
import org.apache.doris.transaction.TransactionState.LoadJobSourceType;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mocked;
import org.apache.spark.launcher.SparkAppHandle;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class SparkLoadJobTest {
    private long dbId;
    private String dbName;
    private String tableName;
    private String label;
    private String clusterName;
    private String broker;
    private long transactionId;
    private long pendingTaskId;
    private String appId;
    private String etlOutputPath;
    private long tableId;
    private long partitionId;
    private long indexId;
    private long tabletId;
    private long replicaId;
    private long backendId;
    private int schemaHash;

    @Before
    public void setUp() {
        dbId = 1L;
        dbName = "database0";
        tableName = "table0";
        label = "label0";
        clusterName = "cluster0";
        broker = "broker0";
        transactionId = 2L;
        pendingTaskId = 3L;
        appId = "application_15888888888_0088";
        etlOutputPath = "hdfs://127.0.0.1:10000/tmp/doris/100/label/101";
        tableId = 10L;
        partitionId = 11L;
        indexId = 12L;
        tabletId = 13L;
        replicaId = 14L;
        backendId = 15L;
        schemaHash = 146886;
    }

    @Test
    public void testCreateFromLoadStmt(@Mocked Catalog catalog, @Injectable LoadStmt loadStmt,
                                       @Injectable DataDescription dataDescription, @Injectable LabelName labelName,
                                       @Injectable Database db, @Injectable OlapTable olapTable,
                                       @Injectable String originStmt, @Injectable EtlClusterMgr etlClusterMgr) {
        List<DataDescription> dataDescriptionList = Lists.newArrayList();
        dataDescriptionList.add(dataDescription);
        Map<String, String> clusterProperties = Maps.newHashMap();
        clusterProperties.put("spark_configs", "spark.executor.memory=1g");
        clusterProperties.put("broker", broker);
        clusterProperties.put("broker.username", "user0");
        clusterProperties.put("broker.password", "password0");
        DataProcessorDesc dataProcessorDesc = new EtlClusterDesc(clusterName, clusterProperties);
        Map<String, String> jobProperties = Maps.newHashMap();
        String hiveTable = "hivedb.table0";
        jobProperties.put("bitmap_data", hiveTable);
        SparkEtlCluster etlCluster = new SparkEtlCluster(clusterName);

        new Expectations() {
            {
                catalog.getDb(dbName);
                result = db;
                catalog.getEtlClusterMgr();
                result = etlClusterMgr;
                db.getTable(tableName);
                result = olapTable;
                db.getId();
                result = dbId;
                loadStmt.getLabel();
                result = labelName;
                loadStmt.getDataDescriptions();
                result = dataDescriptionList;
                loadStmt.getDataProcessorDesc();
                result = dataProcessorDesc;
                loadStmt.getProperties();
                result = jobProperties;
                labelName.getDbName();
                result = dbName;
                labelName.getLabelName();
                result = label;
                dataDescription.getTableName();
                result = tableName;
                dataDescription.getPartitionNames();
                result = null;
                etlClusterMgr.getEtlCluster(clusterName);
                result = etlCluster;
            }
        };

        try {
            Assert.assertTrue(etlCluster.getSparkConfigsMap().isEmpty());
            BulkLoadJob bulkLoadJob = BulkLoadJob.fromLoadStmt(loadStmt, new OriginStatement(originStmt, 0));
            SparkLoadJob sparkLoadJob = (SparkLoadJob) bulkLoadJob;
            // check member
            Assert.assertEquals(dbId, bulkLoadJob.dbId);
            Assert.assertEquals(label, bulkLoadJob.label);
            Assert.assertEquals(JobState.PENDING, bulkLoadJob.getState());
            Assert.assertEquals(EtlJobType.SPARK, bulkLoadJob.getJobType());
            Assert.assertEquals(clusterName, sparkLoadJob.getEtlClusterName());
            Assert.assertEquals(-1L, sparkLoadJob.getEtlStartTimestamp());
            Assert.assertEquals(hiveTable, sparkLoadJob.getHiveTableName());

            // check update etl cluster properties
            Assert.assertEquals(broker, bulkLoadJob.brokerDesc.getName());
            Assert.assertEquals("user0", bulkLoadJob.brokerDesc.getProperties().get("username"));
            Assert.assertEquals("password0", bulkLoadJob.brokerDesc.getProperties().get("password"));
            SparkEtlCluster sparkEtlCluster = Deencapsulation.getField(sparkLoadJob, "etlCluster");
            Assert.assertTrue(sparkEtlCluster.getSparkConfigsMap().containsKey("spark.executor.memory"));
            Assert.assertEquals("1g", sparkEtlCluster.getSparkConfigsMap().get("spark.executor.memory"));
        } catch (DdlException e) {
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testExecute(@Mocked Catalog catalog, @Mocked SparkLoadPendingTask pendingTask,
                            @Injectable String originStmt, @Injectable GlobalTransactionMgr transactionMgr,
                            @Injectable MasterTaskExecutor executor) throws Exception {
        new Expectations() {
            {
                Catalog.getCurrentGlobalTransactionMgr();
                result = transactionMgr;
                transactionMgr.beginTransaction(dbId, Lists.newArrayList(), label, null,
                                                (TransactionState.TxnCoordinator) any, LoadJobSourceType.FRONTEND,
                                                anyLong, anyLong);
                result = transactionId;
                pendingTask.init();
                pendingTask.getSignature();
                result = pendingTaskId;
                catalog.getLoadTaskScheduler();
                result = executor;
                executor.submit((SparkLoadPendingTask) any);
                result = true;
            }
        };

        EtlClusterDesc etlClusterDesc = new EtlClusterDesc(clusterName, Maps.newHashMap());
        SparkLoadJob job = new SparkLoadJob(dbId, label, etlClusterDesc, new OriginStatement(originStmt, 0));
        job.execute();

        // check transaction id and id to tasks
        Assert.assertEquals(transactionId, job.getTransactionId());
        Assert.assertTrue(job.idToTasks.containsKey(pendingTaskId));
    }

    @Test
    public void testOnPendingTaskFinished(@Mocked Catalog catalog, @Injectable String originStmt) throws MetaNotFoundException {
        EtlClusterDesc etlClusterDesc = new EtlClusterDesc(clusterName, Maps.newHashMap());
        SparkLoadJob job = new SparkLoadJob(dbId, label, etlClusterDesc, new OriginStatement(originStmt, 0));
        SparkPendingTaskAttachment attachment = new SparkPendingTaskAttachment(pendingTaskId);
        attachment.setAppId(appId);
        attachment.setOutputPath(etlOutputPath);
        job.onTaskFinished(attachment);

        // check pending task finish
        Assert.assertTrue(job.finishedTaskIds.contains(pendingTaskId));
        Assert.assertEquals(appId, Deencapsulation.getField(job, "appId"));
        Assert.assertEquals(etlOutputPath, Deencapsulation.getField(job, "etlOutputPath"));
        Assert.assertEquals(JobState.ETL, job.getState());
    }

    private SparkLoadJob getEtlStateJob(String originStmt) throws MetaNotFoundException {
        SparkEtlCluster etlCluster = new SparkEtlCluster(clusterName);
        Deencapsulation.setField(etlCluster, "master", "yarn");
        SparkLoadJob job = new SparkLoadJob(dbId, label, null, new OriginStatement(originStmt, 0));
        job.state = JobState.ETL;
        job.maxFilterRatio = 0.15;
        job.transactionId = transactionId;
        Deencapsulation.setField(job, "appId", appId);
        Deencapsulation.setField(job, "etlOutputPath", etlOutputPath);
        Deencapsulation.setField(job, "etlCluster", etlCluster);
        BrokerDesc brokerDesc = new BrokerDesc(broker, Maps.newHashMap());
        job.brokerDesc = brokerDesc;
        return job;
    }

    @Test
    public void testUpdateEtlStatusRunning(@Mocked Catalog catalog, @Injectable String originStmt,
                                           @Mocked SparkEtlJobHandler handler) throws Exception {
        String trackingUrl = "http://127.0.0.1:8080/proxy/application_1586619723848_0088/";
        int progress = 66;
        EtlStatus status = new EtlStatus();
        status.setState(TEtlState.RUNNING);
        status.setTrackingUrl(trackingUrl);
        status.setProgress(progress);

        new Expectations() {
            {
                handler.getEtlJobStatus((SparkAppHandle) any, appId, anyLong, etlOutputPath,
                                        (SparkEtlCluster) any, (BrokerDesc) any);
                result = status;
            }
        };

        SparkLoadJob job = getEtlStateJob(originStmt);
        job.updateEtlStatus();

        // check update etl running
        Assert.assertEquals(JobState.ETL, job.getState());
        Assert.assertEquals(progress, job.progress);
        Assert.assertEquals(trackingUrl, job.loadingStatus.getTrackingUrl());
    }

    @Test(expected = LoadException.class)
    public void testUpdateEtlStatusCancelled(@Mocked Catalog catalog, @Injectable String originStmt,
                                             @Mocked SparkEtlJobHandler handler) throws Exception {
        EtlStatus status = new EtlStatus();
        status.setState(TEtlState.CANCELLED);

        new Expectations() {
            {
                handler.getEtlJobStatus((SparkAppHandle) any, appId, anyLong, etlOutputPath,
                                        (SparkEtlCluster) any, (BrokerDesc) any);
                result = status;
            }
        };

        SparkLoadJob job = getEtlStateJob(originStmt);
        job.updateEtlStatus();
    }

    @Test
    public void testUpdateEtlStatusFinishedQualityFailed(@Mocked Catalog catalog, @Injectable String originStmt,
                                                         @Mocked SparkEtlJobHandler handler) throws Exception {
        EtlStatus status = new EtlStatus();
        status.setState(TEtlState.FINISHED);
        status.getCounters().put("dpp.norm.ALL", "8");
        status.getCounters().put("dpp.abnorm.ALL", "2");

        new Expectations() {
            {
                handler.getEtlJobStatus((SparkAppHandle) any, appId, anyLong, etlOutputPath,
                                        (SparkEtlCluster) any, (BrokerDesc) any);
                result = status;
            }
        };

        SparkLoadJob job = getEtlStateJob(originStmt);
        job.updateEtlStatus();

        // check update etl finished, but quality failed
        Assert.assertEquals(JobState.CANCELLED, job.getState());
        Assert.assertEquals(FailMsg.CancelType.ETL_QUALITY_UNSATISFIED, job.failMsg.getCancelType());
    }

    @Test
    public void testUpdateEtlStatusFinishedAndCommitTransaction(
            @Mocked Catalog catalog, @Injectable String originStmt,
            @Mocked SparkEtlJobHandler handler, @Mocked AgentTaskExecutor executor,
            @Injectable Database db, @Injectable OlapTable table, @Injectable Partition partition,
            @Injectable MaterializedIndex index, @Injectable Tablet tablet, @Injectable Replica replica,
            @Injectable GlobalTransactionMgr transactionMgr) throws Exception {
        EtlStatus status = new EtlStatus();
        status.setState(TEtlState.FINISHED);
        status.getCounters().put("dpp.norm.ALL", "9");
        status.getCounters().put("dpp.abnorm.ALL", "1");
        Map<String, Long> filePathToSize = Maps.newHashMap();
        String filePath = String.format("hdfs://127.0.0.1:10000/doris/jobs/1/label6/9/label6.%d.%d.%d.0.%d.parquet",
                                        tableId, partitionId, indexId, schemaHash);
        long fileSize = 6L;
        filePathToSize.put(filePath, fileSize);
        PartitionInfo partitionInfo = new RangePartitionInfo();
        partitionInfo.addPartition(partitionId, null, (short) 1, false);

        new Expectations() {
            {
                handler.getEtlJobStatus((SparkAppHandle) any, appId, anyLong, etlOutputPath,
                                        (SparkEtlCluster) any, (BrokerDesc) any);
                result = status;
                handler.getEtlFilePaths(etlOutputPath, (BrokerDesc) any);
                result = filePathToSize;
                catalog.getDb(dbId);
                result = db;
                db.getTable(tableId);
                result = table;
                table.getPartition(partitionId);
                result = partition;
                table.getPartitionInfo();
                result = partitionInfo;
                table.getSchemaByIndexId(Long.valueOf(12));
                result = Lists.newArrayList(new Column("k1", PrimitiveType.VARCHAR));
                partition.getMaterializedIndices(MaterializedIndex.IndexExtState.ALL);
                result = Lists.newArrayList(index);
                index.getId();
                result = indexId;
                index.getTablets();
                result = Lists.newArrayList(tablet);
                tablet.getId();
                result = tabletId;
                tablet.getReplicas();
                result = Lists.newArrayList(replica);
                replica.getId();
                result = replicaId;
                replica.getBackendId();
                result = backendId;
                replica.getLastFailedVersion();
                result = -1;
                AgentTaskExecutor.submit((AgentBatchTask) any);
                Catalog.getCurrentGlobalTransactionMgr();
                result = transactionMgr;
                transactionMgr.commitTransaction(dbId, transactionId, (List<TabletCommitInfo>) any,
                                                 (LoadJobFinalOperation) any);
            }
        };

        SparkLoadJob job = getEtlStateJob(originStmt);
        job.updateEtlStatus();

        // check update etl finished
        Assert.assertEquals(JobState.LOADING, job.getState());
        Assert.assertEquals(0, job.progress);
        Map<String, Pair<String, Long>> tabletMetaToFileInfo = Deencapsulation.getField(job, "tabletMetaToFileInfo");
        Assert.assertEquals(1, tabletMetaToFileInfo.size());
        String tabletMetaStr = EtlJobConfig.getTabletMetaStr(filePath);
        Assert.assertTrue(tabletMetaToFileInfo.containsKey(tabletMetaStr));
        Pair<String, Long> fileInfo = tabletMetaToFileInfo.get(tabletMetaStr);
        Assert.assertEquals(filePath, fileInfo.first);
        Assert.assertEquals(fileSize, (long) fileInfo.second);
        Map<Long, Map<Long, PushTask>> tabletToSentReplicaPushTask
                = Deencapsulation.getField(job, "tabletToSentReplicaPushTask");
        Assert.assertTrue(tabletToSentReplicaPushTask.containsKey(tabletId));
        Assert.assertTrue(tabletToSentReplicaPushTask.get(tabletId).containsKey(replicaId));
        Map<Long, Set<Long>> tableToLoadPartitions = Deencapsulation.getField(job, "tableToLoadPartitions");
        Assert.assertTrue(tableToLoadPartitions.containsKey(tableId));
        Assert.assertTrue(tableToLoadPartitions.get(tableId).contains(partitionId));
        Map<Long, Integer> indexToSchemaHash = Deencapsulation.getField(job, "indexToSchemaHash");
        Assert.assertTrue(indexToSchemaHash.containsKey(indexId));
        Assert.assertEquals(schemaHash, (long) indexToSchemaHash.get(indexId));

        // finish push task
        job.addFinishedReplica(replicaId, tabletId, backendId);
        job.updateLoadingStatus();
        Assert.assertEquals(99, job.progress);
        Set<Long> fullTablets = Deencapsulation.getField(job, "fullTablets");
        Assert.assertTrue(fullTablets.contains(tabletId));
    }
}