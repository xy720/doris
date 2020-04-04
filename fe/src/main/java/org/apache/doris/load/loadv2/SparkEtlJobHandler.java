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

import com.google.common.base.Preconditions;
import org.apache.doris.PaloFe;
import org.apache.doris.analysis.BrokerDesc;
import org.apache.doris.catalog.SparkEtlCluster;
import org.apache.doris.common.LoadException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.BrokerUtil;
import org.apache.doris.common.util.Util;
import org.apache.doris.load.EtlStatus;
import org.apache.doris.load.loadv2.etl.EtlJobConfig;
import org.apache.doris.thrift.TBrokerFileStatus;
import org.apache.doris.thrift.TEtlState;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.ApplicationNotFoundException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkAppHandle.Listener;
import org.apache.spark.launcher.SparkAppHandle.State;
import org.apache.spark.launcher.SparkLauncher;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.List;
import java.util.Map;

public class SparkEtlJobHandler {
    private static final Logger LOG = LogManager.getLogger(SparkEtlJobHandler.class);

    public static final String NUM_TASKS = "numTasks";
    public static final String NUM_COMPLETED_TASKS = "numCompletedTasks";

    private static final String JOB_CONFIG_DIR = PaloFe.DORIS_HOME_DIR + "/temp/job_conf";
    private static final String APP_RESOURCE = PaloFe.DORIS_HOME_DIR + "/lib/palo-fe.jar";
    private static final String MAIN_CLASS = "org.apache.doris.load.loadv2.etl.SparkEtlJob";
    private static final String SPARK_DEPLOY_MODE = "cluster";
    private static final String ETL_JOB_NAME = "doris__%s";
    // http://host:port/api/v1/applications/appid/jobs
    private static final String STATUS_URL = "%s/api/v1/applications/%s/jobs";

    private static final int MAX_RETRY = 300;

    class SparkAppListener implements Listener {
        @Override
        public void stateChanged(SparkAppHandle sparkAppHandle) {}

        @Override
        public void infoChanged(SparkAppHandle sparkAppHandle) {}
    }

    public void submitEtlJob(long loadJobId, String loadLabel, SparkEtlCluster etlCluster, BrokerDesc brokerDesc,
                             String jobJsonConfig, SparkPendingTaskAttachment attachment) throws LoadException {
        // check outputPath exist

        // create job config file
        String configDirPath = JOB_CONFIG_DIR + "/" + loadJobId;
        String configFilePath = configDirPath + "/" + EtlJobConfig.JOB_CONFIG_FILE_NAME;
        createJobConfigFile(configDirPath, configFilePath, loadJobId, jobJsonConfig);

        // spark cluster config
        SparkLauncher launcher = new SparkLauncher();
        launcher.setMaster(etlCluster.getMaster())
                .setDeployMode(SPARK_DEPLOY_MODE)
                .setAppResource(APP_RESOURCE)
                .setMainClass(MAIN_CLASS)
                .setAppName(String.format(ETL_JOB_NAME, loadLabel))
                .addFile(configFilePath);
        // spark args: --jars, --files
        for (Map.Entry<String, String> entry : etlCluster.getSparkArgsMap().entrySet()) {
            launcher.addSparkArg(entry.getKey(), entry.getValue());
        }

        // start app
        SparkAppHandle handle = null;
        String appId = null;
        int retry = 0;
        try {
            handle = launcher.startApplication(new SparkAppListener());
            while (retry++ < MAX_RETRY) {
                appId = handle.getAppId();
                if (appId != null) {
                    break;
                }

                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    LOG.warn(e.getMessage());
                }
            }
        } catch (IOException e) {
            String errMsg = "start spark app fail, error: " + e.toString();
            LOG.warn(errMsg, e);
            throw new LoadException(errMsg, e);
        } finally {
            // delete config file
            Util.deleteDirectory(new File(configDirPath));
        }

        if (appId == null && retry >= MAX_RETRY) {
            throw new LoadException("Get appid has been retried too many times");
        }

        // success
        attachment.setAppId(appId);
        attachment.setHandle(handle);
    }

    private void createJobConfigFile(String configDirPath, String configFilePath,
                                     long loadJobId, String jsonConfig) throws LoadException {
        // check config dir
        File configDir = new File(configDirPath);
        if (!Util.deleteDirectory(configDir)) {
            String errMsg = "delete config dir error. job: " + loadJobId;
            LOG.warn(errMsg);
            throw new LoadException(errMsg);
        }
        if (!configDir.mkdirs()) {
            String errMsg = "create config dir error. job: " + loadJobId;
            LOG.warn(errMsg);
            throw new LoadException(errMsg);
        }

        // write file
        File configFile = new File(configFilePath);
        BufferedWriter bw = null;
        try {
            bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(configFile),
                                                           "UTF-8"));
            bw.write(jsonConfig);
            bw.flush();
        } catch (IOException e) {
            Util.deleteDirectory(configDir);
            String errMsg = "create config file error. job: " + loadJobId;
            LOG.warn(errMsg);
            throw new LoadException(errMsg);
        } finally {
            if (bw != null) {
                try {
                    bw.close();
                } catch (IOException e) {
                    LOG.warn("close buffered writer error", e);
                }
            }
        }
    }

    public EtlStatus getEtlJobStatus(SparkAppHandle handle, String appId, long loadJobId, boolean isYarnMaster) {
        EtlStatus status = new EtlStatus();

        // state from handle
        if (!isYarnMaster) {
            if (handle == null) {
                status.setFailMsg("spark app handle is null");
                status.setState(TEtlState.CANCELLED);
                return status;
            }

            State etlJobState = handle.getState();
            switch (etlJobState) {
                case FINISHED:
                    status.setState(TEtlState.FINISHED);
                    break;
                case FAILED:
                case KILLED:
                case LOST:
                    status.setState(TEtlState.CANCELLED);
                    status.setFailMsg("spark app state: " + etlJobState.toString());
                    break;
                default:
                    // UNKNOWN CONNECTED SUBMITTED RUNNING
                    status.setState(TEtlState.RUNNING);
                    break;
            }
            LOG.info("spark app id: {}, load job id: {}, app state: {}", appId, loadJobId, etlJobState);

            return status;
        }

        // state from yarn
        Preconditions.checkNotNull(appId);
        YarnClient client = startYarnClient();
        try {
            ApplicationReport report = client.getApplicationReport(ConverterUtils.toApplicationId(appId));
            LOG.info("yarn application -status {}, load job id: {}, result: {}", appId, loadJobId, report);

            YarnApplicationState etlJobState = report.getYarnApplicationState();
            switch (etlJobState) {
                case FINISHED:
                    status.setState(TEtlState.FINISHED);
                    break;
                case FAILED:
                case KILLED:
                    status.setState(TEtlState.CANCELLED);
                    status.setFailMsg("yarn app state: " + etlJobState.toString());
                    break;
                default:
                    // ACCEPTED NEW NEW_SAVING RUNNING SUBMITTED
                    status.setState(TEtlState.RUNNING);
                    break;
            }

            status.setTrackingUrl(report.getTrackingUrl());
            status.setProgress((int)(report.getProgress() * 100));
        } catch (ApplicationNotFoundException e) {
            LOG.warn("spark app not found, spark app id: {}, load job id: {}", appId, loadJobId, e);
            status.setState(TEtlState.CANCELLED);
            status.setFailMsg(e.getMessage());
        } catch (YarnException | IOException e) {
            LOG.warn("yarn application status failed, spark app id: {}, load job id: {}", appId, loadJobId, e);
        } finally {
            stopYarnClient(client);
        }

        return status;
    }

    public void killEtlJob(SparkAppHandle handle, String appId, boolean isYarnMaster) {
        if (isYarnMaster) {
            Preconditions.checkNotNull(appId);
            YarnClient client = startYarnClient();
            try {
                try {
                    client.killApplication(ConverterUtils.toApplicationId(appId));
                    LOG.info("yarn application -kill {}", appId);
                } catch (YarnException | IOException e) {
                    LOG.warn("yarn application kill failed, app id: {}", appId, e);
                }
            } finally {
                stopYarnClient(client);
            }
        } else {
            if (handle != null) {
                handle.stop();
            }
        }
    }

    public Map<String, Long> getEtlFilePaths(String outputPath, BrokerDesc brokerDesc) throws Exception {
        Map<String, Long> filePathToSize = Maps.newHashMap();

        List<TBrokerFileStatus> fileStatuses = Lists.newArrayList();
        String etlFilePaths = outputPath + "/*";
        try {
            BrokerUtil.parseBrokerFile(etlFilePaths, brokerDesc, fileStatuses);
        } catch (UserException e) {
            throw new Exception(e);
        }

        for (TBrokerFileStatus fstatus : fileStatuses) {
            if (fstatus.isDir) {
                continue;
            }
            filePathToSize.put(fstatus.getPath(), fstatus.getSize());
        }
        LOG.debug("get spark etl file paths. files map: {}", filePathToSize);

        return filePathToSize;
    }

    private YarnClient startYarnClient() {
        Configuration conf = new YarnConfiguration();
        YarnClient client = YarnClient.createYarnClient();
        client.init(conf);
        client.start();
        return client;
    }

    private void stopYarnClient(YarnClient client) {
        client.stop();
    }
}
