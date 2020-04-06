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

import com.google.common.base.Preconditions;
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

    private static final String JOB_CONFIG_DIR = PaloFe.DORIS_HOME_DIR + "/temp/job_conf";
    private static final String APP_RESOURCE = PaloFe.DORIS_HOME_DIR + "/lib/palo-fe.jar";
    private static final String MAIN_CLASS = "org.apache.doris.load.loadv2.etl.SparkEtlJob";
    private static final String SPARK_DEPLOY_MODE = "cluster";
    private static final String ETL_JOB_NAME = "doris__%s";
    // 5min
    private static final int GET_APPID_MAX_RETRY = 300;

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
        // delete when etl job finished or cancelled
        String configFilePath = createJobConfigFile(loadJobId, jobJsonConfig);

        // spark cluster config
        SparkLauncher launcher = new SparkLauncher();
        launcher.setMaster(etlCluster.getMaster())
                .setDeployMode(SPARK_DEPLOY_MODE)
                .setAppResource(APP_RESOURCE)
                .setMainClass(MAIN_CLASS)
                .setAppName(String.format(ETL_JOB_NAME, loadLabel))
                .addFile(configFilePath);
        // spark args: --jars, --files, --queue
        for (Map.Entry<String, String> entry : etlCluster.getSparkArgsMap().entrySet()) {
            launcher.addSparkArg(entry.getKey(), entry.getValue());
        }

        // start app
        SparkAppHandle handle = null;
        State state = null;
        String appId = null;
        int retry = 0;
        String errMsg = "start spark app failed. error: ";
        try {
            handle = launcher.startApplication(new SparkAppListener());
        } catch (IOException e) {
            LOG.warn(errMsg, e);
            throw new LoadException(errMsg + e.getMessage());
        }

        while (retry++ < GET_APPID_MAX_RETRY) {
            appId = handle.getAppId();
            if (appId != null) {
                break;
            }

            // check state and retry
            state = handle.getState();
            if (fromSparkState(state) == TEtlState.CANCELLED) {
                throw new LoadException(errMsg + "spark app state: " + state.toString());
            }
            if (retry >= GET_APPID_MAX_RETRY) {
                throw new LoadException(errMsg + "wait too much time for getting appid, spark app state: "
                                                + state.toString());
            }

            // log
            if (retry % 10 == 0) {
                LOG.info("spark appid that handle get is null, state: {}, retry times: {}",
                         state.toString(), retry);
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                LOG.warn(e.getMessage());
            }
        }

        // success
        attachment.setAppId(appId);
        attachment.setHandle(handle);
    }

    private String createJobConfigFile(long loadJobId, String jsonConfig) throws LoadException {
        // check config dir
        String configDirPath = JOB_CONFIG_DIR + "/" + loadJobId;
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
        String configFilePath = configDirPath + "/" + EtlJobConfig.JOB_CONFIG_FILE_NAME;
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

        return configFilePath;
    }

    public EtlStatus getEtlJobStatus(SparkAppHandle handle, String appId, long loadJobId, boolean isYarnMaster) {
        EtlStatus status = new EtlStatus();

        if (isYarnMaster) {
            // state from yarn
            Preconditions.checkState(appId != null && !appId.isEmpty());
            YarnClient client = startYarnClient();
            try {
                ApplicationReport report = client.getApplicationReport(ConverterUtils.toApplicationId(appId));
                LOG.info("yarn application -status {}, load job id: {}, result: {}", appId, loadJobId, report);

                YarnApplicationState state = report.getYarnApplicationState();
                status.setState(fromYarnState(state));
                if (status.getState() == TEtlState.CANCELLED) {
                    status.setFailMsg("yarn app state: " + state.toString());
                }
                status.setTrackingUrl(report.getTrackingUrl());
                status.setProgress((int) (report.getProgress() * 100));
            } catch (ApplicationNotFoundException e) {
                LOG.warn("spark app not found, spark app id: {}, load job id: {}", appId, loadJobId, e);
                status.setState(TEtlState.CANCELLED);
                status.setFailMsg(e.getMessage());
            } catch (YarnException | IOException e) {
                LOG.warn("yarn application status failed, spark app id: {}, load job id: {}", appId, loadJobId, e);
            } finally {
                stopYarnClient(client);
            }
        } else {
            // state from handle
            if (handle == null) {
                status.setFailMsg("spark app handle is null");
                status.setState(TEtlState.CANCELLED);
                return status;
            }

            State state = handle.getState();
            status.setState(fromSparkState(state));
            if (status.getState() == TEtlState.CANCELLED) {
                status.setFailMsg("spark app state: " + state.toString());
            }
            LOG.info("spark app id: {}, load job id: {}, app state: {}", appId, loadJobId, state);
        }

        // delete config file
        if (status.getState() == TEtlState.FINISHED || status.getState() == TEtlState.CANCELLED) {
            deleteConfigFile(loadJobId);
        }

        return status;
    }

    public void killEtlJob(SparkAppHandle handle, String appId, long loadJobId, boolean isYarnMaster) {
        if (isYarnMaster) {
            Preconditions.checkNotNull(appId);
            YarnClient client = startYarnClient();
            try {
                try {
                    client.killApplication(ConverterUtils.toApplicationId(appId));
                    LOG.info("yarn application -kill {}", appId);
                } catch (YarnException | IOException e) {
                    LOG.warn("yarn application kill failed, app id: {}, load job id: {}", appId, loadJobId, e);
                }
            } finally {
                stopYarnClient(client);
            }
        } else {
            if (handle != null) {
                handle.stop();
            }
        }

        // delete config file
        deleteConfigFile(loadJobId);
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

    private TEtlState fromYarnState(YarnApplicationState state) {
        switch (state) {
            case FINISHED:
                return TEtlState.FINISHED;
            case FAILED:
            case KILLED:
                return TEtlState.CANCELLED;
            default:
                // ACCEPTED NEW NEW_SAVING RUNNING SUBMITTED
                return TEtlState.RUNNING;
        }
    }

    private TEtlState fromSparkState(State state) {
        switch (state) {
            case FINISHED:
                return TEtlState.FINISHED;
            case FAILED:
            case KILLED:
            case LOST:
                return TEtlState.CANCELLED;
            default:
                // UNKNOWN CONNECTED SUBMITTED RUNNING
                return TEtlState.RUNNING;
        }
    }

    private void deleteConfigFile(long loadJobId) {
        String configDirPath = JOB_CONFIG_DIR + "/" + loadJobId;
        Util.deleteDirectory(new File(configDirPath));
    }
}
