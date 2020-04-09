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

package org.apache.doris.catalog;

import org.apache.doris.analysis.EtlClusterDesc;
import org.apache.doris.common.DdlException;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;

import java.util.Map;

public class SparkEtlCluster extends EtlCluster {
    public static final String SEMICOLON_SEPARATOR = ";";
    public static final String EQUAL_SEPARATOR = "=";
    public static final String COMMA_SEPARATOR = ",";

    public static final String MASTER = "master";
    public static final String HDFS_ETL_PATH = "hdfs_etl_path";
    public static final String BROKER = "broker";
    public static final String SPARK_ARGS = "spark_args";
    public static final String YARN_CONFIGS = "yarn_configs";

    private static final String YARN_MASTER = "yarn";
    private static final String YARN_RESOURCE_MANAGER_ADDRESS = "yarn.resourcemanager.address";
    private static final String FS_DEFAULT_FS = "fs.defaultFS";

    @SerializedName(value = MASTER)
    private String master;
    @SerializedName(value = HDFS_ETL_PATH)
    private String hdfsEtlPath;
    @SerializedName(value = BROKER)
    private String broker;
    @SerializedName(value = SPARK_ARGS)
    private Map<String, String> sparkArgsMap;
    @SerializedName(value = YARN_CONFIGS)
    private Map<String, String> yarnConfigsMap;

    public SparkEtlCluster(String name) {
        this(name, null, null, null, Maps.newHashMap(), Maps.newHashMap());
    }

    private SparkEtlCluster(String name, String master, String hdfsEtlPath, String broker,
                            Map<String, String> sparkArgsMap, Map<String, String> yarnConfigsMap) {
        super(name, EtlClusterType.SPARK);
        this.master = master;
        this.hdfsEtlPath = hdfsEtlPath;
        this.broker = broker;
        this.sparkArgsMap = sparkArgsMap;
        this.yarnConfigsMap = yarnConfigsMap;
    }

    public String getMaster() {
        return master;
    }

    public String getHdfsEtlPath() {
        return hdfsEtlPath;
    }

    public String getBroker() {
        return broker;
    }

    public Map<String, String> getSparkArgsMap() {
        return sparkArgsMap;
    }

    public Map<String, String> getYarnConfigsMap() {
        return yarnConfigsMap;
    }

    public SparkEtlCluster getCopiedEtlCluster() {
        return new SparkEtlCluster(name, master, hdfsEtlPath, broker, Maps.newHashMap(sparkArgsMap),
                                   Maps.newHashMap(yarnConfigsMap));
    }

    public boolean isYarnMaster() {
        return master.equalsIgnoreCase(YARN_MASTER);
    }

    public void update(EtlClusterDesc etlClusterDesc) throws DdlException {
        Preconditions.checkState(name.equals(etlClusterDesc.getName()));

        Map<String, String> properties = etlClusterDesc.getProperties();
        if (properties == null) {
            return;
        }

        // replace master, hdfsEtlPath, broker if exist in etlClusterDesc
        if (properties.containsKey(MASTER)) {
            master = properties.get(MASTER);
        }
        if (properties.containsKey(HDFS_ETL_PATH)) {
            hdfsEtlPath = properties.get(hdfsEtlPath);
        }
        if (properties.containsKey(broker)) {
            broker = properties.get(broker);
        }

        // merge spark args
        if (properties.containsKey(SPARK_ARGS)) {
            Map<String, String> newArgsMap = getArgsMap(properties, SPARK_ARGS);
            for (Map.Entry<String, String> entry : newArgsMap.entrySet()) {
                String argKey = entry.getKey();
                if (sparkArgsMap.containsKey(argKey)) {
                    sparkArgsMap.put(argKey, sparkArgsMap.get(argKey) + COMMA_SEPARATOR + entry.getValue());
                } else {
                    sparkArgsMap.put(argKey, entry.getValue());
                }
            }
        }

        // merge yarn configs
        if (properties.containsKey(YARN_CONFIGS)) {
            Map<String, String> newConfigsMap = getYarnConfigsMap(properties, YARN_CONFIGS);
            yarnConfigsMap.putAll(newConfigsMap);
        }
    }

    @Override
    protected void setProperties(Map<String, String> properties) throws DdlException {
        Preconditions.checkState(properties != null);

        master = properties.get(MASTER);
        if (master == null) {
            throw new DdlException("Missing spark master in properties");
        }
        hdfsEtlPath = properties.get(HDFS_ETL_PATH);
        if (hdfsEtlPath == null) {
            throw new DdlException("Missing hdfs etl path in properties");
        }

        broker = properties.get(BROKER);
        if (broker == null) {
            throw new DdlException("Missing broker in properties");
        }
        // check broker exist
        if (!Catalog.getInstance().getBrokerMgr().contaisnBroker(broker)) {
            throw new DdlException("Unknown broker name(" + broker + ")");
        }

        sparkArgsMap = getSparkArgsMap(properties, SPARK_ARGS);
        yarnConfigsMap = getYarnConfigsMap(properties, YARN_CONFIGS);
    }

    private Map<String, String> getSparkArgsMap(Map<String, String> properties, String argsKey) throws DdlException {
        Map<String, String> sparkArgsMap = getArgsMap(properties, argsKey);
        for (Map.Entry<String, String> entry : sparkArgsMap.entrySet()) {
            if (!entry.getKey().startsWith("--")) {
                throw new DdlException(argsKey + " format error, use '--key1=value1,value2;--key2=value3'");
            }
        }
        return sparkArgsMap;
    }

    private Map<String, String> getYarnConfigsMap(Map<String, String> properties, String argsKey) throws DdlException {
        Map<String, String> yarnConfigsMap = getArgsMap(properties, argsKey);
        if ((!yarnConfigsMap.containsKey(YARN_RESOURCE_MANAGER_ADDRESS) || !yarnConfigsMap.containsKey(FS_DEFAULT_FS))
                && isYarnMaster()) {
            throw new DdlException("Missing " + argsKey + "(" + YARN_RESOURCE_MANAGER_ADDRESS + " and " + FS_DEFAULT_FS
                                           + ") in yarn master");
        }
        return yarnConfigsMap;
    }

    private Map<String, String> getArgsMap(Map<String, String> properties, String argsKey) throws DdlException {
        Map<String, String> argsMap = Maps.newHashMap();

        String argsStr = properties.get(argsKey);
        if (Strings.isNullOrEmpty(argsStr)) {
            return argsMap;
        }

        // --jars=xxx.jar,yyy.jar;--files=/tmp/aaa,/tmp/bbb
        // yarn.resourcemanager.address=host:port;fs.defaultFS=hdfs://host:port
        String[] args = argsStr.split(SEMICOLON_SEPARATOR);
        for (String argStr : args) {
            if (argStr.trim().isEmpty()) {
                continue;
            }

            String[] argArr = argStr.trim().split(EQUAL_SEPARATOR);
            if (argArr.length != 2 || argArr[0].isEmpty() || argArr[1].isEmpty()) {
                throw new DdlException(argsKey + " format error, use 'key1=value1;key2=value2'");
            }
            argsMap.put(argArr[0], argArr[1]);
        }
        return argsMap;
    }
}

