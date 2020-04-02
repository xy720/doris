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

    @SerializedName(value = MASTER)
    private String master;
    @SerializedName(value = HDFS_ETL_PATH)
    private String hdfsEtlPath;
    @SerializedName(value = BROKER)
    private String broker;
    @SerializedName(value = SPARK_ARGS)
    private Map<String, String> sparkArgsMap;

    public SparkEtlCluster(String name) {
        this(name, null, null, null, Maps.newHashMap());
    }

    private SparkEtlCluster(String name, String master, String hdfsEtlPath, String broker, Map<String, String> sparkArgsMap) {
        super(name, EtlClusterType.SPARK);
        this.master = master;
        this.hdfsEtlPath = hdfsEtlPath;
        this.broker = broker;
        this.sparkArgsMap = sparkArgsMap;
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

    public SparkEtlCluster getCopiedEtlCluster() {
        return new SparkEtlCluster(name, master, hdfsEtlPath, broker, Maps.newHashMap(sparkArgsMap));
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
            Map<String, String> newArgsMap = getSparkArgsMap(properties.get(SPARK_ARGS));
            for (Map.Entry<String, String> entry : newArgsMap.entrySet()) {
                String argKey = entry.getKey();
                if (sparkArgsMap.containsKey(argKey)) {
                    sparkArgsMap.put(argKey, sparkArgsMap.get(argKey) + COMMA_SEPARATOR + entry.getValue());
                } else {
                    sparkArgsMap.put(argKey, entry.getValue());
                }
            }
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

        sparkArgsMap = getSparkArgsMap(properties.get(SPARK_ARGS));
    }

    private Map<String, String> getSparkArgsMap(String sparkArgsStr) throws DdlException {
        Map<String, String> argsMap = Maps.newHashMap();
        if (Strings.isNullOrEmpty(sparkArgsStr)) {
            return argsMap;
        }

        // jars=xxx.jar,yyy.jar;files=/tmp/aaa,/tmp/bbb
        String[] sparkArgs = sparkArgsStr.split(SEMICOLON_SEPARATOR);
        for (String sparkArgStr : sparkArgs) {
            if (sparkArgStr.isEmpty()) {
                continue;
            }

            String[] sparkArgArr = sparkArgStr.split(EQUAL_SEPARATOR);
            if (sparkArgArr.length != 2 || sparkArgArr[0].isEmpty() || sparkArgArr[1].isEmpty()) {
                throw new DdlException("Spark args format error, use 'key1=value1,value2;key2=value3'");
            }

            argsMap.put(sparkArgArr[0], sparkArgArr[1]);
        }
        return argsMap;
    }
}

