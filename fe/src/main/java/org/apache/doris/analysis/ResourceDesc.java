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

package org.apache.doris.analysis;

import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Resource;
import org.apache.doris.catalog.Resource.ResourceType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.load.EtlJobType;

import java.io.DataInput;
import java.io.IOException;
import java.util.Map;

// Resource descriptor
//
// Spark example:
// WITH RESOURCE "spark0"
// (
//     "spark.jars" = "xxx.jar,yyy.jar",
//     "spark.files" = "/tmp/aaa,/tmp/bbb",
//     "spark.executor.memory" = "1g",
//     "spark.yarn.queue" = "queue0",
//     "spark.hadoop.yarn.resourcemanager.address" = "127.0.0.1:9999",
//     "spark.hadoop.fs.defaultFS" = "hdfs://127.0.0.1:10000"
// )
public class ResourceDesc extends DataProcessorDesc {
    private ResourceDesc() {
        this(null, null);
    }

    public ResourceDesc(String name, Map<String, String> properties) {
        super(name, properties);
    }

    @Override
    public void analyze() throws AnalysisException {
        super.analyze();

        // check etl cluster exist
        if (!Catalog.getCurrentCatalog().getResourceMgr().containsResource(getName())) {
            throw new AnalysisException("Resource does not exist. name: " + getName());
        }
    }

    @Override
    public EtlJobType getEtlJobType() throws DdlException {
        Resource etlCluster = Catalog.getCurrentCatalog().getResourceMgr().getResource(getName());
        if (etlCluster == null) {
            throw new DdlException("Resource does not exist. name: " + getName());
        }

        if (etlCluster.getType() == ResourceType.SPARK) {
            return EtlJobType.SPARK;
        }

        return null;
    }

    public static ResourceDesc read(DataInput in) throws IOException {
        ResourceDesc resourceDesc = new ResourceDesc();
        resourceDesc.readFields(in);
        return resourceDesc;
    }
}