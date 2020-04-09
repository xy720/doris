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

package org.apache.doris.common.util;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.doris.analysis.BrokerDesc;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.FsBroker;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ClientPool;
import org.apache.doris.common.Config;
import org.apache.doris.common.UserException;
import org.apache.doris.service.FrontendOptions;
import org.apache.doris.thrift.TBrokerCloseReaderRequest;
import org.apache.doris.thrift.TBrokerDeletePathRequest;
import org.apache.doris.thrift.TBrokerFD;
import org.apache.doris.thrift.TBrokerFileStatus;
import org.apache.doris.thrift.TBrokerListPathRequest;
import org.apache.doris.thrift.TBrokerListResponse;
import org.apache.doris.thrift.TBrokerOpenReaderRequest;
import org.apache.doris.thrift.TBrokerOpenReaderResponse;
import org.apache.doris.thrift.TBrokerOperationStatus;
import org.apache.doris.thrift.TBrokerOperationStatusCode;
import org.apache.doris.thrift.TBrokerPReadRequest;
import org.apache.doris.thrift.TBrokerReadResponse;
import org.apache.doris.thrift.TBrokerVersion;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TPaloBrokerService;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.TException;

import java.io.UnsupportedEncodingException;
import java.util.Collections;
import java.util.List;

public class BrokerUtil {
    private static final Logger LOG = LogManager.getLogger(BrokerUtil.class);

    public static void parseBrokerFile(String path, BrokerDesc brokerDesc, List<TBrokerFileStatus> fileStatuses)
            throws UserException {
        TNetworkAddress address = getAddress(brokerDesc);
        TPaloBrokerService.Client client = borrowClient(address);
        boolean failed = true;
        try {
            TBrokerListPathRequest request = new TBrokerListPathRequest(
                    TBrokerVersion.VERSION_ONE, path, false, brokerDesc.getProperties());
            TBrokerListResponse tBrokerListResponse = null;
            try {
                tBrokerListResponse = client.listPath(request);
            } catch (TException e) {
                reopenClient(client);
                tBrokerListResponse = client.listPath(request);
            }
            if (tBrokerListResponse.getOpStatus().getStatusCode() != TBrokerOperationStatusCode.OK) {
                throw new UserException("Broker list path failed. path=" + path
                        + ",broker=" + address + ",msg=" + tBrokerListResponse.getOpStatus().getMessage());
            }
            failed = false;
            for (TBrokerFileStatus tBrokerFileStatus : tBrokerListResponse.getFiles()) {
                if (tBrokerFileStatus.isDir) {
                    continue;
                }
                fileStatuses.add(tBrokerFileStatus);
            }
        } catch (TException e) {
            LOG.warn("Broker list path exception, path={}, address={}, exception={}", path, address, e);
            throw new UserException("Broker list path exception.path=" + path + ",broker=" + address);
        } finally {
            returnClient(client, address, failed);
        }
    }

    public static String printBroker(String brokerName, TNetworkAddress address) {
        return brokerName + "[" + address.toString() + "]";
    }

    public static List<String> parseColumnsFromPath(String filePath, List<String> columnsFromPath) throws UserException {
        if (columnsFromPath == null || columnsFromPath.isEmpty()) {
            return Collections.emptyList();
        }
        String[] strings = filePath.split("/");
        if (strings.length < 2) {
            throw new UserException("Fail to parse columnsFromPath, expected: " + columnsFromPath + ", filePath: " + filePath);
        }
        String[] columns = new String[columnsFromPath.size()];
        int size = 0;
        for (int i = strings.length - 2; i >= 0; i--) {
            String str = strings[i];
            if (str != null && str.isEmpty()) {
                continue;
            }
            if (str == null || !str.contains("=")) {
                throw new UserException("Fail to parse columnsFromPath, expected: " + columnsFromPath + ", filePath: " + filePath);
            }
            String[] pair = str.split("=", 2);
            if (pair.length != 2) {
                throw new UserException("Fail to parse columnsFromPath, expected: " + columnsFromPath + ", filePath: " + filePath);
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
            throw new UserException("Fail to parse columnsFromPath, expected: " + columnsFromPath + ", filePath: " + filePath);
        }
        return Lists.newArrayList(columns);
    }

    public static String readBrokerFile(String path, BrokerDesc brokerDesc) throws UserException {
        TNetworkAddress address = getAddress(brokerDesc);
        TPaloBrokerService.Client client = borrowClient(address);
        boolean failed = true;
        TBrokerFD fd = null;
        try {
            // get file size
            TBrokerListPathRequest request = new TBrokerListPathRequest(
                    TBrokerVersion.VERSION_ONE, path, false, brokerDesc.getProperties());
            TBrokerListResponse tBrokerListResponse = null;
            try {
                tBrokerListResponse = client.listPath(request);
            } catch (TException e) {
                reopenClient(client);
                tBrokerListResponse = client.listPath(request);
            }
            if (tBrokerListResponse.getOpStatus().getStatusCode() != TBrokerOperationStatusCode.OK) {
                throw new UserException("Broker list path failed. path=" + path + ", broker=" + address
                                                + ",msg=" + tBrokerListResponse.getOpStatus().getMessage());
            }
            failed = false;
            List<TBrokerFileStatus> fileStatuses = tBrokerListResponse.getFiles();
            if (fileStatuses.size() != 1) {
                throw new UserException("Broker files num error. path=" + path + ", broker=" + address
                                                + ", files num: " + fileStatuses.size());
            }

            Preconditions.checkState(!fileStatuses.get(0).isIsDir());
            long fileSize = fileStatuses.get(0).getSize();

            // open reader
            String clientId = FrontendOptions.getLocalHostAddress() + ":" + Config.rpc_port;
            TBrokerOpenReaderRequest tOpenReaderRequest = new TBrokerOpenReaderRequest(
                    TBrokerVersion.VERSION_ONE, path, 0, clientId, brokerDesc.getProperties());
            TBrokerOpenReaderResponse tOpenReaderResponse = null;
            try {
                tOpenReaderResponse = client.openReader(tOpenReaderRequest);
            } catch (TException e) {
                reopenClient(client);
                tOpenReaderResponse = client.openReader(tOpenReaderRequest);
            }
            if (tOpenReaderResponse.getOpStatus().getStatusCode() != TBrokerOperationStatusCode.OK) {
                throw new UserException("Broker open reader failed. path=" + path + ", broker=" + address
                                                + ", msg=" + tOpenReaderResponse.getOpStatus().getMessage());
            }
            fd = tOpenReaderResponse.getFd();

            // read
            TBrokerPReadRequest tPReadRequest = new TBrokerPReadRequest(
                    TBrokerVersion.VERSION_ONE, fd, 0, fileSize);
            TBrokerReadResponse tReadResponse = null;
            try {
                tReadResponse = client.pread(tPReadRequest);
            } catch (TException e) {
                reopenClient(client);
                tReadResponse = client.pread(tPReadRequest);
            }
            if (tReadResponse.getOpStatus().getStatusCode() != TBrokerOperationStatusCode.OK) {
                throw new UserException("Broker read failed. path=" + path + ", broker=" + address
                                                + ", msg=" + tReadResponse.getOpStatus().getMessage());
            }
            byte[] data = tReadResponse.getData();
            return new String(data, "UTF-8");
        } catch (TException | UnsupportedEncodingException e) {
            LOG.warn("Broker read path exception, path={}, address={}, exception={}", path, address, e);
            throw new UserException("Broker read path exception.path=" + path + ",broker=" + address);
        } finally {
            // close reader
            if (fd != null) {
                TBrokerCloseReaderRequest tCloseReaderRequest = new TBrokerCloseReaderRequest(
                        TBrokerVersion.VERSION_ONE, fd);
                TBrokerOperationStatus tOperationStatus = null;
                try {
                    tOperationStatus = client.closeReader(tCloseReaderRequest);
                } catch (TException e) {
                    reopenClient(client);
                    try {
                        tOperationStatus = client.closeReader(tCloseReaderRequest);
                    } catch (TException e1) {
                        LOG.warn("Broker close reader failed. path={}, address={}", path, address, e1);
                    }
                    if (tOperationStatus.getStatusCode() != TBrokerOperationStatusCode.OK) {
                        LOG.warn("Broker close reader failed. path={}, address={}, error={}", path, address,
                                 tOperationStatus.getMessage());
                    }
                }
            }

            // return client
            returnClient(client, address, failed);
        }
    }

    public static void deleteBrokerPath(String path, BrokerDesc brokerDesc) throws UserException {
        TNetworkAddress address = getAddress(brokerDesc);
        TPaloBrokerService.Client client = borrowClient(address);
        boolean failed = true;
        try {
            TBrokerDeletePathRequest tDeletePathRequest = new TBrokerDeletePathRequest(
                    TBrokerVersion.VERSION_ONE, path, brokerDesc.getProperties());
            TBrokerOperationStatus tOperationStatus = null;
            try {
                tOperationStatus = client.deletePath(tDeletePathRequest);
            } catch (TException e) {
                reopenClient(client);
                tOperationStatus = client.deletePath(tDeletePathRequest);
            }
            if (tOperationStatus.getStatusCode() != TBrokerOperationStatusCode.OK) {
                throw new UserException("Broker delete path failed.path=" + path + ", broker=" + address
                                                + ", msg=" + tOperationStatus.getMessage());
            }
            failed = false;
        } catch (TException e) {
            LOG.warn("Broker read path exception, path={}, address={}, exception={}", path, address, e);
            throw new UserException("Broker read path exception.path=" + path + ",broker=" + address);
        } finally {
            returnClient(client, address, failed);
        }
    }

    private static TNetworkAddress getAddress(BrokerDesc brokerDesc) throws UserException {
        FsBroker broker = null;
        try {
            String localIP = FrontendOptions.getLocalHostAddress();
            broker = Catalog.getInstance().getBrokerMgr().getBroker(brokerDesc.getName(), localIP);
        } catch (AnalysisException e) {
            throw new UserException(e.getMessage());
        }
        return new TNetworkAddress(broker.ip, broker.port);
    }

    private static TPaloBrokerService.Client borrowClient(TNetworkAddress address) throws UserException {

        TPaloBrokerService.Client client = null;
        try {
            client = ClientPool.brokerPool.borrowObject(address);
        } catch (Exception e) {
            try {
                client = ClientPool.brokerPool.borrowObject(address);
            } catch (Exception e1) {
                throw new UserException("Create connection to broker(" + address + ") failed.");
            }
        }
        return client;
    }

    private static void returnClient(TPaloBrokerService.Client client, TNetworkAddress address, boolean failed) {
        if (failed) {
            ClientPool.brokerPool.invalidateObject(address, client);
        } else {
            ClientPool.brokerPool.returnObject(address, client);
        }
    }

    private static void reopenClient(TPaloBrokerService.Client client) {
        ClientPool.brokerPool.reopen(client);
    }
}
