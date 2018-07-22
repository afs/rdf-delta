/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.seaborne.delta.server.local.patchstores.zk;

import static org.seaborne.delta.zk.Zk.zkPath;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.jena.atlas.logging.Log;
import org.seaborne.delta.DataSourceDescription;
import org.seaborne.delta.DeltaConfigException;
import org.seaborne.delta.DeltaConst;
import org.seaborne.delta.server.local.DPS;
import org.seaborne.delta.server.local.LocalServerConfig;
import org.seaborne.delta.server.local.PatchStore;
import org.seaborne.delta.server.local.PatchStoreProvider;
import org.seaborne.delta.server.local.patchstores.PatchLogIndex;
import org.seaborne.delta.server.local.patchstores.PatchStorage;

public class PatchStoreProviderZk implements PatchStoreProvider {

    public PatchStoreProviderZk() { }
    
    private static CuratorFramework makeClient(String connectString) {
        RetryPolicy policy = new ExponentialBackoffRetry(10000, 5);
        if ( connectString == null || connectString.isEmpty() )
            return null;
        
        try {
          CuratorFramework client = CuratorFrameworkFactory.newClient(connectString, policy);

//            CuratorFramework client = CuratorFrameworkFactory.builder()
//                .connectString(connectString)
//                //.connectionHandlingPolicy(ConnectionHandlingPolicy.)
//                .retryPolicy(policy)
//                .build();
            client.start();
            client.blockUntilConnected();
            return client;
        }
        catch (Exception ex) {
            Log.error(PatchStoreProviderZk.class,  "Failed to setup zookeeper backed PatchStore", ex);
            throw new DeltaConfigException("Failed to setup zookeeper backed PatchStore", ex);
        }
    }
    
    @Override
    public PatchStore create(LocalServerConfig config) {
        String connectionString = config.getProperty(DeltaConst.pDeltaZk);
        if ( connectionString == null )
            Log.error(PatchStoreProviderZk.class, "No connection string in configuration"); 
        CuratorFramework client = makeClient(connectionString);
        return new PatchStoreZk(client, this); 
    }

    @Override
    public PatchLogIndex newPatchLogIndex(DataSourceDescription dsd, PatchStore patchStore, LocalServerConfig configuration) {
        PatchStoreZk patchStoreZk = (PatchStoreZk)patchStore;
        String logPath = zkPath(ZkConst.pLogs, dsd.getName());
        return new PatchLogIndexZk(patchStoreZk.getClient(), patchStoreZk.getInstance(), dsd, logPath);
    }

    @Override
    public PatchStorage newPatchStorage(DataSourceDescription dsd, PatchStore patchStore, LocalServerConfig configuration) {
        PatchStoreZk patchStoreZk = (PatchStoreZk)patchStore;
        String logPath = zkPath(ZkConst.pLogs, dsd.getName());
        return new PatchStorageZk(patchStoreZk.getClient(), patchStoreZk.getInstance(), logPath);
    }

    @Override
    public String getProviderName() {
        return DPS.PatchStoreZkProvider;
    }
    
    @Override
    public String getShortName() {
        return DPS.pspZk;
    }
}
