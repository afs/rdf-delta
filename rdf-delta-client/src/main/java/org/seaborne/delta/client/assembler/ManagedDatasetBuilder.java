/*
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 *  See the NOTICE file distributed with this work for additional
 *  information regarding copyright ownership.
 */

package org.seaborne.delta.client.assembler;

import static org.seaborne.delta.DeltaConst.symDeltaClient;
import static org.seaborne.delta.DeltaConst.symDeltaConnection;
import static org.seaborne.delta.DeltaConst.symDeltaZone;

import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.util.Context;
import org.seaborne.delta.DataSourceDescription;
import org.seaborne.delta.DeltaConfigException;
import org.seaborne.delta.Id;
import org.seaborne.delta.client.*;
import org.seaborne.delta.link.DeltaLink;

/** Builder for {@link DatasetGraph} connected to a patch server over a {@link DeltaLink}. */
public class ManagedDatasetBuilder {
    public static ManagedDatasetBuilder create() { return new ManagedDatasetBuilder(); }
    protected ManagedDatasetBuilder() {}

    private String logName;
    private DeltaLink deltaLink;
    private SyncPolicy syncPolicy;
    private Zone zone;
    private LocalStorageType storageType;
    private DatasetGraph externalDataset;

    public ManagedDatasetBuilder logName(String logName) {
        this.logName = logName;
        return this;
    }

    public ManagedDatasetBuilder deltaLink(DeltaLink deltaLink) {
        this.deltaLink = deltaLink;
        return this;
    }

    public ManagedDatasetBuilder syncPolicy(SyncPolicy syncPolicy) {
        this.syncPolicy = syncPolicy;
        return this;
    }

    public ManagedDatasetBuilder zone(Zone zone) {
        this.zone = zone;
        return this;
    }

    public ManagedDatasetBuilder storageType(LocalStorageType storageType) {
        this.storageType = storageType;
        return this;
    }

    public ManagedDatasetBuilder externalDataset(DatasetGraph dataset) {
        this.externalDataset = dataset;
        return this;
    }

    public DatasetGraph build() {
        if ( zone == null )         throw new DeltaConfigException("zone not set");
        if ( deltaLink == null )    throw new DeltaConfigException("deltaLink not set");
        if ( logName == null )      throw new DeltaConfigException("logName not set");
        if ( syncPolicy == null )   throw new DeltaConfigException("syncPolicy not set");

        if ( externalDataset != null && storageType == null )
            this.storageType = LocalStorageType.EXTERNAL;

        if ( externalDataset != null && storageType != LocalStorageType.EXTERNAL )
            throw new DeltaConfigException("External dataset but storage is not 'external'");

        DeltaClient deltaClient = DeltaClient.create(zone, deltaLink);
        deltaLink.ping();

        DataSourceDescription dsd = deltaLink.getDataSourceDescriptionByName(logName);
        Id dsRef;
        if ( dsd == null )
            dsRef = deltaClient.newDataSource(logName, "delta:"+logName);
        else
            dsRef = dsd.getId();
        if ( zone.exists(dsRef)) {
            if ( externalDataset == null )
                deltaClient.connect(dsRef, syncPolicy);
            else
                deltaClient.connectExternal(dsRef, externalDataset, syncPolicy);
        }
        else {
            // Zones are always cached ahead of time by the startup disk scan.
            // Should not be here if restarting and now using a zone.
            // storageType required because we will create a new zone-log.
            if ( storageType == null )
                throw new DeltaConfigException("storageType not set");
            // Register, which is "attach" then "connect".
            // "attach" creates the zone dataset storage.
            if ( externalDataset == null )
                deltaClient.register(dsRef, storageType, syncPolicy);
            else
                deltaClient.registerExternal(dsRef, externalDataset, syncPolicy);
        }

        DeltaConnection deltaConnection = deltaClient.getLocal(dsRef);
        DatasetGraph dsg = deltaConnection.getDatasetGraph();
        // Put state into dsg Context "for the record".
        Context cxt = dsg.getContext();
        cxt.set(symDeltaClient, deltaClient);
        cxt.set(symDeltaConnection, deltaConnection);
        cxt.set(symDeltaZone, zone);
        return dsg;
    }

}