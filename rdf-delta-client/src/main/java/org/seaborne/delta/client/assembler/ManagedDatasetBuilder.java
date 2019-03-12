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
import org.seaborne.delta.client.DeltaClient;
import org.seaborne.delta.client.DeltaConnection;
import org.seaborne.delta.client.LocalStorageType;
import org.seaborne.delta.client.SyncPolicy;
import org.seaborne.delta.client.Zone;
import org.seaborne.delta.link.DeltaLink;

// "Maker"- like a builder but not mimicking the fields.
public class ManagedDatasetBuilder {
    public static ManagedDatasetBuilder create() { return new ManagedDatasetBuilder(); }
    protected ManagedDatasetBuilder() {}

    private String logName;
    private DeltaLink deltaLink;
    private SyncPolicy syncPolicy;
    private Zone zone;
    private LocalStorageType storageType;

    // Convenience methods.
    // These do some setup and call the core setters.


    // Core setters

    public  ManagedDatasetBuilder logName(String logName) {
        this.logName = logName;
        return this;
    }

    public  ManagedDatasetBuilder deltaLink(DeltaLink deltaLink) {
        this.deltaLink = deltaLink;
        return this;
    }

    public  ManagedDatasetBuilder syncPolicy(SyncPolicy syncPolicy) {
        this.syncPolicy = syncPolicy;
        return this;
    }

    public  ManagedDatasetBuilder zone(Zone zone) {
        this.zone = zone;
        return this;
    }

    public  ManagedDatasetBuilder storageType(LocalStorageType storageType) {
        this.storageType = storageType;
        return this;
    }

    public DatasetGraph build() {
        if ( zone == null )         throw new DeltaConfigException("zone not set");
        if ( deltaLink == null )    throw new DeltaConfigException("deltaLink not set");
        if ( logName == null )      throw new DeltaConfigException("logName not set");
        if ( syncPolicy == null )   throw new DeltaConfigException("syncPolicy not set");

        DeltaClient deltaClient = DeltaClient.create(zone, deltaLink);
        deltaLink.ping();

        DataSourceDescription dsd = deltaLink.getDataSourceDescriptionByName(logName);
        Id dsRef;
        if ( dsd == null )
            dsRef = deltaClient.newDataSource(logName, "delta:"+logName);
        else
            dsRef = dsd.getId();
        if ( zone.exists(dsRef))
            // Connect - for when the zone already has the dataset.
            deltaClient.connect(dsRef, syncPolicy);
        else {
            // Zones are always cached ahead of time by the startup disk scan.
            // Should not be here if restarting and now using a zone.
            // storageType required because we will create a new zone-log.
            if ( storageType == null )
                throw new DeltaConfigException("storageType not set");
            // Register (which is attach then connect).
            // "attach" creates the zone dataset storage.
            deltaClient.register(dsRef, storageType, syncPolicy);
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