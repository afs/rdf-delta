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

import java.util.ArrayList;
import java.util.List;

import org.apache.jena.atlas.web.HttpException;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.tdb.base.file.Location;
import org.seaborne.delta.client.DeltaClient;
import org.seaborne.delta.client.DeltaLinkHTTP;
import org.seaborne.delta.client.DeltaLinkSwitchable;
import org.seaborne.delta.client.LocalStorageType;
import org.seaborne.delta.client.SyncPolicy;
import org.seaborne.delta.client.Zone;
import org.seaborne.delta.link.DeltaLink;

public class LibBuildDC {


    /** Build a Delta-backed dataset, with Delta at a zone location and having an external dataset. */
    public static DatasetGraph setupDataset(String dsName, Location zoneLocation, DatasetGraph externalDataset, List<String> destURLs) {
        return setupDataset(dsName, zoneLocation, LocalStorageType.EXTERNAL, externalDataset, destURLs);
    }

    /** Build a Delta-backed datasets at a zone location, with managed storage. */
    public static DatasetGraph setupDataset(String dsName, Location zoneLocation, LocalStorageType storage, List<String> destURLs) {
        return setupDataset(dsName, zoneLocation, storage, null, destURLs);
    }

    /** Build a Delta-backed datasets at a zone location. */
    public static DatasetGraph setupDataset(String dsName, Location zoneLocation, LocalStorageType storage, DatasetGraph externalDataset, List<String> destURLs) {
        // Link to log server.
        DeltaLink deltaLink;
        if ( destURLs.size() == 1 )
            deltaLink = DeltaLinkHTTP.connect(destURLs.get(0));
        else {
            List<DeltaLink> links = new ArrayList<>(destURLs.size());
            for ( String destURL  : destURLs )
                links.add(DeltaLinkHTTP.connect(destURL));
            deltaLink = new DeltaLinkSwitchable(links);
        }

        Zone zone = Zone.connect(zoneLocation);
        DeltaClient deltaClient = DeltaClient.create(zone, deltaLink);
        SyncPolicy syncPolicy = SyncPolicy.TXN_RW;
        try { deltaLink.ping(); }
        catch (HttpException ex) {
            // rc < 0 : failed to connect - ignore?
            if ( ex.getStatusCode() > 0 )
                throw ex;
        }

        DatasetGraph dsg = ManagedDatasetBuilder.create()
            .deltaLink(deltaLink)
            .logName(dsName)
            .zone(zone)
            .syncPolicy(syncPolicy)
            .storageType(storage)
            .externalDataset(externalDataset)
            .build();
        return dsg;
     }

}
