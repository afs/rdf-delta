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

package org.seaborne.delta.client;

import java.math.BigInteger;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import org.apache.jena.atlas.logging.FmtLog;
import org.apache.jena.atlas.logging.Log;
import org.apache.jena.atlas.web.HttpException;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.system.Txn;
import org.seaborne.delta.Delta;
import org.seaborne.delta.DeltaOps;
import org.seaborne.delta.link.DeltaLink;
import org.seaborne.delta.link.Id;
import org.seaborne.patch.RDFChanges;
import org.seaborne.patch.RDFPatch ;
import org.seaborne.patch.changes.RDFChangesApply ;
import org.seaborne.patch.system.DatasetGraphChanges;
import org.slf4j.Logger;
import txnx.TransPInteger;

/** Provides an interface to a specific dataset over the general {@link DeltaLink} API. 
 * This is the client API, c.f. JDBC connection
 */ 
public class DeltaConnection {
    
    // Need local version tracking?
    //   yes - for bring DSG up to date.
    
    // Or via DeltaLink?
    
    
    private static Logger LOG = Delta.DELTA_LOG;
    
    // The version of the remote copy.
    
    private final DeltaLink link ;
    
    private final AtomicInteger remoteEpoch = new AtomicInteger(0);
    private final AtomicInteger localEpoch = new AtomicInteger(0);
    private final TransPInteger localEpochPersistent;

    private final DatasetGraph base;
    private final DatasetGraphChanges managed;
    
    private final RDFChanges target;
    private final String label;
    private final Id clientId;
    private final Id datasourceId;
    
    public static DeltaConnection create(String label, Id clientId, Id datasourceId, DatasetGraph dsg, DeltaLink link) {
        Objects.requireNonNull(datasourceId, "Null data source Id");
        Objects.requireNonNull(link, "Null link");
        
        DeltaConnection client = new DeltaConnection(label, clientId, datasourceId, dsg, link);
        client.start();
        FmtLog.info(Delta.DELTA_LOG, "%s", client);
        return client;
    }
    
    private DeltaConnection(String label, Id clientId, Id datasourceId, DatasetGraph dsg, DeltaLink link) {
        localEpochPersistent = new TransPInteger(label);
        localEpoch.set(0);
        
        if ( dsg instanceof DatasetGraphChanges )
            Log.warn(this.getClass(), "DatasetGraphChanges passed into DeltaClient");
        
        this.label = label;
        this.base = dsg;
        this.datasourceId = datasourceId ;
        this.clientId = clientId;
        this.link = link;
        
        if ( dsg != null  ) {
            // Where to put incoming changes. 
            this.target = new RDFChangesApply(dsg);
            // Where to send outgoing changes.
            // Make RDFChangesHTTP one shot.
            RDFChanges monitor = link.createRDFChanges(datasourceId);
            this.managed = new DatasetGraphChanges(dsg, monitor);
        } else {
            this.target = null;
            this.managed = null;
        }
    }
    
    public void start() {
        register(); 
        sync();
    }
    
    public void finish() { }

    private void register() {
        link.register(clientId);
    }
    
    public void sync() {
        // [Delta] replace with a one-shot "get all missing patches" operation.

        // Their update id.
        int remoteVer;
        try {
            remoteVer = getRemoteVersionLatest();
        } catch (HttpException ex) {
            // Much the same as : ex.getResponse() == null; HTTP didn't do its thing.
            if ( ex.getCause() instanceof java.net.ConnectException ) {
                FmtLog.warn(LOG, "Failed to connect to get remote version: "+ex.getMessage());
                return;
            }
            if ( ex.getStatusLine() != null ) {
                FmtLog.warn(LOG, "Failed; "+ex.getStatusLine());
                return;
            }
            FmtLog.warn(LOG, "Failed to get remote version: "+ex.getMessage());
            throw ex;
        }

        int localVer = getLocalVersionNumber();

        //FmtLog.info(LOG, "Versions : [%d, %d]", localVer, remoteVer);

        if ( localVer > remoteVer ) 
            FmtLog.info(LOG, "Local version ahead of remote : [%d, %d]", localEpoch, remoteEpoch);
        if ( localVer >= remoteVer ) {
            //FmtLog.info(LOG, "Versions : [%d, %d]", localVer, remoteVer);
            return;
        }
        // bring up-to-date.
        FmtLog.info(LOG, "Patch range [%d, %d]", localVer+1, remoteVer);
        IntStream.rangeClosed(localVer+1, remoteVer).forEach((x)->{
            FmtLog.info(LOG, "Sync: patch=%d", x);
            RDFPatch patch = fetchPatch(x);
            RDFChanges c = target;
            if ( true )
                c = DeltaOps.print(c);
            patch.apply(c);
        });
        setRemoteVersionNumber(remoteVer);
        setLocalVersionNumber(remoteVer);
    }

//    public void syncAll() {
//        
//    }
    
    public String getName() {
        return label;
    }

    
    public Id getClientId() {
    return clientId;
}

    public Id getDatasourceId() {
        return datasourceId;
    }

    /** Actively get the remote version */  
    public int getRemoteVersionLatest() {
        return link.getCurrentVersion(datasourceId);
    }
    
    /** Return the version of the local data store */ 
    public int getLocalVersionNumber() {
        return localEpoch.get();
    }
    
    /** Update the version of the local data store */ 
    public void setLocalVersionNumber(int version) {
        Txn.executeWrite(localEpochPersistent, ()->{
            localEpochPersistent.set(BigInteger.valueOf(version));
        });
        localEpoch.set(version);
    }
    
    /** Return our local track of the remote version */ 
    public int getRemoteVersionNumber() {
        return remoteEpoch.get();
    }
    
    /** Update the version of the local belief of remote version */ 
    private void setRemoteVersionNumber(int version) {
        remoteEpoch.set(version);
    }

    /** The "record changes" version */  
    public DatasetGraph getDatasetGraph() {
        return managed;
    }

    /** The "without changes" storage */   
    public DatasetGraph getStorage() {
        return base;
    }

    private synchronized void sendPatch(RDFPatch patch) {
        int ver = link.sendPatch(datasourceId, patch);
        int ver0 = localEpoch.get();
        if ( ver0 > ver )
            FmtLog.warn(LOG, "Version did not advance: %d -> %d", ver0 , ver); 
        localEpoch.set(ver);
    }

    private RDFPatch fetchPatch(int id) {
        return link.fetch(datasourceId, id);
    }
    
    @Override
    public String toString() {
        return String.format("Client '%s' [local=%d, remote=%d]", getName(),
                             getLocalVersionNumber(), getRemoteVersionNumber());
    }

}
