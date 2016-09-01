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

import java.io.OutputStream ;
import java.math.BigInteger ;
import java.util.stream.IntStream ;

import org.apache.jena.atlas.io.IO ;
import org.apache.jena.atlas.json.JsonObject ;
import org.apache.jena.atlas.json.JsonValue ;
import org.apache.jena.atlas.lib.NotImplemented ;
import org.apache.jena.atlas.logging.FmtLog ;
import org.apache.jena.atlas.logging.Log ;
import org.apache.jena.sparql.core.DatasetGraph ;
import org.apache.jena.system.Txn ;
import org.seaborne.delta.DP ;
import org.seaborne.delta.Delta ;
import org.seaborne.delta.DeltaOps ;
import org.seaborne.delta.base.DatasetGraphChanges ;
import org.seaborne.delta.base.PatchReader ;
import org.seaborne.delta.lib.J ;
import org.seaborne.patch.RDFChanges ;
import org.seaborne.patch.RDFChangesApply ;
import org.seaborne.patch.RDFChangesWriter ;
import org.slf4j.Logger ;
import txnx.TransPInteger ;

public class DeltaClient {
    
    private static Logger LOG = Delta.DELTA_LOG ;
    
    public static RDFChanges connect(String dest) {
        if ( dest.startsWith("file:") ) {
            OutputStream out = IO.openOutputFile(dest) ;
            RDFChanges sc = new RDFChangesWriter(out) ;
            return sc ;
        }
        
        if ( dest.startsWith("delta:") ) { // TCP connection delta:HOST:PORT
            throw new NotImplemented(dest) ; 
        }
        
        if ( dest.startsWith("http:") ) {
            // triggered on each transaction.
            return new RDFChangesHTTP(dest) ;
        }
        throw new IllegalArgumentException("Not understood: "+dest) ;
    }

    public static DeltaClient create(String localName, String url, DatasetGraph dsg) {
        DeltaClient client = new DeltaClient(localName, url, dsg) ;
        client.register() ;
        FmtLog.info(LOG, "%s", client) ;
        return client ;
    }
    
    @Override
    public String toString() {
        return String.format("Client '%s' [local=%d, remote=%d]", getName(),
                             getLocalVersionNumber(), getRemoteVersionNumber()) ;
    }
    
    // The version of the remote copy.
    private int remoteEpoch = 0 ;
    private int localEpoch = 0 ;
    private final TransPInteger localEpochPersistent ;

    private final String remoteServer ;
    
    private final DatasetGraph base ;
    private final DatasetGraphChanges managed ;
    
    private final RDFChanges target ;
    private final String label ;
    
    public DeltaClient(String localName, String controller, DatasetGraph dsg) {
        // [Delta]
        localEpochPersistent = new TransPInteger(localName) ;
        
        localEpoch = 0 ;
        
        if ( dsg instanceof DatasetGraphChanges )
            Log.warn(this.getClass(), "DatasetGraphChanges passed into DeltaClient") ;
        
        this.label = localName ;
        this.remoteServer = controller ; 
        this.base = dsg ;
        this.target = new RDFChangesApply(dsg) ;
        RDFChanges monitor = new RDFChangesHTTP(controller+DP.EP_Patch) ;
        this.managed = new DatasetGraphChanges(dsg, monitor) ; 
    }
    
    private void register() {
        sync() ;
    }
    
    synchronized
    public void sync() {
        // [Delta] replace with a one-shot "get all missing patches" operation.
 
        // Their update id.
        remoteEpoch = getRemoteVersionLatest() ;
        if ( localEpoch > remoteEpoch ) 
            FmtLog.info(LOG, "Local version ahead fo remote : [%d, %d]", localEpoch, remoteEpoch) ;
        if ( localEpoch >= remoteEpoch )
            return ;
        // bring up-to-date.
        FmtLog.info(LOG, "Patch range [%d, %d]", localEpoch+1, remoteEpoch) ;
        IntStream.rangeClosed(localEpoch+1, remoteEpoch).forEach((x)->{
            FmtLog.info(LOG, "Sync: patch=%d", x) ;
            PatchReader pr = fetchPatch(x) ;
            RDFChanges c = target ;
            if ( true )
                c = DeltaOps.print(c) ;
            pr.apply(c);
        });
        //localEpoch = remoteEpoch ;
        setLocalVersionNumber(remoteEpoch) ;
    }

//    public void syncAll() {
//        
//    }
    
    public String getName() {
        return label ;
    }
    /** Return the version of the local data store */ 
    public int getLocalVersionNumber() {
        return localEpoch ;
    }
    
    /** Update the version of the local data store */ 
    public void setLocalVersionNumber(int version) {
        Txn.execWrite(localEpochPersistent, ()->{
            localEpochPersistent.set(BigInteger.valueOf(version));
        });
        localEpoch = version ;
    }

    /** Return our local track of the remote version */ 
    public int getRemoteVersionNumber() {
        return remoteEpoch ;
    }
    
    /** The "record changes" version */  
    public DatasetGraph getDatasetGraph() {
        return managed ;
    }

    /** The "without changes" storage */   
    public DatasetGraph getStorage() {
        return base ;
    }

    /** actively get the remote version */  
    public int getRemoteVersionLatest() {
        
        JsonObject obj = J.buildObject((b)-> b.key("operation").value(DP.OP_EPOCH)) ;
        JsonValue r = DRPC.rpc(remoteServer+DP.EP_RPC, obj) ;
        if ( ! r.isNumber() )
            System.err.println("Not a number: "+r) ;
        return r.getAsNumber().value().intValue() ;
    }
    
    public PatchReader fetchPatch(int id) {
        return LibPatchFetcher.fetchByPath(remoteServer+DP.EP_Patch, id) ;
    }

}
