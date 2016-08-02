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

import java.util.concurrent.atomic.AtomicInteger ;
import java.util.stream.IntStream ;

import org.apache.jena.atlas.json.JsonObject ;
import org.apache.jena.atlas.json.JsonValue ;
import org.apache.jena.atlas.logging.FmtLog ;
import org.apache.jena.sparql.core.DatasetGraph ;
import org.seaborne.delta.DP ;
import org.seaborne.delta.base.DatasetGraphChanges ;
import org.seaborne.delta.base.PatchReader ;
import org.seaborne.delta.lib.J ;
import org.seaborne.patch.RDFChanges ;
import org.seaborne.patch.RDFChangesApply ;
import org.slf4j.Logger ;
import org.slf4j.LoggerFactory ;

public class DeltaClient {
    
    private static Logger LOG = LoggerFactory.getLogger("Client") ;
    
    public static DeltaClient create(String url, DatasetGraph dsg) {
        DeltaClient client = new DeltaClient(url, dsg) ;
        client.register() ;
        return client ;
    }
    
    // The version of the remote copy.
    private int remoteEpoch = 0 ;
    private AtomicInteger localEpoch = new AtomicInteger(0) ;
    private String remoteServer ;
    private DatasetGraph base ;
    private RDFChanges target ;
    private DatasetGraphChanges managed ;
    
    public DeltaClient(String controller, DatasetGraph dsg) {
        this.remoteServer = controller ; 
        this.base = dsg ;
        this.target = new RDFChangesApply(dsg) ;
        RDFChanges monitor = new RDFChangesHTTP(controller+DP.EP_Patch) ;
        this.managed = new DatasetGraphChanges(dsg, monitor) ; 
    }
    
    private void register() {
        // Their update id.
        remoteEpoch = getRemoteVersionLatest() ;
        // bring up-to-date.
        // Range????
        FmtLog.info(LOG, "Patch range [%d, %d]", 1, remoteEpoch) ;
        IntStream.rangeClosed(1, remoteEpoch).forEach((x)->{
            LOG.info("Register: patch="+x) ;
            PatchReader pr = fetchPatch(x) ;
            pr.apply(target);
        });
    }
    
    public void sync1() {
        
    }

    public void syncAll() {
        
    }
    
    /** Return the version of the local data store */ 
    public int getLocalVersionNumber() {
        return localEpoch.get();
    }
    
    /** Return the version of the local data store */ 
    public void setLocalVersionNumber(int version) {
        localEpoch.set(version); 
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
