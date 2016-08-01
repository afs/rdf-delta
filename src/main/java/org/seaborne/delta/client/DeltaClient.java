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

import java.util.stream.IntStream ;

import org.apache.jena.atlas.json.JsonObject ;
import org.apache.jena.atlas.json.JsonValue ;
import org.apache.jena.sparql.core.DatasetGraph ;
import org.seaborne.delta.DP ;
import org.seaborne.delta.base.DatasetGraphChanges ;
import org.seaborne.delta.base.PatchReader ;
import org.seaborne.delta.lib.L ;
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
    private int localEpoch = 0 ;
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
        IntStream.rangeClosed(1, remoteEpoch).forEach((x)->{
            LOG.info("Init: patch="+x) ;
            PatchReader pr = fetchPatch(x) ;
            pr.apply(target);
        });
    }
    
    public void sync1() {
        
    }

    public void syncAll() {
        
    }

    
    public int getCurrentUpdateId() {
        return localEpoch ;
    }

    public int getRemoteVersion() {
        return remoteEpoch ;
    }
    
    public DatasetGraph getDatasetGraph() {
        return managed ;
    }

    /** ctively get the remote version */  
    public int getRemoteVersionLatest() {
        
        JsonObject obj = L.buildObject((b)-> b.key("operation").value(DP.OP_EPOCH)) ;
        JsonValue r = DRPC.rpc(remoteServer+DP.EP_RPC, obj) ;
        if ( ! r.isNumber() )
            System.err.println("Not a number: "+r) ;
        return r.getAsNumber().value().intValue() ;
    }
    
    public PatchReader fetchPatch(int id) {
        return LibPatchFetcher.fetchByPath(remoteServer+DP.EP_Patch, id) ;
    }

}
