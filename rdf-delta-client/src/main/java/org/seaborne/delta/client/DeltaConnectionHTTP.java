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

import java.io.InputStream ;

import org.apache.jena.atlas.json.JsonObject ;
import org.apache.jena.atlas.json.JsonValue ;
import org.apache.jena.atlas.lib.NotImplemented ;
import org.apache.jena.atlas.web.HttpException ;
import org.apache.jena.riot.web.HttpOp ;
import org.seaborne.delta.DP ;
import org.seaborne.delta.Delta ;
import org.seaborne.delta.conn.DeltaConnection ;
import org.seaborne.delta.conn.Id ;
import org.seaborne.delta.lib.J ;
import org.seaborne.patch.PatchReader ;
import org.seaborne.patch.RDFChanges ;
import org.seaborne.patch.RDFPatch ;
import org.seaborne.patch.changes.RDFChangesCollector ;

/** Implementation of {@link DeltaConnection} that encodes operations
 * onto the HTTP protocol and decode results.    
 */
public class DeltaConnectionHTTP implements DeltaConnection {

    private final String remoteServer;
    private final String remoteSend;
    private final String remoteReceive;

    
    public DeltaConnectionHTTP(String serverURL) {
        if ( ! serverURL.endsWith("/" ))
            serverURL= serverURL+"/";
        
        this.remoteServer = serverURL;
        
        // XXX Constants!
        this.remoteSend = serverURL+"patch";
        this.remoteReceive = serverURL+"fetch";
    }
    
    @Override
    public int getCurrentVersion(Id dsRef) {
        JsonObject obj = J.buildObject((b)-> { 
            b.key("operation").value(DP.OP_EPOCH);
            b.key("datasource").value(dsRef.asParam());
        });
        JsonValue r = DRPC.rpc(remoteServer+DP.EP_RPC, obj);
        if ( ! r.isNumber() )
            System.err.println("Not a number: "+r);
        return r.getAsNumber().value().intValue();

    }

    @Override
    public void sendPatch(Id dsRef, RDFPatch patch) {
        RDFChanges remote = new RDFChangesHTTP(remoteSend+"?dataset="+dsRef.asParam());
        patch.apply(remote);
    }

    @Override
    public RDFPatch fetch(Id dsRef, int version) {
        String s = remoteReceive+"?version="+version+"&dataset="+dsRef.asParam();
        Delta.DELTA_HTTP_LOG.info("Fetch request: "+s);
        try {
            InputStream in = HttpOp.execHttpGet(s) ;
            if ( in == null )
                return null ;
            PatchReader pr = new PatchReader(in) ;
            RDFChangesCollector collector = new RDFChangesCollector();
            pr.apply(collector);
            return collector.getRDFPatch();
        } catch (HttpException ex) {
            System.err.println("HTTP Exception: "+ex.getMessage()) ;
            return null ;
        }
        
        //return LibPatchFetcher.fetch_byID(remoteServer+DP.EP_Fetch, dsRef.asParam(), version);
    }

    @Override
    public RDFPatch fetch(Id dsRef, Id patchId) {
        throw new NotImplemented();
    }

    public String getServerURL() {
        return remoteServer ;
    }

    public String getServerSendURL() {
        return remoteSend ;
    }

    public String getServerReceiveURL() {
        return remoteReceive ;
    }


}
