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

import org.apache.jena.atlas.json.JSON;
import org.apache.jena.atlas.json.JsonArray;
import org.apache.jena.atlas.json.JsonObject ;
import org.apache.jena.atlas.json.JsonValue ;
import org.apache.jena.atlas.lib.NotImplemented ;
import org.apache.jena.atlas.logging.FmtLog;
import org.apache.jena.atlas.web.HttpException ;
import org.apache.jena.riot.web.HttpOp ;
import org.seaborne.delta.DPNames ;
import org.seaborne.delta.Delta ;
import org.seaborne.delta.DeltaException;
import org.seaborne.delta.lib.J ;
import org.seaborne.delta.link.DeltaLink;
import org.seaborne.delta.link.Id;
import org.seaborne.delta.link.RegToken;
import org.seaborne.patch.PatchReader ;
import org.seaborne.patch.RDFChanges ;
import org.seaborne.patch.RDFPatch ;
import org.seaborne.patch.changes.RDFChangesCollector ;

/** Implementation of {@link DeltaLink} that encodes operations
 * onto the HTTP protocol and decode results.    
 */
public class DeltaLinkHTTP implements DeltaLink {

    private final String remoteServer;
    private final String remoteSend;
    private final String remoteReceive;
    
    public DeltaLinkHTTP(String serverURL) {
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
            b.key("datasource").value(dsRef.asParam());
        });
        JsonValue r = DRPC.rpc(remoteServer+DPNames.EP_RPC, DPNames.OP_EPOCH, obj);
        if ( ! r.isNumber() )
            System.err.println("Not a number: "+r);
        return r.getAsNumber().value().intValue();
    }

    @Override
    public RDFChanges createRDFChanges(Id dsRef) {
        return new RDFChangesHTTP(remoteSend+"?dataset="+dsRef.asParam());
    }

    @Override
    public int sendPatch(Id dsRef, RDFPatch patch) {
        RDFChanges remote = createRDFChanges(dsRef);
        patch.apply(remote);
        FmtLog.warn(getClass(), "NO VERSION");
        return -1;
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

    // ----
    // /register?id=    -> JSON { token : "uuid" }
    // /register?name=
    // /register?name=
    
    
    @Override
    public RegToken register(Id clientId) {
        JsonObject obj = J.buildObject((b) -> {
            b.key(DPNames.F_CLIENT).value(clientId.asString());
        });
        JsonValue r = DRPC.rpc(remoteServer + DPNames.EP_RPC, DPNames.OP_REGISTER, obj);
        if ( ! r.isObject() )
            throw new DeltaException("Bad result to 'register': "+JSON.toStringFlat(r));
        String s = r.getAsObject().get(DPNames.F_TOKEN).getAsString().value();
        RegToken token = new RegToken(s);
        return token; 
    }

    @Override
    public RegToken register(String name) {
        throw new NotImplemented();
    }

    @Override
    public void deregister(RegToken token) {
        throw new NotImplemented();

    }

    @Override
    public void deregister(Id clientId) {
        throw new NotImplemented();
    }

    @Override
    public boolean isRegistered(Id id) {
        throw new NotImplemented();
    }

    @Override
    public boolean isRegistered(RegToken regToken) {
        throw new NotImplemented();
    }

    @Override
    public JsonArray getDatasets() {
        throw new NotImplemented();
    }
}
