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
import java.util.List;
import java.util.stream.Collectors;

import org.apache.jena.atlas.json.JSON;
import org.apache.jena.atlas.json.JsonArray;
import org.apache.jena.atlas.json.JsonObject ;
import org.apache.jena.atlas.json.JsonValue ;
import org.apache.jena.atlas.lib.NotImplemented ;
import org.apache.jena.atlas.logging.FmtLog;
import org.apache.jena.atlas.web.HttpException ;
import org.apache.jena.riot.web.HttpOp ;
import org.seaborne.delta.*;
import org.seaborne.delta.lib.JSONX;
import org.seaborne.delta.link.DeltaLink;
import org.seaborne.delta.link.RegToken;
import org.seaborne.patch.PatchReader ;
import org.seaborne.patch.RDFChanges ;
import org.seaborne.patch.RDFPatch ;
import org.seaborne.patch.changes.RDFChangesCollector ;

/** Implementation of {@link DeltaLink} that encodes operations
 * onto the HTTP protocol and decode results.    
 */
public class DeltaLinkHTTP implements DeltaLink { // DeltaLinkBase?

    private final String remoteServer;
    private final String remoteSend;
    private final String remoteReceive;
    private RegToken regToken = null;
    private Id clientId = null;
    
    private final static JsonObject emptyObject = new JsonObject();
    
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
        JsonObject arg = JSONX.buildObject((b)-> {
            b.key("datasource").value(dsRef.asParam());
        });
        
        JsonValue r = rpcToValue(DPNames.OP_EPOCH, arg);
        if ( ! r.isNumber() )
            System.err.println("Not a number: "+r);
        return r.getAsNumber().value().intValue();
    }

    @Override
    public RDFChanges createRDFChanges(Id dsRef) {
        String s = DeltaLib.makeURL(remoteSend, DPNames.paramDatasource, dsRef.asParam());
        return new RDFChangesHTTP(s);
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
        String s = DeltaLib.makeURL(remoteReceive, DPNames.paramDatasource, dsRef.asParam(), DPNames.paramVersion, version);
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
        JsonObject arg = JSONX.buildObject((b) -> {
            b.key(DPNames.F_CLIENT).value(clientId.asJsonString());
        });
        JsonObject obj = rpc(DPNames.OP_REGISTER, arg);
        String s = obj.get(DPNames.F_TOKEN).getAsString().value();
        RegToken token = new RegToken(s);
        this.clientId = clientId; 
        this.regToken = token;
        return token; 
    }

    @Override
    public void deregister() {
        // Also in the header
        JsonObject arg = JSONX.buildObject((b) -> {
            b.key(DPNames.F_TOKEN).value(regToken.asString());
        });
        JsonObject obj = rpc(DPNames.OP_DEREGISTER, arg);
    }

    @Override
    public boolean isRegistered() {
        throw new NotImplemented();
    }

    @Override
    public RegToken getRegToken() {
        return regToken;
    }

    @Override
    public Id getClientId() {
        return clientId;
    }
    
    @Override
    public List<Id> listDatasets() {
        JsonObject obj = rpc(DPNames.OP_LIST_DS, emptyObject);
        JsonArray array = obj.get(DPNames.F_RESULT).getAsArray();
        List<Id> x = array.stream().map(jv-> 
            {return Id.fromString(jv.getAsString().value());} ).collect(Collectors.toList()) ;
        return x ;
    }

    @Override
    public Id newDataset(JsonObject description) {
        throw new NotImplemented();
    }

    @Override
    public Id removeDataset(Id dsRef) {
        throw new NotImplemented();
    }

    @Override
    public DataSourceDescription getDataSourceDescription(Id dsRef) {
        JsonObject arg = JSONX.buildObject((b) -> {
            b.key(DPNames.F_DATASOURCE).value(dsRef.asJsonString());
        });
        JsonObject obj = rpc(DPNames.OP_DESCR_DS, arg);
        return DataSourceDescription.fromJson(obj);
    }

    private JsonValue rpcToValue(String opName, JsonObject arg) {
        if ( arg == null )
            arg = emptyObject;
        return DRPC.rpc(remoteServer + DPNames.EP_RPC, opName, regToken, arg);
    }
    
    private JsonObject rpc(String opName, JsonObject arg) {
        JsonValue r = rpcToValue(opName, arg);
        if ( ! r.isObject() )
            throw new DeltaException("Bad result to '"+opName+"': "+JSON.toStringFlat(r));
        return r.getAsObject();
    }
}
