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
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.jena.atlas.json.JSON;
import org.apache.jena.atlas.json.JsonArray;
import org.apache.jena.atlas.json.JsonObject ;
import org.apache.jena.atlas.json.JsonValue ;
import org.apache.jena.atlas.web.HttpException ;
import org.apache.jena.riot.web.HttpOp ;
import org.seaborne.delta.*;
import org.seaborne.delta.lib.JSONX;
import org.seaborne.delta.link.DeltaLink;
import org.seaborne.delta.link.DeltaNotConnectedException;
import org.seaborne.delta.link.RegToken;
import org.seaborne.patch.PatchReader ;
import org.seaborne.patch.RDFChanges ;
import org.seaborne.patch.RDFPatch ;
import org.seaborne.patch.changes.RDFChangesCollector ;
import org.seaborne.patch.changes.RDFChangesLog ;

/** Implementation of {@link DeltaLink} that encodes operations
 * onto the HTTP protocol and decode results.    
 */
public class DeltaLinkHTTP implements DeltaLink {

    private final String remoteServer;
    
    private final String remoteSend;
    private final String remoteReceive;
    private final String remoteData;
    
    private RegToken regToken = null;
    private Id clientId = null;
    private boolean linkOpen = false;
    
    private final static JsonObject emptyObject = new JsonObject();
    
    public static DeltaLink connect(String serverURL) {
        Objects.requireNonNull(serverURL, "DelatLinkHTTP: Null URL for the server");
        if ( ! serverURL.startsWith("http://") && ! serverURL.startsWith("https://") )  
            throw new IllegalArgumentException("Bad server URL: '"+serverURL+"'");
        return new DeltaLinkHTTP(serverURL);
    }
    
    private DeltaLinkHTTP(String serverURL) {
        if ( ! serverURL.endsWith("/" ))
            serverURL= serverURL+"/";

        this.remoteServer = serverURL;
        this.linkOpen = true;

        // One URL
        this.remoteSend     = serverURL+DeltaConst.EP_PatchLog;
        this.remoteReceive  = serverURL+DeltaConst.EP_PatchLog;
        this.remoteData     = serverURL+DeltaConst.EP_InitData;
//        // Separate URLs
//        this.remoteSend = serverURL+DPConst.EP_Append;
//        this.remoteReceive = serverURL+DPConst.EP_Fetch;
    }
    
    @Override
    public void close() {
        linkOpen = false;
    }
    
    @Override
    public void ping() {
        checkLink();
//        String url = remoteServer+"/ping";
//        HttpOp.execHttpHead(url);
        JsonObject obj = rpc(DeltaConst.OP_PING, emptyObject);
    }

    private void checkLink() {
        if ( ! linkOpen )
            throw new DeltaNotConnectedException("Not connected to URL = "+remoteServer);
    }

    // A quick local check only.
    private void checkRegistered() {
        if ( regToken == null ) 
            throw new DeltaNotConnectedException("Not registered");
    }

    @Override
    public int getCurrentVersion(Id dsRef) {
        checkLink();
        JsonObject arg = JSONX.buildObject((b)-> {
            b.key("datasource").value(dsRef.asPlainString());
        });
        
        JsonValue r = rpcToValue(DeltaConst.OP_VERSION, arg);
        if ( ! r.isNumber() )
            System.err.println("Not a number: "+r);
        return r.getAsNumber().value().intValue();
    }

    public RDFChangesHTTP createRDFChanges(Id dsRef) {
        Objects.requireNonNull(dsRef);
        checkLink();
        checkRegistered();
        
        String s = DeltaLib.makeURL(remoteSend, DeltaConst.paramReg, regToken.asString(), DeltaConst.paramDatasource, dsRef.asParam());
        return new RDFChangesHTTP(s);
    }

    // Non-streaming - collect patch then replay to send it.  
    @Override
    public int sendPatch(Id dsRef, RDFPatch patch) {
        checkLink();
        RDFChangesHTTP remote = createRDFChanges(dsRef);
        patch.apply(remote);
        String str = remote.getResponse();
        if ( str != null ) {
            try {
                JsonObject obj = JSON.parse(str);
                int version = obj.get(DeltaConst.F_VERSION).getAsNumber().value().intValue();
                return version;
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
        return -1;
    }

    @Override
    public RDFPatch fetch(Id dsRef, int version) {
        checkLink();
        String s = DeltaLib.makeURL(remoteReceive, DeltaConst.paramDatasource, dsRef.asParam(), DeltaConst.paramVersion, version);
        return fetchCommon(s);
    }

    @Override
    public RDFPatch fetch(Id dsRef, Id patchId) {
        checkLink();
        String s = DeltaLib.makeURL(remoteReceive, DeltaConst.paramDatasource, dsRef.asParam(), DeltaConst.paramPatch, patchId.asParam());
        return fetchCommon(s);
    }
    
    private RDFPatch fetchCommon(String s) {
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
    }

    @Override
    public String initialState(Id dsRef) {
        Delta.DELTA_HTTP_LOG.info("initialState request: "+dsRef);
        // XXX Better URI design
        return DeltaLib.makeURL(remoteData, DeltaConst.paramDatasource, dsRef.asParam());
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
        checkLink();
        JsonObject arg = JSONX.buildObject((b) -> {
            b.key(DeltaConst.F_CLIENT).value(clientId.asPlainString());
        });
        JsonObject obj = rpc(DeltaConst.OP_REGISTER, arg);
        String s = obj.get(DeltaConst.F_TOKEN).getAsString().value();
        RegToken token = new RegToken(s);
        this.clientId = clientId; 
        this.regToken = token;
        return token; 
    }

    @Override
    public void deregister() {
        checkLink();
        JsonObject obj = rpc(DeltaConst.OP_DEREGISTER, emptyObject);
        regToken = null;
    }

    @Override
    public boolean isRegistered() {
        checkLink();
        JsonObject obj = rpc(DeltaConst.OP_ISREGISTERED, emptyObject);
        return obj.get(DeltaConst.F_VALUE).getAsBoolean().value();
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
        JsonObject obj = rpc(DeltaConst.OP_LIST_DS, emptyObject);
        JsonArray array = obj.get(DeltaConst.F_ARRAY).getAsArray();
        List<Id> x = array.stream()
            .map(jv->Id.fromString(jv.getAsString().value()))
            .collect(Collectors.toList()) ;
        return x ;
    }

    @Override
    public List<DataSourceDescription> allDescriptions() {
        JsonObject obj = rpc(DeltaConst.OP_LIST_DSD, emptyObject);
        JsonArray array = obj.get(DeltaConst.F_ARRAY).getAsArray();
        List<DataSourceDescription> x = array.stream()
            .map(jv->getDataSourceDescription(jv.getAsObject()))
            .collect(Collectors.toList()) ;
        return x ;
    }
    
    @Override
    public Id newDataSource(String name, String uri) {
        Objects.requireNonNull(name);
        JsonObject arg = JSONX.buildObject((b) -> {
            b.key(DeltaConst.F_NAME).value(name);
            if ( uri != null )
                b.key(DeltaConst.F_URI).value(uri);
        });
        JsonObject obj = rpc(DeltaConst.OP_CREATE_DS, arg);

        // Exists?
        
        String idStr = obj.get(DeltaConst.F_ID).getAsString().value();
        Id dsRef = Id.fromString(idStr);
        return dsRef;
    }

    // XXX getFieldAsXXX - share with S_DRPC.
    
    @Override
    public void removeDataset(Id dsRef) {
        JsonObject arg = JSONX.buildObject((b) -> {
            b.key(DeltaConst.F_DATASOURCE).value(dsRef.asPlainString());
        });
        JsonObject obj = rpc(DeltaConst.OP_REMOVE_DS, arg);
    }

    @Override
    public DataSourceDescription getDataSourceDescription(Id dsRef) {
        JsonObject arg = JSONX.buildObject((b) -> {
            b.key(DeltaConst.F_DATASOURCE).value(dsRef.asPlainString());
        });
        return getDataSourceDescription(arg);
    }

    @Override
    public DataSourceDescription getDataSourceDescription(String uri) {
        JsonObject arg = JSONX.buildObject((b) -> {
            b.key(DeltaConst.F_URI).value(uri);
        });
        return getDataSourceDescription(arg);
    }
    
    private DataSourceDescription getDataSourceDescription(JsonObject arg) {
        JsonObject obj = rpc(DeltaConst.OP_DESCR_DS, arg);
        if ( obj.isEmpty() )
            return null;
        return DataSourceDescription.fromJson(obj);
    }

    @Override
    public PatchLogInfo getPatchLogInfo(Id dsRef) {
        JsonObject arg = JSONX.buildObject((b) -> {
            b.key(DeltaConst.F_DATASOURCE).value(dsRef.asPlainString());
        });
        return getPatchLogInfo(arg);
    }

    private PatchLogInfo getPatchLogInfo(JsonObject arg) {
        JsonObject obj = rpc(DeltaConst.OP_DESCR_LOG, arg);
        if ( obj.isEmpty() )
            return null;
        return PatchLogInfo.fromJson(obj);
    }
    
    private JsonValue rpcToValue(String opName, JsonObject arg) {
        if ( arg == null )
            arg = emptyObject;
        return DRPC.rpc(remoteServer + DeltaConst.EP_RPC, opName, regToken, arg);
    }
    
    private JsonObject rpc(String opName, JsonObject arg) {
        JsonValue r = rpcToValue(opName, arg);
        if ( ! r.isObject() )
            throw new DeltaException("Bad result to '"+opName+"': "+JSON.toStringFlat(r));
        return r.getAsObject();
    }
}
