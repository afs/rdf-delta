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

import static java.lang.String.format;

import java.io.InputStream ;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier ;
import java.util.stream.Collectors;

import org.apache.jena.atlas.json.JSON;
import org.apache.jena.atlas.json.JsonArray;
import org.apache.jena.atlas.json.JsonObject ;
import org.apache.jena.atlas.json.JsonValue ;
import org.apache.jena.atlas.logging.FmtLog ;
import org.apache.jena.atlas.web.HttpException ;
import org.apache.jena.riot.web.HttpOp ;
import org.apache.jena.web.HttpSC ;
import org.seaborne.delta.*;
import org.seaborne.delta.lib.JSONX;
import org.seaborne.delta.link.DeltaLink;
import org.seaborne.delta.link.DeltaNotConnectedException ;
import org.seaborne.delta.link.DeltaNotRegisteredException ;
import org.seaborne.delta.link.RegToken;
import org.seaborne.patch.RDFPatch ;
import org.seaborne.patch.changes.RDFChangesCollector ;
import org.seaborne.patch.text.RDFPatchReaderText ;

/** 
 * Implementation of {@link DeltaLink} that encodes operations
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
        this.remoteSend     = serverURL+"{"+DeltaConst.paramDatasource+"}";
        this.remoteReceive  = serverURL+"{"+DeltaConst.paramDatasource+"}";
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
        JsonObject obj = rpcOnce(DeltaConst.OP_PING, emptyObject);
        // Pass/fail.  Ignore the result.
    }

    private void checkLink() {
        if ( ! linkOpen )
            throw new DeltaNotConnectedException("Not connected to URL = "+remoteServer);
    }

    // A quick local check only.
    private void checkRegistered() {
        if ( regToken == null )
            throw new DeltaNotRegisteredException("Not registered");
    }

    private static int RETRIES_REGISTRATION = 2 ;
    
    // With backoff.
    private static int RETRIES_COMMS_FAILURE = 2 ;

    private static int RETRIES_HTTP_FAILURE = 2 ;

    // Like Callable but no Exception.
    interface Action<T> { T action() ; }

    /** Perform a retryable operation. Retry only happen if HttpException happens. */
    private <T> T retry(Action<T> callable, Supplier<Boolean> retryable, Supplier<String> retryMsg, Supplier<String> failureMsg) {
        for ( int i = 1 ; ; i++ ) {
            try {
                return callable.action();
            } catch (HttpException ex) {
                if ( ex.getResponseCode() == HttpSC.UNAUTHORIZED_401 ) {
                    if ( retryable.get() && i < RETRIES_REGISTRATION ) {
                        // Reregister
                        reregister();
                        continue;
                    }
                    if ( retryMsg != null  )
                        Delta.DELTA_HTTP_LOG.warn(retryMsg.get());
                    else
                        Delta.DELTA_HTTP_LOG.warn("Failed to register");
                    // Failed to register.
                    if ( failureMsg != null )
                        throw new DeltaNotRegisteredException(failureMsg.get());
                    else
                        throw new DeltaNotRegisteredException(ex.getMessage());
                }
                // Other HTTP problems.
                if ( failureMsg != null )
                    // Other...
                    Delta.DELTA_HTTP_LOG.warn(failureMsg.get());
                throw ex;
            }
            // Any other exception - don't retry.
        }
    }
    
    private RDFChangesHTTP createRDFChanges(Id dsRef) {
        Objects.requireNonNull(dsRef);
        checkLink();
        checkRegistered();
        return new RDFChangesHTTP(dsRef.toSchemeString("ds:"),
                                  ()->calcChangesURL(dsRef),
                                  ()->reregister());
    }
    
    /** Calculate the patch log URL */ 
    private String calcChangesURL(Id dsRef) {
        String url = createURL(remoteSend, DeltaConst.paramDatasource, dsRef.asParam());
        url = addToken(url);
        return url;
    }

    @Override
    public long append(Id dsRef, RDFPatch patch) {
        checkLink();
        
        long t1 = System.currentTimeMillis();
        String str = retry(()->{
                            RDFChangesHTTP remote = createRDFChanges(dsRef);
                            // [NET] Network point
                            // If not re-applyable, we need a copy.
                            patch.apply(remote);
                            return remote.getResponse();
                        }, 
                        ()->patch.repeatable(),
                        ()->"Retry append patch.", ()->"Failed to append patch : "+dsRef);
        long t2 = System.currentTimeMillis();
        long elapsed_ms = (t2-t1);
        if ( str != null ) {
            try {
                JsonObject obj = JSON.parse(str);
                int version = obj.get(DeltaConst.F_VERSION).getAsNumber().value().intValue();
                return version;
            } catch (Exception ex) {
                FmtLog.warn(this.getClass(), "[%s] Error in response body : %s", dsRef, ex.getMessage());
            }
        } else {
            FmtLog.warn(this.getClass(), "[%s] No response body", dsRef);
        }
        // No response body or syntax error.
        return -1;
    }

    @Override
    public RDFPatch fetch(Id dsRef, long version) {
        if ( version < 0 )
            return null;
        return fetchCommon(dsRef, DeltaConst.paramVersion, version, Long.toString(version));
    }

    @Override
    public RDFPatch fetch(Id dsRef, Id patchId) {
        return fetchCommon(dsRef, DeltaConst.paramPatch, patchId.asParam(), patchId.toString());
    }

    private RDFPatch fetchCommon(Id dsRef, String param, Object value, String valueLogStr) {
        checkLink();
        
        String url = remoteReceive;
        url = createURL(url, DeltaConst.paramDatasource, dsRef.asParam());
        url = appendURL(url, value.toString());
        url = addToken(url);
        final String s = url;
        FmtLog.info(Delta.DELTA_HTTP_LOG, "Fetch request: %s %s=%s [%s]", dsRef, param, valueLogStr, url);
        try { 
            return retry(()->{
                // [NET] Network point
                InputStream in = HttpOp.execHttpGet(s) ;
                if ( in == null )
                    return null ;
                RDFPatchReaderText pr = new RDFPatchReaderText(in) ;
                RDFChangesCollector collector = new RDFChangesCollector();
                pr.apply(collector);
                return collector.getRDFPatch();
            }, ()->true, ()->"Retry fetch patch.", ()->"Failed to fetch patch.");
        }
        catch ( HttpException ex) {
            if ( ex.getResponseCode() == HttpSC.NOT_FOUND_404 )
                return null ; //throw new DeltaNotFoundException(ex.getMessage());
            throw ex;
        }
    }

    private String addToken(String url) {
        // If registered.
        if ( regToken != null )
            url = DeltaLib.makeURL(url, DeltaConst.paramReg, regToken.asString());
        return url;
    }

    private static String appendURL(String url, String string) {
        if ( url.endsWith("/") )
            return url+string;
        else
            return url+"/"+string;
    }

    private static String createURL(String url, String param, String value) {
        return url.replace("{"+param+"}", value);
    }

    @Override
    public String initialState(Id dsRef) {
        // Better URI design
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

    /**
     * If this returns we did contact the server,
     * otherwise we get an exception.
     */
    
    @Override
    public boolean isRegistered() {
        checkLink();
        JsonObject obj = rpc(DeltaConst.OP_ISREGISTERED, emptyObject);
        return obj.get(DeltaConst.F_VALUE).getAsBoolean().value();
    }

    /** Re-register with the same client id. A new {@link RegToken} is likely.  */ 
    private RegToken reregister() {
        return register(clientId);
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
    public List<DataSourceDescription> listDescriptions() {
        JsonObject obj = rpc(DeltaConst.OP_LIST_DSD, emptyObject);
        JsonArray array = obj.get(DeltaConst.F_ARRAY).getAsArray();
        List<DataSourceDescription> x = array.stream()
            .map(jv->getDataSourceDescription(jv.getAsObject()))
            .collect(Collectors.toList()) ;
        return x ;
    }
    
    @Override
    public List<PatchLogInfo> listPatchLogInfo() {
        JsonObject obj = rpc(DeltaConst.OP_LIST_LOG_INFO, emptyObject);
        JsonArray array = obj.get(DeltaConst.F_ARRAY).getAsArray();
        List<PatchLogInfo> x = array.stream()
            .map(jv->PatchLogInfo.fromJson(jv.getAsObject()))
            .collect(Collectors.toList()) ;
        return x ;
    }

    @Override
    public Id newDataSource(String name, String uri) {
        Objects.requireNonNull(name);
        
        if ( ! DeltaOps.isValidName(name) )
            throw new IllegalArgumentException("Invalid data source name: '"+name+"'"); 
        
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

    @Override
    public void removeDataSource(Id dsRef) {
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
    public DataSourceDescription getDataSourceDescriptionByName(String name) {
        JsonObject arg = JSONX.buildObject((b) -> {
            b.key(DeltaConst.F_NAME).value(name);
        });
        return getDataSourceDescription(arg);
    }
    
    @Override
    public DataSourceDescription getDataSourceDescriptionByURI(String uri) {
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
    
    private JsonObject rpc(String opName, JsonObject arg) {
        JsonValue r = rpcToValue(opName, arg);
        if ( ! r.isObject() )
            throw new DeltaException("Bad result to '"+opName+"': "+JSON.toStringFlat(r));
        return r.getAsObject();
    }

    private JsonValue rpcToValue(String opName, JsonObject arg) {
        JsonObject argx = ( arg == null ) ? emptyObject : arg;
        // [NET] Network point
        return retry(()->DRPC.rpc(remoteServer + DeltaConst.EP_RPC, opName, regToken, argx),
                     ()->true,
                     ()->format("Retry : %s",opName),
                     ()->format("Failed : %s %s",opName,JSON.toStringFlat(argx))
                     );
    }
    
    // Perform an RPC, once - no retries, no logging.
    private JsonObject rpcOnce(String opName, JsonObject arg) {
        JsonValue r = rpcOnceToValue(opName, arg);
        if ( ! r.isObject() )
            throw new DeltaException("Bad result to '"+opName+"': "+JSON.toStringFlat(r));
        return r.getAsObject();
    }

    private JsonValue rpcOnceToValue(String opName, JsonObject arg) {
        JsonObject argx = ( arg == null ) ? emptyObject : arg;
        // [NET] Network point
        return DRPC.rpc(remoteServer + DeltaConst.EP_RPC, opName, regToken, argx);
    }
    
    @Override
    public String toString() {
        return "link:"+remoteServer;
    }
}
