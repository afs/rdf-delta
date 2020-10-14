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

package org.seaborne.delta.client;

import static java.lang.String.format;
import static org.seaborne.delta.DeltaConst.F_ARRAY;
import static org.seaborne.delta.DeltaConst.F_DATASOURCE;
import static org.seaborne.delta.DeltaConst.F_LOCK_REF;
import static org.seaborne.delta.DeltaConst.F_LOCK_TICKS;

import java.io.InputStream ;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Supplier ;
import java.util.stream.Collectors;

import org.apache.jena.atlas.json.*;
import org.apache.jena.atlas.logging.FmtLog ;
import org.apache.jena.atlas.web.HttpException ;
import org.apache.jena.riot.web.HttpOp ;
import org.apache.jena.web.HttpSC ;
import org.seaborne.delta.*;
import org.seaborne.delta.lib.JSONX;
import org.seaborne.delta.link.DeltaLink;
import org.seaborne.delta.link.DeltaLinkListener;
import org.seaborne.delta.link.DeltaNotConnectedException ;
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

    private Id clientId = null;
    private boolean linkOpen = false;

    private Set<DeltaLinkListener> listeners = ConcurrentHashMap.newKeySet();

    private final static JsonObject emptyObject = new JsonObject();

    public static DeltaLink connect(String serverURL) {
        Objects.requireNonNull(serverURL, "DelatLinkHTTP: Null URL for the server");
        if ( ! serverURL.startsWith("http://") && ! serverURL.startsWith("https://") )
            throw new IllegalArgumentException("Bad server URL: '"+serverURL+"'");
        DeltaLink link = new DeltaLinkHTTP(serverURL);
        link.start();
        return link;
    }

    private DeltaLinkHTTP(String serverURL) {
        if ( ! serverURL.endsWith("/" ))
            serverURL= serverURL+"/";

        this.remoteServer = serverURL;
        // One URL
        this.remoteSend     = serverURL+"{"+DeltaConst.paramDatasource+"}";
        this.remoteReceive  = serverURL+"{"+DeltaConst.paramDatasource+"}";
        this.remoteData     = serverURL+DeltaConst.EP_InitData;
//        // Separate URLs
//        this.remoteSend = serverURL+DPConst.EP_Append;
//        this.remoteReceive = serverURL+DPConst.EP_Fetch;
    }

    @Override
    public void start() {
        linkOpen = true;
    }

    @Override
    public void close() {
        linkOpen = false;
    }

    @Override
    public JsonObject ping() {
        checkLink();
        return rpcOnce(DeltaConst.OP_PING, emptyObject);
    }

    private void checkLink() {
        if ( ! linkOpen )
            throw new DeltaNotConnectedException("Not connected to URL = "+remoteServer);
    }

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
        return new RDFChangesHTTP(dsRef.toSchemeString("ds:"), calcChangesURL(dsRef));
    }

    /** Calculate the patch log URL */
    private String calcChangesURL(Id dsRef) {
        return createURL(remoteSend, DeltaConst.paramDatasource, dsRef.asParam());
    }

    @Override
    public Version append(Id dsRef, RDFPatch patch) {
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
                Version version = Version.fromJson(obj, DeltaConst.F_VERSION);
                event(listener->listener.append(dsRef, version, patch));
                return version;
            } catch (Exception ex) {
                FmtLog.warn(this.getClass(), "[%s] Error in response body : %s", dsRef, ex.getMessage());
            }
        } else {
            FmtLog.warn(this.getClass(), "[%s] No response body", dsRef);
        }
        // No response body or syntax error.
        event(listener->listener.append(dsRef, Version.UNSET, patch));
        return Version.UNSET;
    }

    @Override
    public RDFPatch fetch(Id dsRef, Version version) {
        if ( !Version.isValid(version) )
            return null;
        RDFPatch patch = fetchCommon(dsRef, DeltaConst.paramVersion, version.asParam());
        event(listener->listener.fetchByVersion(dsRef, version, patch));
        return patch;
    }

    @Override
    public RDFPatch fetch(Id dsRef, Id patchId) {
        RDFPatch patch = fetchCommon(dsRef, DeltaConst.paramPatch, patchId.asParam());
        event(listener->listener.fetchById(dsRef, patchId, patch));
        return patch;
    }

    private RDFPatch fetchCommon(Id dsRef, String param, String paramStr) {
        checkLink();

        String url = remoteReceive;
        url = createURL(url, DeltaConst.paramDatasource, dsRef.asParam());
        url = appendURL(url, paramStr);
        final String s = url;
        try {
            RDFPatch patch =  retry(()->{
                // [NET] Network point
                InputStream in = HttpOp.execHttpGet(s) ;
                if ( in == null )
                    return null ;
                RDFPatchReaderText pr = new RDFPatchReaderText(in) ;
                RDFChangesCollector collector = new RDFChangesCollector();
                pr.apply(collector);
                return collector.getRDFPatch();
            }, ()->true, ()->"Retry fetch patch.", ()->"Failed to fetch patch.");
            return patch;
        }
        catch ( HttpException ex) {
            if ( ex.getStatusCode() == HttpSC.NOT_FOUND_404 ) {
                return null ; //throw new DeltaNotFoundException(ex.getMessage());
            }
            throw ex;
        }
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
        return String.format("%s?%s=%s", remoteData, DeltaConst.paramDatasource, dsRef.asParam());
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

        Id dsRef = idFromJson(obj);
        listeners.forEach(listener->listener.newDataSource(dsRef, name));
        return dsRef;
    }

    @Override
    public Id copyDataSource(Id dsRef, String oldName, String newName) {
        JsonObject arg = JSONX.buildObject((b) -> {
            b.key(DeltaConst.F_DATASOURCE).value(dsRef.asPlainString());
            b.key(DeltaConst.F_SRC_NAME).value(oldName);
            b.key(DeltaConst.F_DST_NAME).value(newName);
        });
        JsonObject obj = rpc(DeltaConst.OP_COPY_DS, arg);
        Id dsRef2 = idFromJson(obj);
        listeners.forEach(listener->listener.copyDataSource(dsRef, dsRef2, oldName, newName));
        return dsRef2;
    }

    @Override
    public Id renameDataSource(Id dsRef, String oldName, String newName) {
        JsonObject arg = JSONX.buildObject((b) -> {
            b.key(DeltaConst.F_DATASOURCE).value(dsRef.asPlainString());
            b.key(DeltaConst.F_SRC_NAME).value(oldName);
            b.key(DeltaConst.F_DST_NAME).value(newName);
        });
        JsonObject obj = rpc(DeltaConst.OP_RENAME_DS, arg);
        Id dsRef2 = idFromJson(obj);
        listeners.forEach(listener->listener.renameDataSource(dsRef, dsRef2, oldName, newName));
        return dsRef2;
    }

    @Override
    public void removeDataSource(Id dsRef) {
        JsonObject arg = JSONX.buildObject((b) -> {
            b.key(DeltaConst.F_DATASOURCE).value(dsRef.asPlainString());
        });
        JsonObject obj = rpc(DeltaConst.OP_REMOVE_DS, arg);
        listeners.forEach(listener->listener.removeDataSource(dsRef));
    }

    @Override
    public Id acquireLock(Id datasourceId) {
        Objects.requireNonNull(datasourceId);
        JsonObject arg = JSONX.buildObject(b->{
            b.key(DeltaConst.F_DATASOURCE).value(datasourceId.asPlainString());
            //b.key(DeltaConst.F_TIMEOUT).value(datasourceId.asPlainString());
        });

        JsonObject obj = rpcOnce(DeltaConst.OP_LOCK, arg);
        Id lockSession = idOrNullFromField(obj, DeltaConst.F_LOCK_REF);
//        if ( lockSession == null )
//            throw new DeltaException("Failed to get the patch server lock.");
        return lockSession;
    }

    @Override
    public boolean refreshLock(Id datasourceId, Id lockRef) {
        Objects.requireNonNull(datasourceId);
        Objects.requireNonNull(lockRef);
        // DRY with S_DRPC
        JsonObject x = JSONX.buildObject(b->{
            b.pair(F_DATASOURCE, datasourceId.asPlainString());
            b.pair(F_LOCK_REF, lockRef.asPlainString());
        });
        JsonArray array = new JsonArray();
        array.add(x);
        JsonObject args = JSONX.buildObject(b->b.pair(F_ARRAY, array));
        JsonObject rtn = rpcOnce(DeltaConst.OP_LOCK_REFRESH, args);
        // One  lock.
        Optional<JsonValue> foo = rtn.getArray(F_ARRAY).findFirst();
        // NB Return locks that do not exist
        if ( foo.isEmpty() )
            return true;
        JsonObject rtnLock = foo.get().getAsObject();
        rtnLock.get(F_DATASOURCE);
        rtnLock.get(F_LOCK_REF);
        return false;
    }

//    @Override
//    public Set refreshLocks(Set<Id> lockSet) {
//        JsonObject arg = buildRefreshArg(lockSet);
//        JsonObject obj = rpcOnce(DeltaConst.OP_LOCK_REFRESH, arg);
//        // ???
//    }
//
//    // batch refresh operation { array: [ { datasource: "", lock: ""} ] }
//    /*package*/ static JsonObject buildRefreshArg(Set<Id> locks) {
//        List<JsonObject> x = locks.stream().map(lockId->{
//            return JSONX.buildObject(b->{
//                b.pair(F_DATASOURCE, lockId.asPlainString());
//                b.pair(F_LOCK_REF, lockId.asPlainString());
//            });
//        }).collect(toList());
//        JsonArray array = new JsonArray();
//        array.addAll(x);
//        return JSONX.buildObject(b->b.pair(F_ARRAY, array));
//    }
//
//    /*package*/ static Set<Id> buildRefreshRtn(JsonObject obj) {
//        obj.getArray(F_ARRAY).
//
//        List<JsonObject> x = locks.stream().map(lockId->{
//            return JSONX.buildObject(b->{
//                b.pair(F_DATASOURCE, lockId.asPlainString());
//                b.pair(F_LOCK_REF, lockId.asPlainString());
//            });
//        }).collect(toList());
//        JsonArray array = new JsonArray();
//        array.addAll(x);
//        return JSONX.buildObject(b->b.pair(F_ARRAY, array));
//    }


    @Override
    public LockState readLock(Id datasourceId) {
        Objects.requireNonNull(datasourceId);
        JsonObject arg = JSONX.buildObject(b->{
            b.key(DeltaConst.F_DATASOURCE).value(datasourceId.asPlainString());
        });
        JsonObject rtn = rpcOnce(DeltaConst.OP_LOCK_READ, arg);
        if ( rtn.isEmpty() )
            return LockState.FREE;
        Id lockSession = idFromField(rtn, F_LOCK_REF);
        long ticks = rtn.get(F_LOCK_TICKS).getAsNumber().value().longValue();
        return LockState.create(lockSession, ticks);
    }

    @Override
    public Id grabLock(Id datasourceId, Id session) {
        Objects.requireNonNull(datasourceId);
        Objects.requireNonNull(session);
        JsonObject arg = JSONX.buildObject(b->{
            b.key(DeltaConst.F_DATASOURCE).value(datasourceId.asPlainString());
            b.key(DeltaConst.F_LOCK_REF).value(session.asPlainString());
        });

        JsonObject obj = rpcOnce(DeltaConst.F_LOCK_GRAB, arg);
        Id lockSession = idOrNullFromField(obj, DeltaConst.F_LOCK_REF);
//        if ( lockSession == null )
//            throw new DeltaException("Failed to get the patch server lock.");
        return lockSession;
    }

    @Override
    public void releaseLock(Id datasourceId, Id lockSession) {
        Objects.requireNonNull(datasourceId);
        Objects.requireNonNull(lockSession);
        JsonObject arg = JSONX.buildObject(b->{
            b.key(DeltaConst.F_DATASOURCE).value(datasourceId.asPlainString());
            b.key(DeltaConst.F_LOCK_REF).value(lockSession.asPlainString());
        });
        JsonObject obj = rpcOnce(DeltaConst.OP_UNLOCK, arg);
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

    private static Id idFromJson(JsonObject obj) {
        return idFromField(obj, DeltaConst.F_ID);
    }

    /** Return an Id, from a field. Assumes the field is present. */
    private static Id idFromField(JsonObject obj, String fieldName) {
        try {
            String idStr = obj.get(fieldName).getAsString().value();
            Id id = Id.fromString(idStr);
            return id;
        } catch (NullPointerException | JsonException ex) {
            throw new DeltaException("Failed to extract id from field '"+fieldName+"' in"+JSON.toStringFlat(obj));
        }
    }

    private static Id idOrNullFromField(JsonObject obj, String fieldName) {
        JsonValue jv = obj.get(fieldName);
        if ( jv == null )
            return null;
        if ( jv.isNull() )
            return null;
        if ( ! jv.isString() )
            throw new DeltaException("String expected for field '"+fieldName+"' in"+JSON.toStringFlat(obj));
        String idStr = jv.getAsString().value();
        Id dsRef = Id.fromString(idStr);
        return dsRef;
    }

    /** Normal RPC primitive */
    private JsonObject rpc(String opName, JsonObject arg) {
        JsonValue r = rpcToValue(opName, arg);
        if ( ! r.isObject() )
            throw new DeltaException("Bad result to '"+opName+"': "+JSON.toStringFlat(r));
        return r.getAsObject();
    }

    /** RPC primitive returning JsonValue */
    private JsonValue rpcToValue(String opName, JsonObject arg) {
        JsonObject argx = ( arg == null ) ? emptyObject : arg;
        // [NET] Network point
        return retry(()->DRPC.rpc(remoteServer + DeltaConst.EP_RPC, opName, argx),
                     ()->true,
                     ()->format("Retry : %s",opName),
                     ()->format("Failed : %s %s",opName,JSON.toStringFlat(argx))
                     );
    }

    /** Perform an RPC, once - no retries, no logging. */
    private JsonObject rpcOnce(String opName, JsonObject arg) {
        JsonValue r = rpcOnceToValue(opName, arg);
        if ( ! r.isObject() )
            throw new DeltaException("Bad result to '"+opName+"': "+JSON.toStringFlat(r));
        return r.getAsObject();
    }

    /** Perform an RPC, once - no retries, no logging. */
    private JsonValue rpcOnceToValue(String opName, JsonObject arg) {
        JsonObject argx = ( arg == null ) ? emptyObject : arg;
        // [NET] Network point
        return DRPC.rpc(remoteServer + DeltaConst.EP_RPC, opName, argx);
    }

    private <X> void event(Consumer<DeltaLinkListener> action) {
        listeners.forEach(action);
    }

    @Override
    public void addListener(DeltaLinkListener listener) {
        listeners.add(listener);
    }

    @Override
    public void removeListener(DeltaLinkListener listener) {
        listeners.remove(listener);
    }

    @Override
    public String toString() {
        return "link:"+remoteServer;
    }
}
