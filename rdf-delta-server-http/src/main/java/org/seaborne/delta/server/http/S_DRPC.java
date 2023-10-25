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

package org.seaborne.delta.server.http;

import static java.lang.String.format;
import static org.seaborne.delta.DeltaConst.* ;

import java.io.IOException ;
import java.io.InputStream ;
import java.io.OutputStream ;
import java.io.PrintStream ;
import java.util.List;
import java.util.stream.Collectors;

import jakarta.servlet.http.HttpServletRequest ;
import jakarta.servlet.http.HttpServletResponse ;

import org.apache.jena.atlas.RuntimeIOException;
import org.apache.jena.atlas.io.IndentedWriter;
import org.apache.jena.atlas.json.*;
import org.apache.jena.atlas.lib.InternalErrorException;
import org.apache.jena.atlas.logging.FmtLog ;
import org.apache.jena.web.HttpSC ;
import org.seaborne.delta.* ;
import org.apache.jena.atlas.io.IOX;
import org.seaborne.delta.lib.JSONX;
import org.seaborne.delta.lib.LibX;
import org.seaborne.delta.link.DeltaLink;
import org.slf4j.Logger ;

/** Receive a JSON object, return a JSON object */
public class S_DRPC extends DeltaServlet {

    private static Logger     LOG       = Delta.DELTA_RPC_LOG;
    private static JsonObject noResults = new JsonObject();
    private static JsonObject resultTrue = JSONX.buildObject(b -> b.key(F_VALUE).value(true));
    private static JsonObject resultFalse = JSONX.buildObject(b-> b.key(F_VALUE).value(false));

    public S_DRPC(DeltaLink engine) {
        super(engine) ;
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        doCommon(req, resp);
    }

    @Override
    protected DeltaAction parseRequest(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        JsonObject input ;
        try (InputStream in = req.getInputStream()  ) {
            input = JSON.parse(in) ;
        } catch (JsonException ex) {
            throw new DeltaBadRequestException("Bad JSON request: "+ex.getMessage()) ;
        }
        //LOG.info("RPC: "+JSON.toStringFlat(input));

        // Switch to IndentedWriter.clone(IndentedWriter.stdout);
        if ( false ) {
            IndentedWriter iWrite = IndentedWriter.stdout;
            iWrite.incIndent(4);
            try { JSON.write(iWrite, input); iWrite.ensureStartOfLine(); }
            finally { iWrite.decIndent(4); }
        }
        // Header

        try {
            // Header: "op", "client", "token"?
            String op = getFieldAsString(input, F_OP);
            String opid = getFieldAsString(input, F_OP_ID);
            JsonObject arg = getFieldAsObject(input, F_ARG);

            if ( op == null )
                resp.sendError(HttpSC.BAD_REQUEST_400, "No op name: "+JSON.toStringFlat(input));
            if ( arg == null )
                resp.sendError(HttpSC.BAD_REQUEST_400, "No argument to RPC: "+JSON.toStringFlat(input));
            String token = getFieldAsString(input,  F_TOKEN, false);
            return DeltaAction.create(req, resp, getLink(), token, op, opid, arg, input);
        } catch (JsonException ex) {
            throw new DeltaBadRequestException("Bad JSON in request: "+ex.getMessage()+ " : "+JSON.toStringFlat(input));
        }
    }

    @Override
    protected void validateAction(DeltaAction action) throws IOException {
        // XXX
        // Checking once basic parsing of the request has been done to produce the JsonAction
        switch(action.opName) {
            case OP_PING:
            case OP_LIST_DS:
            case OP_LIST_DSD:
            case OP_LIST_LOG_INFO:
            case OP_DESCR_DS:
            case OP_DESCR_LOG:
            case OP_CREATE_DS:
            case OP_COPY_DS:
            case OP_RENAME_DS:
            case OP_REMOVE_DS:
            case OP_LOCK:
            case OP_LOCK_REFRESH:
            case OP_LOCK_READ:
            case OP_LOCK_GRAB:
            case OP_UNLOCK:
                break;
            default:
                LOG.warn("Unknown operation: "+action.opName);
                throw new DeltaBadRequestException("Unknown operation: "+action.opName);
        }
    }

    private String lastOpName = null;
    private static JsonObject emptyObject = new JsonObject();
    private static JsonObject emptyObjectArray =
    JSONX.buildObject(b->{
        b.key(F_ARRAY);
        b.startArray();
        b.finishArray();
    });

    @Override
    protected void executeAction(DeltaAction action) throws IOException {
        JsonValue rslt = null ;
        JsonObject arg = action.rpcArg;
        // Some operations are logged at DEBUG because they are high-volume polling.
        // They should all be "read" operations.
        boolean infoLogThisRPC = true;
        String recordOp = null;
        try {
            switch(action.opName) {
                case OP_PING:
                    infoLogThisRPC = false;
                    rslt = ping(action);
                    break ;
                case OP_LIST_DS:
                    infoLogThisRPC = false;
                    rslt = listDataSources(action);
                    break ;
                case OP_DESCR_DS:
                    infoLogThisRPC = false;
                    rslt = describeDataSource(action);
                    break ;
                case OP_DESCR_LOG:
                    infoLogThisRPC = false;
                    rslt = describePatchLog(action);
                    break ;
                case OP_LIST_LOG_INFO:
                    // This operation is used to poll for changes so there can be a lot
                    // of such requests. Don't log.
                    // If "same as last time" infoLogThisRPC = ! OP_LIST_LOG_INFO.equals(lastOpName);
                    infoLogThisRPC = false;
                    rslt = listPatchLogInfo(action);
                    break ;
                case OP_LIST_DSD:
                    infoLogThisRPC = false;
                    rslt = listDataSourcesDescriptions(action);
                    break ;

                // Operations that change the server.
                case OP_CREATE_DS:
                    rslt = createDataSource(action);
                    break;
                case OP_COPY_DS:
                    rslt = copyDataSource(action);
                    break;
                case OP_RENAME_DS:
                    rslt = renameDataSource(action);
                    break;
                case OP_REMOVE_DS:
                    rslt = removeDataSource(action);
                    break;
                case OP_LOCK:
                    infoLogThisRPC = false;
                    rslt = acquirePatchLogLock(action);
                    break;
                case OP_LOCK_REFRESH:
                    infoLogThisRPC = false;
                    rslt = refreshPatchLogLock(action);
                    break;
                case OP_LOCK_READ:
                    infoLogThisRPC = false;
                    rslt = readPatchLogLock(action);
                    break;
                case OP_LOCK_GRAB:
                    infoLogThisRPC = true;
                    rslt = grabPatchLogLock(action);
                    break;
                case OP_UNLOCK:
                    infoLogThisRPC = false;
                    rslt = releasePatchLogLock(action);
                    break;
                default:
                    throw new InternalErrorException("Unknown operation: "+action.opName);
            }
            //
            recordOp = action.opName;
        }
        catch (Exception ex) { recordOp = null; throw ex ; }
        finally {
            if ( infoLogThisRPC )
                lastOpName = recordOp;
        }

        if ( infoLogThisRPC )
            FmtLog.info(LOG, "[%d] %s %s => %s", action.id, action.opName, JSON.toStringFlat(arg), JSON.toStringFlat(rslt)) ;
        else
            FmtLog.debug(LOG, "[%d] %s %s => %s", action.id, action.opName, JSON.toStringFlat(arg), JSON.toStringFlat(rslt)) ;
        sendJsonResponse(action.response, rslt);
    }

    static public void sendJsonResponse(HttpServletResponse resp, JsonValue rslt) {
        try {
            OutputStream out = resp.getOutputStream() ;
            try {
                if ( rslt != null ) {
                    resp.setStatus(HttpSC.OK_200);
                    JSON.write(out, rslt);
                } else {
                    resp.setStatus(HttpSC.NO_CONTENT_204);
                }
            }
            catch (Throwable ex) {
                if ( looksLikeEOF(ex) ) {
                    LOG.warn("Jetty EofException (client has left?)");
                    return;
                }
                LOG.warn("500 "+ex.getMessage(), ex) ;
                try {
                    // Try to send information.
                    resp.sendError(HttpSC.INTERNAL_SERVER_ERROR_500, "Exception: "+ex.getMessage()) ;
                    PrintStream ps = new PrintStream(out) ;
                    ex.printStackTrace(ps);
                    ps.flush();
                } catch (Throwable ex2) {}
                return ;
            }
        } catch (IOException ex) { throw IOX.exception(ex); }
    }

    // Is the theowable
    private static boolean looksLikeEOF(Throwable ex) {
        if ( ex instanceof org.eclipse.jetty.io.EofException )
            return true;
        if ( ex.getCause() == null )
            return false;
        // Wrapped?
        if ( ! ( ex instanceof RuntimeIOException ) && ! ( ex instanceof org.eclipse.jetty.io.RuntimeIOException) )
            return false;
        Throwable ex2 = ex.getCause();
        if ( ex2 instanceof org.eclipse.jetty.io.EofException )
            return true;
        return false;
    }

    // {} -> { "value" ; "...now..."}
    private JsonValue ping(DeltaAction action) {
        return DeltaLib.ping();
    }

    private JsonValue listDataSources(DeltaAction action) {
        List<Id> ids = action.dLink.listDatasets();
        return JSONX.buildObject(b->{
            b.key(F_ARRAY);
            b.startArray();
            ids.forEach(id->b.value(id.asPlainString()));
            b.finishArray();
        });
    }

    private JsonValue describeDataSource(DeltaAction action) {
        DataSourceDescription dsd = getDataSourceDescription(action);
        if ( dsd == null )
            return noResults;
        return dsd.asJson();
    }

    // Decide the DataSourceDescription (which may be null).
    private DataSourceDescription getDataSourceDescription(DeltaAction action) {
        String dataSourceId = getFieldAsString(action, F_DATASOURCE, false);
        String name = getFieldAsString(action, F_NAME, false);
        String uri = getFieldAsString(action, F_URI, false);

        int x = LibX.countNonNulls(dataSourceId, uri, name);
        if ( x == 0 )
            throw new DeltaBadRequestException(format("No field: '%s' nor '%s' nor '%s'", F_DATASOURCE, F_NAME, F_URI));
        // Use the first defined.
        if ( dataSourceId != null ) {
            Id dsRef = Id.fromString(dataSourceId);
            return action.dLink.getDataSourceDescription(dsRef);
        }
        if ( name != null )
            return action.dLink.getDataSourceDescriptionByName(name);
        if ( uri != null )
            return action.dLink.getDataSourceDescriptionByURI(uri);
        throw new InternalErrorException();
    }

    private JsonValue describePatchLog(DeltaAction action) {
        // If Id or URI.
//        String uri = getFieldAsString(action, F_URI, false);
//        String dataSourceId = getFieldAsString(action, F_DATASOURCE, false);
//        if ( uri == null && dataSourceId == null )
//            throw new DeltaBadRequestException(format("No field: '%s' nor '%s'", F_DATASOURCE, F_URI));
//        if ( uri != null && dataSourceId != null )
//            throw new DeltaBadRequestException(format("Only one of fields '%s' nor '%s' allowed", F_DATASOURCE, F_URI));
        // Must be by Id.
        String dataSourceId = getFieldAsString(action, F_DATASOURCE, false);
        if ( dataSourceId == null )
            throw new DeltaBadRequestException(format("No field: '%s' ", F_DATASOURCE));
        Id dsRef = Id.fromString(dataSourceId);
        PatchLogInfo logInfo = action.dLink.getPatchLogInfo(dsRef);
        if ( logInfo == null )
            return noResults;
        return logInfo.asJson();
    }

    private JsonValue listPatchLogInfo(DeltaAction action) {
        List<PatchLogInfo> info = action.dLink.listPatchLogInfo();
        return JSONX.buildObject(b->{
            b.key(F_ARRAY);
            b.startArray();
            info.forEach(x->x.addJsonObject(b));
            b.finishArray();
        });
    }

    private JsonValue listDataSourcesDescriptions(DeltaAction action) {
        List<DataSourceDescription> x = action.dLink.listDescriptions();
        return JSONX.buildObject(b->{
            b.key(F_ARRAY);
            b.startArray();
            x.forEach(dsd->dsd.addJsonObject(b));
            b.finishArray();
        });
    }

    private JsonValue createDataSource(DeltaAction action) {
        String name = getFieldAsString(action, F_NAME);
        String uri = getFieldAsString(action, F_URI, false);
        Id dsRef = action.dLink.newDataSource(name, uri);
        return JSONX.buildObject(b->b.key(F_ID).value(dsRef.asPlainString()));
    }

    private JsonValue copyDataSource(DeltaAction action) {
        String dataSourceId = getFieldAsString(action, F_DATASOURCE);
        String oldName = getFieldAsString(action, F_SRC_NAME);
        String newName = getFieldAsString(action, F_DST_NAME);
        Id dsRef = Id.fromString(dataSourceId);
        Id dsRef2 = action.dLink.copyDataSource(dsRef, oldName, newName);
        return JSONX.buildObject(b->b.key(F_ID).value(dsRef2.asPlainString()));
    }

    private JsonValue renameDataSource(DeltaAction action) {
        String dataSourceId = getFieldAsString(action, F_DATASOURCE);
        String oldName = getFieldAsString(action, F_SRC_NAME);
        String newName = getFieldAsString(action, F_DST_NAME);
        Id dsRef = Id.fromString(dataSourceId);
        Id dsRef2 = action.dLink.renameDataSource(dsRef, oldName, newName);
        return JSONX.buildObject(b->b.key(F_ID).value(dsRef2.asPlainString()));
    }

    private JsonValue removeDataSource(DeltaAction action) {
        String dataSourceId = getFieldAsString(action, F_DATASOURCE);
        Id dsRef = Id.fromString(dataSourceId);
        action.dLink.removeDataSource(dsRef);
        return noResults;
    }

    private JsonValue acquirePatchLogLock(DeltaAction action) {
        Id dsRef = getFieldAsId(action, F_DATASOURCE);
        Id session = action.dLink.acquireLock(dsRef);
        if ( session == null )
            return JSONX.buildObject(b->b.key(F_LOCK_REF).value(JsonNull.instance));
        return JSONX.buildObject(b->b.key(F_LOCK_REF).value(session.asPlainString()));
    }

    private JsonValue readPatchLogLock(DeltaAction action) {
        Id dsRef = getFieldAsId(action, F_DATASOURCE);
        LockState x = action.dLink.readLock(dsRef);
        if ( LockState.isFree(x) )
            return emptyObject;
        return JSONX.buildObject(b-> {
            b.key(F_LOCK_REF).value(x.session.asPlainString());
            b.key(F_LOCK_TICKS).value(x.ticks);
        });
    }

    private JsonValue grabPatchLogLock(DeltaAction action) {
        Id dsRef = getFieldAsId(action, F_DATASOURCE);
        Id session1 = getFieldAsId(action, F_LOCK_REF);

        Id session2 = action.dLink.grabLock(dsRef, session1);
        if ( session2 == null )
            return JSONX.buildObject(b->b.key(F_LOCK_REF).value(JsonNull.instance));
        return JSONX.buildObject(b->b.key(F_LOCK_REF).value(session2.asPlainString()));
    }

    private JsonValue refreshPatchLogLock(DeltaAction action) {
        JsonArray array = getFieldAsArray(action, DeltaConst.F_ARRAY);
        if ( array.isEmpty() ) {
            LOG.warn("Empty array in lock refresh "+ JSON.toStringFlat(action.rpcArg)) ;
            return emptyObjectArray;
            //throw new DeltaBadRequestException("Empty array in lock refresh "+ JSON.toStringFlat(action.rpcArg)) ;
        }
        List<JsonObject> rslt =
                array.stream().map(JsonValue::getAsObject).map(arg->{
                    Id dsRef = getFieldAsId(arg, F_DATASOURCE);
                    Id session = getFieldAsId(arg, F_LOCK_REF);
                    boolean bRslt = action.dLink.refreshLock(dsRef, session);
                    // If it did not refresh, pass arg back.
                    return bRslt ? null : arg ;
                })
                .filter(x -> x != null )
                .collect(Collectors.toList());
        return JSONX.buildObject(b->{
            b.key(F_ARRAY);
            b.startArray();
            rslt.forEach(obj->b.value(obj));
            b.finishArray();
        });
    }

    private JsonValue releasePatchLogLock(DeltaAction action) {
        Id dsRef = getFieldAsId(action, F_DATASOURCE);
        Id session = getFieldAsId(action, F_LOCK_REF);
        action.dLink.releaseLock(dsRef, session);
        return noResults;
    }

    private static String getFieldAsString(DeltaAction action, String field) {
        return getFieldAsString(action.rpcArg, field);
    }

    private static String getFieldAsString(DeltaAction action, String field, boolean required) {
        return getFieldAsString(action.rpcArg, field, required);
    }

    private static JsonObject getFieldAsObject(DeltaAction action, String field) {
        return getFieldAsObject(action.rpcArg, field);
    }

    private static JsonArray getFieldAsArray(DeltaAction action, String field) {
        return getFieldAsArray(action.rpcArg, field);
    }

    private static Id getFieldAsId(DeltaAction action, String field) {
        return getFieldAsId(action.rpcArg, field);
    }

    private static String getFieldAsString(JsonObject arg, String field) {
        return getFieldAsString(arg, field, true);
    }

    private static String getFieldAsString(JsonObject arg, String field, boolean required) {
        try {
            if ( ! arg.hasKey(field) ) {
                if ( required ) {
                    LOG.warn("Bad request: Missing Field: "+field+" Arg: "+JSON.toStringFlat(arg)) ;
                    throw new DeltaBadRequestException("Missing field: "+field) ;
                }
                return null;
            }
            return arg.get(field).getAsString().value() ;
        } catch (JsonException ex) {
            LOG.warn("Bad request: Field not a string: "+field+" Arg: "+JSON.toStringFlat(arg)) ;
            throw new DeltaBadRequestException("Bad field '"+field+"' : "+arg.get(field)) ;
        }
    }

    private static JsonObject getFieldAsObject(JsonObject arg, String field) {
        try {
            if ( ! arg.hasKey(field) ) {
                LOG.warn("Bad request: Missing Field: "+field+" Arg: "+JSON.toStringFlat(arg)) ;
                throw new DeltaBadRequestException("Missing field: "+field) ;
            }
            JsonValue jv = arg.get(field) ;
            if ( ! jv.isObject() ) {
                LOG.warn("Bad request: Bad Field: "+field+" Arg: "+JSON.toStringFlat(arg)) ;
                throw new DeltaBadRequestException("Field: "+field+" is not a JSON object");
            }
            return jv.getAsObject();
        } catch (JsonException ex) {
            LOG.warn("Bad request: Field: "+field+" Arg: "+JSON.toStringFlat(arg)) ;
            throw new DeltaBadRequestException("Bad field '"+field+"' : "+arg.get(field)) ;
        }
    }

    private static JsonArray getFieldAsArray(JsonObject arg, String field) {
        try {
            if ( ! arg.hasKey(field) ) {
                LOG.warn("Bad request: Missing Field: "+field+" Arg: "+JSON.toStringFlat(arg)) ;
                throw new DeltaBadRequestException("Missing field: "+field) ;
            }
            JsonValue jv = arg.get(field) ;
            if ( ! jv.isArray() ) {
                LOG.warn("Bad request: Bad Field: "+field+" Arg: "+JSON.toStringFlat(arg)) ;
                throw new DeltaBadRequestException("Field: "+field+" is not a JSON array");
            }
            return jv.getAsArray();
        } catch (JsonException ex) {
            LOG.warn("Bad request: Field: "+field+" Arg: "+JSON.toStringFlat(arg)) ;
            throw new DeltaBadRequestException("Bad field '"+field+"' : "+arg.get(field)) ;
        }
    }

    private static Id getFieldAsId(JsonObject arg, String field) {
        return Id.fromString(getFieldAsString(arg, field));
    }
}
