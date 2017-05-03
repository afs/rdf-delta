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

package org.seaborne.delta.server.http;

import static java.lang.String.format;
import static org.seaborne.delta.DeltaConst.F_ARG ;
import static org.seaborne.delta.DeltaConst.F_ARRAY ;
import static org.seaborne.delta.DeltaConst.F_CLIENT ;
import static org.seaborne.delta.DeltaConst.F_DATASOURCE ;
import static org.seaborne.delta.DeltaConst.F_ID ;
import static org.seaborne.delta.DeltaConst.F_NAME ;
import static org.seaborne.delta.DeltaConst.F_OP ;
import static org.seaborne.delta.DeltaConst.F_TOKEN ;
import static org.seaborne.delta.DeltaConst.F_URI ;
import static org.seaborne.delta.DeltaConst.F_VALUE ;
import static org.seaborne.delta.DeltaConst.OP_CREATE_DS ;
import static org.seaborne.delta.DeltaConst.OP_DEREGISTER ;
import static org.seaborne.delta.DeltaConst.OP_DESCR_DS ;
import static org.seaborne.delta.DeltaConst.OP_DESCR_LOG ;
import static org.seaborne.delta.DeltaConst.OP_ISREGISTERED ;
import static org.seaborne.delta.DeltaConst.OP_LIST_DS ;
import static org.seaborne.delta.DeltaConst.OP_LIST_DSD ;
import static org.seaborne.delta.DeltaConst.OP_PING ;
import static org.seaborne.delta.DeltaConst.OP_REGISTER ;
import static org.seaborne.delta.DeltaConst.OP_REMOVE_DS ;
import static org.seaborne.delta.DeltaConst.OP_VERSION ;

import java.io.IOException ;
import java.io.InputStream ;
import java.io.OutputStream ;
import java.io.PrintStream ;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import javax.servlet.http.HttpServletRequest ;
import javax.servlet.http.HttpServletResponse ;

import org.apache.jena.atlas.io.IndentedWriter;
import org.apache.jena.atlas.json.* ;
import org.apache.jena.atlas.lib.InternalErrorException;
import org.apache.jena.atlas.logging.FmtLog ;
import org.apache.jena.web.HttpSC ;
import org.seaborne.delta.* ;
import org.seaborne.delta.lib.IOX;
import org.seaborne.delta.lib.JSONX;
import org.seaborne.delta.link.DeltaLink;
import org.seaborne.delta.link.RegToken;
import org.slf4j.Logger ;

/** Receive a JSON object, return a JSON object */ 
public class S_DRPC extends DeltaServletBase {

    private static Logger     LOG       = Delta.DELTA_RPC_LOG;
    private static JsonObject noResults = new JsonObject();
    private static JsonObject resultTrue = JSONX.buildObject(b -> b.key(F_VALUE).value(true));  
    private static JsonObject resultFalse = JSONX.buildObject(b-> b.key(F_VALUE).value(false));  
    
    public S_DRPC(AtomicReference<DeltaLink> engine) {
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
            JsonObject arg = getFieldAsObject(input, F_ARG);
    
            if ( op == null )
                resp.sendError(HttpSC.BAD_REQUEST_400, "No op name: "+JSON.toStringFlat(input));
            if ( arg == null )
                resp.sendError(HttpSC.BAD_REQUEST_400, "No argument to RPC: "+JSON.toStringFlat(input));
            RegToken regToken = null;
            if ( input.hasKey(F_TOKEN) )
                regToken = new RegToken(getFieldAsString(input,  F_TOKEN));
            return DeltaAction.create(req, resp, getLink(), regToken, op, arg, input);
        } catch (JsonException ex) {
            throw new DeltaBadRequestException("Bad JSON in request: "+ex.getMessage()+ " : "+JSON.toStringFlat(input));
        } 
    }

    @Override
    protected void validateAction(DeltaAction action) throws IOException {
        // Checking once basic parsing of the request has been done to produce the JsonAction 
        switch(action.opName) {
            case OP_PING:
                return;
                // Does own check.
            case OP_REGISTER:
                // No registration required.
            case OP_ISREGISTERED:
            case OP_LIST_DS:
            case OP_LIST_DSD:
            case OP_DESCR_DS:
            case OP_DESCR_LOG:
                break;
            // Registration required.
            case OP_VERSION:
            case OP_DEREGISTER:
            case OP_CREATE_DS:
            case OP_REMOVE_DS:
                checkRegistration(action);
                break;
            default:
                LOG.warn("Unknown operation: "+action.opName);
                throw new DeltaBadRequestException("Unknown operation: "+action.opName);
        }
    }

    protected void checkRegistration(DeltaAction action) {
        if ( action.regToken == null )
            throw new DeltaBadRequestException("No registration token") ;
        if ( !isRegistered(action.regToken) )
            throw new DeltaBadRequestException("Not registered") ;
    }

    @Override
    protected void executeAction(DeltaAction action) throws IOException {
        JsonValue rslt = null ;
        JsonObject arg = action.rpcArg;
        switch(action.opName) {
            case OP_PING:
                rslt = ping(action);
                break ;
            case OP_VERSION:
                rslt = version(action);
                break ;
            case OP_REGISTER:
                rslt = register(action);
                break ;
            case OP_DEREGISTER:
                rslt = deregister(action);
                break ;
            case OP_ISREGISTERED:
                rslt = isRegistered(action);
                break ;
            case OP_LIST_DS:
                rslt = listDataSources(action);
                break ;
            case OP_DESCR_DS:
                rslt = describeDataSource(action);
                break ;
            case OP_DESCR_LOG:
                rslt = describePatchLog(action);
                break ;
            case OP_LIST_DSD:
                rslt = describeAllDataSources(action);
                break ;
            case OP_CREATE_DS:
                rslt = createDataSource(action);
                break;
            case OP_REMOVE_DS:
                rslt = removeDataSource(action);
                break;
            default:
                throw new InternalErrorException("Unknown operation: "+action.opName);
        }
        
        OutputStream out = action.response.getOutputStream() ;
        if ( ! OP_VERSION.equals(action.opName) )
            FmtLog.info(LOG, "%s %s => %s", action.opName, JSON.toStringFlat(arg), JSON.toStringFlat(rslt)) ;
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
                LOG.warn("500 "+ex.getMessage(), ex) ;
                resp.sendError(HttpSC.INTERNAL_SERVER_ERROR_500, "Exception: "+ex.getMessage()) ;
                PrintStream ps = new PrintStream(out) ; 
                ex.printStackTrace(ps);
                ps.flush();
                return ;
            }
        } catch (IOException ex) { throw IOX.exception(ex); }
    }
        
    static class DRPPEception extends RuntimeException {
        private int code;

        public DRPPEception(int code, String message) {
            super(message);
            this.code = code ;
        }
    }

    // {} -> {}
    private JsonValue ping(DeltaAction action) {
        return new JsonObject();
    }

    // Header: { client: } -> { token: }   
    private JsonValue register(DeltaAction action) {
        Id client = getFieldAsId(action, F_CLIENT);
        // XXX Proper Registration - hook to policy here.
        RegToken token = new RegToken();
        register(client, token);
        JsonValue jv = JSONX.buildObject((x)-> {
            x.key(F_TOKEN).value(token.getUUID().toString());
        });
        return jv;
    }

    // Header: { token: } -> { boolean: }   
    private JsonValue isRegistered(DeltaAction action) {
        // Registation not checked (it would be a "bad request" - this is done in validateAction) 
       if ( action.regToken == null )
           return resultFalse;
       if ( !isRegistered(action.regToken) )
           return resultFalse;
        return resultTrue;
    }

    private JsonValue deregister(DeltaAction action) {
        deregister(action.regToken);
        return noResults;
    }
    
    private JsonValue version(DeltaAction action) {
        Id dsRef = getFieldAsId(action, F_DATASOURCE);
        int version = action.dLink.getCurrentVersion(dsRef);
        return JsonNumber.value(version);
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
        String uri = getFieldAsString(action, F_URI, false);
        String dataSourceId = getFieldAsString(action, F_DATASOURCE, false);
        if ( uri == null && dataSourceId == null )
            throw new DeltaBadRequestException(format("No field: '%s' nor '%s'", F_DATASOURCE, F_URI));
        if ( uri != null && dataSourceId != null )
            throw new DeltaBadRequestException(format("Only one of fields '%s' nor '%s' allowed", F_DATASOURCE, F_URI));
        
        DataSourceDescription dsd;
        if ( dataSourceId != null ) {
            Id dsRef = Id.fromString(dataSourceId);
            dsd = action.dLink.getDataSourceDescription(dsRef);
        } else {
            dsd = action.dLink.getDataSourceDescription(uri);
        }
        if ( dsd == null )
            return noResults;
        return dsd.asJson();
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
    
    private JsonValue describeAllDataSources(DeltaAction action) {
        List<DataSourceDescription> x = action.dLink.allDescriptions();
        return JSONX.buildObject(b->{
            b.key(F_ARRAY);
            b.startArray();
            x.forEach(dsd->b.value(dsd.asJson()));
            b.finishArray();
        });
    }
    
    private JsonValue createDataSource(DeltaAction action) {
        String name = getFieldAsString(action, F_NAME);
        String uri = getFieldAsString(action, F_URI, false);
        Id dsRef = action.dLink.newDataSource(name, uri);
        return JSONX.buildObject(b->b.key(F_ID).value(dsRef.asPlainString()));
    }
    
    private JsonValue removeDataSource(DeltaAction action) {
        String dataSourceId = getFieldAsString(action, F_DATASOURCE);
        Id dsRef = Id.fromString(dataSourceId);
        action.dLink.removeDataset(dsRef);
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
                
            }
            return jv.getAsObject();
        } catch (JsonException ex) {
            LOG.warn("Bad request: Field: "+field+" Arg: "+JSON.toStringFlat(arg)) ;
            throw new DeltaBadRequestException("Bad field '"+field+"' : "+arg.get(field)) ;
        }
    }

    private static Id getFieldAsId(JsonObject arg, String field) {
        return Id.fromString(getFieldAsString(arg, field));
    }
}
