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

import static org.seaborne.delta.DPConst.*;

import java.io.IOException ;
import java.io.InputStream ;
import java.io.OutputStream ;
import java.io.PrintStream ;
import java.util.List;

import javax.servlet.http.HttpServletRequest ;
import javax.servlet.http.HttpServletResponse ;

import org.apache.jena.atlas.io.IO;
import org.apache.jena.atlas.io.IndentedWriter;
import org.apache.jena.atlas.json.* ;
import org.apache.jena.atlas.lib.InternalErrorException;
import org.apache.jena.atlas.logging.FmtLog ;
import org.apache.jena.web.HttpSC ;
import org.seaborne.delta.Delta ;
import org.seaborne.delta.DeltaBadRequestException;
import org.seaborne.delta.Id;
import org.seaborne.delta.lib.JSONX;
import org.seaborne.delta.link.DeltaLink;
import org.seaborne.delta.link.RegToken;
import org.slf4j.Logger ;

/** Receive a JSON object, return a JSON object */ 
public class S_DRPC extends DeltaServletBase {

    private static Logger     LOG       = Delta.DELTA_RPC_LOG;
    private static JsonObject noResults = new JsonObject();
    
    public S_DRPC(DeltaLink engine) {
        super(engine) ;
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
            return DeltaAction.create(req, resp, regToken, op, arg, input);
        } catch (JsonException ex) {
            throw new DeltaBadRequestException("Bad JSON in request: "+ex.getMessage()+ " : "+JSON.toStringFlat(input));
        } 
    }

    @Override
    protected void validateAction(DeltaAction action) throws IOException {
        // Checking once basic parsing of the request has been done to produce the JsonAction 
        switch(action.opName) {
            // No registration required.
            case OP_REGISTER:
            case OP_LIST_DS:
            case OP_DESCR_DS:
                
                break;
            // Registration required.
            case OP_EPOCH:
            case OP_DEREGISTER:
//            case OP_CREATE_DS:
//            case OP_REMOVE_DS:
                checkRegistration(action);
                break;
            case OP_CREATE_DS:
            case OP_REMOVE_DS:
                LOG.warn("Not implemented: "+action.opName);
                throw new DeltaBadRequestException("Not implemented: "+action.opName);
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
            case OP_EPOCH:
                rslt = epoch(action);
                break ;
            case OP_REGISTER:
                rslt = register(action);
                break ;
            case OP_DEREGISTER:
                rslt = deregister(action);
                break ;
            case OP_LIST_DS:
                rslt = listDataSources(action);
                break ;
            case OP_DESCR_DS:
                rslt = describeDataSource(action);
                break ;
            default:
                throw new InternalErrorException("Unknown operation: "+action.opName);
        }
        
        OutputStream out = action.response.getOutputStream() ;
        if ( ! OP_EPOCH.equals(action.opName) )
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
        } catch (IOException ex) {
            IO.exception(ex);
        }
    }
        
    static class DRPPEception extends RuntimeException {
        private int code;

        public DRPPEception(int code, String message) {
            super(message);
            this.code = code ;
        }
    }
    
    //private JsonValue register(JsonObject arg) {
    
    private JsonValue register(DeltaAction action) {
        Id client = getFieldAsId(action, F_CLIENT);
        RegToken token = getLink(action).register(client);
        register(client, token);
        JsonValue jv = JSONX.buildObject((x)-> {
            x.key(F_TOKEN).value(token.getUUID().toString());
        });
        return jv;
    }

    private JsonValue deregister(DeltaAction action) {
        getLink(action).deregister();
        deregister(action.regToken);
        return noResults;
    }
    
    private JsonValue epoch(DeltaAction action) {
        Id dsRef = getFieldAsId(action, F_DATASOURCE);
        int version = getLink(action).getCurrentVersion(dsRef);
        return JsonNumber.value(version);
    }

    private JsonValue listDataSources(DeltaAction action) {
        List<Id> ids = getLink(action).listDatasets();
        return JSONX.buildObject(b->{
            b.key(F_RESULT);
            b.startArray();
            ids.forEach(id->b.value(id.asJsonString()));
            b.finishArray();
        });
    }

    private JsonValue describeDataSource(DeltaAction action) {
        String dataSourceId = getFieldAsString(action, F_DATASOURCE);
        Id dsRef = Id.fromString(dataSourceId);
        return getLink(action).getDataSourceDescription(dsRef).asJson();
    }
    
    private static String getFieldAsString(DeltaAction action, String field) {
        return getFieldAsString(action.rpcArg, field); 
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
