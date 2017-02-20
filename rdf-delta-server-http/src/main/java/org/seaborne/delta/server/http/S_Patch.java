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

import java.io.IOException ;
import java.io.InputStream ;
import java.io.OutputStream;
import java.util.concurrent.atomic.AtomicReference;

import javax.servlet.http.HttpServletRequest ;
import javax.servlet.http.HttpServletResponse ;

import org.apache.jena.atlas.json.JSON;
import org.apache.jena.atlas.json.JsonBuilder;
import org.apache.jena.atlas.json.JsonNumber;
import org.apache.jena.atlas.json.JsonValue;
import org.apache.jena.riot.WebContent;
import org.apache.jena.web.HttpSC ;
import org.seaborne.delta.DPConst;
import org.seaborne.delta.Delta ;
import org.seaborne.delta.DeltaBadRequestException;
import org.seaborne.delta.Id;
import org.seaborne.delta.link.DeltaLink;
import org.seaborne.patch.RDFPatch ;
import org.seaborne.patch.RDFPatchOps ;
import org.slf4j.Logger ;

/** Receive an incoming patch. */
public class S_Patch extends HttpOperationBase {
    static private Logger LOG = Delta.getDeltaLogger("Patch") ;
    
    public S_Patch(AtomicReference<DeltaLink> engine) {
        super(engine);
    }
    
    @Override
    protected void doPatch(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        doPost(req, resp);
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        doCommon(req, resp);
    }
    
    @Override
    protected void checkRegistration(DeltaAction action) {
        if ( action.regToken == null )
            throw new DeltaBadRequestException("No registration token") ;
        if ( !isRegistered(action.regToken) )
            throw new DeltaBadRequestException("Not registered") ;
    }

    @Override
    protected void validateAction(Args httpArgs) {
        if ( httpArgs.dataset == null )
            throw new DeltaBadRequestException("No data source id");
    }
    
    @Override
    protected String getOpName() {
        return "patch";
    }
    
    @Override
    protected void executeAction(DeltaAction action) throws IOException {
        LOG.info("Patch");
        Id ref = Id.fromString(action.httpArgs.dataset);
        try (InputStream in = action.request.getInputStream()) {
            RDFPatch patch = RDFPatchOps.read(in);
            int version = action.dLink.sendPatch(ref, patch);
            JsonValue x = JsonNumber.value(version);
            JsonValue rslt = JsonBuilder.create()
                .startObject()
                .key(DPConst.F_VERSION).value(version)
                .finishObject()
                .build();
            OutputStream out = action.response.getOutputStream();
            action.response.setContentType(WebContent.contentTypeJSON);
            action.response.setStatus(HttpSC.OK_200);
            JSON.write(out, rslt);
            out.flush();
        } catch (RuntimeException ex) {
            ex.printStackTrace(System.err);
            LOG.warn("Failed to process", ex); 
            action.response.sendError(HttpSC.INTERNAL_SERVER_ERROR_500, ex.getMessage()) ;
            return ;
        }
    }
    
    private static Id getDataId(DeltaAction action) {
        String datasetStr = action.httpArgs.dataset;
        if ( datasetStr != null )
            return Id.fromString(datasetStr) ;
       
        // Get from path
        String s = action.request.getServletPath() ;
        String requestURI = action.request.getRequestURI() ;
        if ( requestURI.startsWith(s) ) {
            String dname = requestURI.substring(s.length()) ;
            if ( dname.isEmpty() )
                throw new DeltaBadRequestException("No dataset parameter");
            LOG.info("Dataset name (path) = "+dname);
            return Id.fromString(dname) ;
        }
        throw new DeltaBadRequestException("Failed to find dataset parameter");
    }
}
