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

import javax.servlet.http.HttpServletRequest ;
import javax.servlet.http.HttpServletResponse ;

import org.apache.jena.atlas.json.JsonBuilder;
import org.apache.jena.atlas.json.JsonNumber;
import org.apache.jena.atlas.json.JsonValue;
import org.apache.jena.web.HttpSC ;
import org.seaborne.delta.DPNames;
import org.seaborne.delta.Delta ;
import org.seaborne.delta.DeltaBadRequestException;
import org.seaborne.delta.Id;
import org.seaborne.delta.link.DeltaLink;
import org.seaborne.patch.RDFPatch ;
import org.seaborne.patch.RDFPatchOps ;
import org.slf4j.Logger ;

/** Receive an incoming patch.
 * Translates from HTTP to the internal protocol agnostic API. 
 */
public class S_Patch extends ServletBase {
    static private Logger LOG = Delta.getDeltaLogger("Patch") ;
    
    public S_Patch(DeltaLink engine) {
        super(engine);
    }
    
    @Override
    protected void doPatch(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        doPost(req, resp);
    }
    
    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        LOG.info("Patch");
        Args args = Args.args(req);
        Id ref ; 
        try {
            ref = getDataId(req) ;
            LOG.info("Dataset ref = "+ref);
        } catch (RuntimeException ex) {
            resp.sendError(HttpSC.BAD_REQUEST_400, "Bad data id") ;
            return ;
        }
        try (InputStream in = req.getInputStream()) {
            RDFPatch patch = RDFPatchOps.read(in);
            int version = engine.sendPatch(ref, patch);
            JsonValue x = JsonNumber.value(version);
            JsonValue rslt = JsonBuilder.create()
                .startObject()
                .key(DPNames.F_VERSION).value(version)
                .finishObject()
                .build();
            
            //resp.setStatus(HttpSC.NO_CONTENT_204) ;
        } catch (RuntimeException ex) {
            ex.printStackTrace(System.err);
            LOG.warn("Failed to process", ex); 
            resp.sendError(HttpSC.INTERNAL_SERVER_ERROR_500, ex.getMessage()) ;
            return ;
        }
    }
    
    private static Id getDataId(HttpServletRequest req) {
        // XXX Args
        Args args = Args.args(req);
        // -----
        //String datasetStr = req.getParameter(C.paramDataset) ;
        String datasetStr = args.dataset;
        
        if ( datasetStr != null )
            return Id.fromString(datasetStr) ;
       
        // Get from path
        String s = req.getServletPath() ;
        String requestURI = req.getRequestURI() ;
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
