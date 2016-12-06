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

import java.io.IOException;
import java.io.OutputStream;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.jena.atlas.io.IO;
import org.apache.jena.atlas.logging.FmtLog;
import org.apache.jena.web.HttpSC;
import org.seaborne.delta.DeltaBadRequestException;
import org.seaborne.delta.conn.DeltaConnection ;
import org.seaborne.delta.conn.Id ;
import org.seaborne.delta.server.local.DataRegistry;
import org.seaborne.delta.server.local.DataSource;
import org.seaborne.patch.RDFPatch ;
import org.seaborne.patch.RDFPatchOps ;

/** Framework for fetching a patch over HTTP. */ 
abstract class FetchBase extends ServletBase {

    public FetchBase(DeltaConnection engine) {
        super(engine);
    }

    /** Extract the arguments. In case errors, throw DeltaExceptionBadRequest */
    protected abstract Args getArgs(HttpServletRequest req); ; 

    // Fetch a file
    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        doCommon(req, resp);
    }
    
    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        doCommon(req, resp);
    }
    
    private void doCommon(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        try {
            //Get dataset and id.
            // :: /zone/dataset/id
            // :: /registrationId : mapped to zone/dataset
            
            // Or short names for zone,dataset
            // /z-1/ds-5/12345-5678-9-0-977
            
            Args args = getArgs(req) ;
            if ( args.dataset == null ) {
                throw new DeltaBadRequestException("No datasource specificed");
            }
            Id dsRef = Id.fromString(args.dataset);
            DataSource ds = DataRegistry.get().get(dsRef);
            if ( ds == null )
                throw new DeltaBadRequestException("Not found: datasource for "+args.dataset);
            
            
            OutputStream out = resp.getOutputStream() ;
            RDFPatch patch;
            
            if ( args.patchId == null ) {
                if ( args.version == null )
                    throw new DeltaBadRequestException("No version, no patch id");
                int version = Integer.parseInt(args.version);
                patch = engine.fetch(dsRef, version);
            } else {
                Id patchId = Id.fromString(args.patchId) ;
                patch = engine.fetch(dsRef, patchId);
            }
//          resp.setCharacterEncoding(WebContent.charsetUTF8);
            resp.setStatus(HttpSC.OK_200);
            resp.setContentType("application/rdf-patch+text"); 
            RDFPatchOps.write(out, patch);
            IO.flush(out);
        } catch (DeltaBadRequestException ex) {
            FmtLog.warn(S_Fetch.LOG, "", ex.getStatusCode(), ex.getMessage());
            resp.sendError(ex.getStatusCode(), ex.getMessage());
            return;
        }
            
//            API.fetch(null, null);
//            
//            String filename = DPS.patchFilename(id) ;
//            Path path = Paths.get(filename) ;
//
//            resp.setContentType("application/rdf-patch+text"); 
//            resp.setCharacterEncoding(WebContent.charsetUTF8);
//            OutputStream out = resp.getOutputStream() ;
//
//            if ( ! Files.exists(path) ) {
//                S_Fetch.LOG.info("No such patch: "+filename) ;
//                resp.sendError(HttpSC.NOT_FOUND_404, "No such patch file: "+filename ) ;
//                return ;
//            }
//
//            S_Fetch.LOG.info("Patch = "+filename) ;
//            Files.copy(path, out) ;
//            out.flush();
//            resp.setStatus(HttpSC.OK_200);
//        } catch(DeltaExceptionBadRequest ex) {
//            resp.sendError(ex.getStatusCode(), ex.getMessage()) ;
//        }

    }
}