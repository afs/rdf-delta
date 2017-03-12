/*
remoteData; * Licensed to the Apache Software Foundation (ASF) under one
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

import java.io.FileInputStream ;
import java.io.IOException;
import java.io.InputStream ;
import java.util.concurrent.atomic.AtomicReference;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.io.IOUtils ;
import org.apache.jena.atlas.web.ContentType ;
import org.apache.jena.riot.RDFLanguages ;
import org.apache.jena.web.HttpSC ;
import org.seaborne.delta.Delta;
import org.seaborne.delta.Id ;
import org.seaborne.delta.link.DeltaLink;
import org.slf4j.Logger ;

/** Data over HTTP. */ 
public class S_Data extends HttpOperationBase {
    static private Logger LOG = Delta.getDeltaLogger("Data") ;
    
    public S_Data(AtomicReference<DeltaLink> engine) {
        super(engine);
    }

    // Fetch a file
    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        doCommon(req, resp);
    }
    
    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        doCommon(req, resp);
    }
    
    // XXX Share with S_Fetch
    
    @Override
    protected void checkRegistration(DeltaAction action) {
        // Only warnings.
        if ( action.regToken == null )
            logger.warn("Data: No registration token") ;
        if ( !isRegistered(action.regToken) )
            logger.warn("Data: Not registered") ;
    }
    
    @Override
    protected void validateAction(Args httpArgs) {
    }
    
    @Override
    protected void executeAction(DeltaAction action) throws IOException {
        LOG.info("GET");
        Id dsRef = Id.fromString(action.httpArgs.dataset);
        String filenameIRI = determineData(action, dsRef);
        ContentType ct = RDFLanguages.guessContentType(filenameIRI) ;
        String fn = filenameIRI.substring("file:".length());
        InputStream in = new FileInputStream(fn);
        action.response.setStatus(HttpSC.OK_200);
        action.response.setContentType(ct.getContentType());
        IOUtils.copy(in, action.response.getOutputStream());
    }
    
    private String determineData(DeltaAction action, Id dsRef) {
        return action.dLink.initialState(dsRef);
    }

    @Override
    protected String getOpName() {
        return "initial-data";
    }
}