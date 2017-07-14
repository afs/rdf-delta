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

import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.seaborne.delta.DeltaBadRequestException;
import org.seaborne.delta.link.DeltaLink;

/** Framework for fetching a patch over HTTP. */ 
public class S_Fetch extends HttpOperationBase {

    public S_Fetch(AtomicReference<DeltaLink> engine) {
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
    
    @Override
    protected void checkRegistration(DeltaAction action) {
        // Only warnings.
        if ( action.regToken == null )
            logger.warn("Fetch: No registration token") ;
        if ( !isRegistered(action.regToken) )
            logger.warn("Fetch: Not registered") ;
    }
    
    @Override
    protected void validateAction(Args httpArgs) {
//        if ( httpArgs.zone == null )
//            Delta.DELTA_HTTP_LOG.warn("No Zone specified");
        if ( httpArgs.datasourceName == null )
            throw new DeltaBadRequestException("No datasource specified");
        if ( httpArgs.patchId == null && httpArgs.version == null )
            throw new DeltaBadRequestException("No version, no patch id");
    }
    
    @Override
    protected void executeAction(DeltaAction action) throws IOException {
        LogOp.fetch(action);
    }
    
    @Override
    protected String getOpName() {
        return "patch-fetch";
    }
}