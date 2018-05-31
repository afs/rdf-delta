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
import java.util.concurrent.atomic.AtomicReference;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.jena.web.HttpSC ;
import org.seaborne.delta.DeltaHttpException ;
import org.seaborne.delta.link.DeltaLink;
import org.seaborne.delta.link.DeltaNotRegisteredException ;

/** Base class for operations working on HTTPrequest directly, unlike RPCs */ 
public abstract class HttpOperationBase extends DeltaServlet {

    public HttpOperationBase(AtomicReference<DeltaLink> engine) {
        super(engine);
    }
    
    @Override
    final
    protected DeltaAction parseRequest(HttpServletRequest request, HttpServletResponse response) throws IOException {
        Args args = parseArgs(request);
        return DeltaAction.create(request, response, getLink(), args.regToken, getOpName(), null, args);
    }

    @Override
    final
    protected void validateAction(DeltaAction action) throws IOException {
        checkRegistration(action);
        validateAction(action.httpArgs);
    }

    protected Args parseArgs(HttpServletRequest request) {
        // Default - parse on query string. 
        return Args.argsParams(request);
    }
    
    protected abstract void checkRegistration(DeltaAction action);
    
    /** Helper implementation of checkRegistration when resgutration required. */
    protected void checkRegistered(DeltaAction action) {
        if ( action.regToken == null )
            throw new DeltaHttpException(HttpSC.FORBIDDEN_403, "No registration token") ;
        if ( !isRegistered(action.regToken) )
            throw new DeltaNotRegisteredException("Not registered") ;
    }

    protected abstract void validateAction(Args httpArgs);
    
    protected abstract String getOpName();
}
