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

import java.io.IOException;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import org.seaborne.delta.link.DeltaLink;

/** Base class for operations working on HTTPrequest directly, unlike RPCs */ 
public abstract class HttpOperationBase extends DeltaServlet {

    public HttpOperationBase(DeltaLink engine) {
        super(engine);
    }
    
    @Override
    final
    protected DeltaAction parseRequest(HttpServletRequest request, HttpServletResponse response) throws IOException {
        Args args = parseArgs(request);
        return DeltaAction.create(request, response, getLink(), args.token, getOpName(), null, args);
    }

    @Override
    final
    protected void validateAction(DeltaAction action) throws IOException {
        validateAction(action.httpArgs);
    }

    protected Args parseArgs(HttpServletRequest request) {
        // Default - parse on query string. 
        return Args.argsParams(request);
    }
    
    protected abstract void validateAction(Args httpArgs);
    
    protected abstract String getOpName();
}
