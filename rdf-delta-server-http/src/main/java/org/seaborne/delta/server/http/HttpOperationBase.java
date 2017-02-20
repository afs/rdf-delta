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
import org.seaborne.delta.link.DeltaLink;

/** Base class for operations working on HTTPrequest directly, unlike RPCs */ 
public abstract class HttpOperationBase extends DeltaServletBase {

    public HttpOperationBase(AtomicReference<DeltaLink> engine) {
        super(engine);
    }
    
    protected Args getArgs(HttpServletRequest request) throws IOException {
        return Args.args(request);
    }
    
    @Override
    final
    protected DeltaAction parseRequest(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        Args args = getArgs(req);
        return DeltaAction.create(req, resp, getLink(), args.regToken, getOpName(), args);
    }

    @Override
    final
    protected void validateAction(DeltaAction action) throws IOException {
        checkRegistration(action);
        validateAction(action.httpArgs);
    }

    protected abstract void checkRegistration(DeltaAction action);

    protected abstract void validateAction(Args httpArgs);
    
    protected abstract String getOpName();
}
