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

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.jena.riot.web.HttpNames ;
import org.seaborne.delta.Delta ;
import org.seaborne.delta.link.DeltaLink;
import org.slf4j.Logger;

/** Servlet for both append and fetch patches - the RDF Patch protocol.
 *    <tt>GET  /{name}/id</tt> -- get patch
 *    <tt>GET  /{name}/version</tt> -- get patch
 *    <tt>POST /{name}/</tt> -- append patch.
 */
public class S_GetPostLog extends HttpOperationBase {

    public S_GetPostLog(DeltaLink engine) {
        super(engine);
    }

    static private Logger LOG = Delta.getDeltaLogger("Patch") ;

    @Override
    protected Args parseArgs(HttpServletRequest request) {
        return Args.pathArgs(request);
    }

    @Override
    protected void validateAction(Args httpArgs) {
        if ( isFetchOperation(httpArgs) )
            return ;
        if ( isAppendOperation(httpArgs) )
            return ;
        DeltaAction.errorBadRequest("Not a log fetch or append operation : "+httpArgs.url);
    }

    @Override
    protected void executeAction(DeltaAction action) throws IOException {
        if ( isFetchOperation(action) )
            LogOp.fetch(action);
        else
            LogOp.append(action);
    }

    // Supported as HTTP methods but then rejected in "validate action" if not appropriate.
    @Override
    protected void doPatch(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        doCommon(req, resp);
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        doCommon(req, resp);
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        doCommon(req, resp);
    }

    @Override
    protected String getOpName() {
        return "patch-log";
    }

    private boolean isFetchOperation(DeltaAction action) {
        return isFetchOperation(action.httpArgs);
    }

    private boolean isAppendOperation(DeltaAction action) {
        return isAppendOperation(action.httpArgs);
    }

    // Minimum for a log operation.
    private boolean isLogOperation(Args args) {
        return args.datasourceName != null;
    }

    private boolean isFetchOperation(Args args) {
        return isLogOperation(args)
            && (args.patchId != null || args.version != null ) ;
    }

    private boolean isAppendOperation(Args args) {
        return isLogOperation(args)
            && (args.method.equals(HttpNames.METHOD_POST) || args.method.equals(HttpNames.METHOD_PATCH))
            && (args.patchId == null && args.version == null);
    }
}
