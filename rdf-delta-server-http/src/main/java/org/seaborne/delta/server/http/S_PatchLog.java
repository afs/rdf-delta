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

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.jena.atlas.logging.Log;
import org.seaborne.delta.DPConst;
import org.seaborne.delta.Delta;
import org.seaborne.delta.link.DeltaLink;
import org.slf4j.Logger;

// Switch between two engines - the "fetch patch" code and the "send patch" code.  
public class S_PatchLog extends HttpOperationBase {
    private final HttpOperationBase fetchServlet;
    private final HttpOperationBase appendServlet; 
    
    
    public S_PatchLog(AtomicReference<DeltaLink> engine) {
        super(engine);
        this.fetchServlet = new S_Fetch(engine);
        this.appendServlet = new S_Patch(engine);
    }

    static private Logger LOG = Delta.getDeltaLogger("Patch") ;

    @Override
    protected void checkRegistration(DeltaAction action) {
        if ( isFetchOperation(action) )
            fetchServlet.checkRegistration(action);
        else
            appendServlet.checkRegistration(action);
    }

    @Override
    protected void validateAction(Args httpArgs) {
        if ( isFetchOperation(httpArgs) )
            fetchServlet.validateAction(httpArgs);
        else
            appendServlet.validateAction(httpArgs);
    }

    @Override
    protected void executeAction(DeltaAction action) throws IOException {
        if ( isFetchOperation(action) )
            fetchServlet.executeAction(action);
        else
            appendServlet.executeAction(action);
    }

    @Override
    protected void doPatch(HttpServletRequest request, HttpServletResponse response) throws IOException {
        // Must be a push of a patch.
        if ( isAppendOperation(request) ) {
            executePush(request, response);
            return;
        }
        super.doPatch(request, response);
    }
    
    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response) throws IOException {
        if ( isAppendOperation(request) ) {
            executePush(request, response);
            return;
        }
        super.doPost(request, response);
    }
    
    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
        if ( isAppendOperation(request) ) {
            // Can't GET to push a patch.
            super.doGet(request, response);
            return;
        }
        String alt = getTrailing(request);
//        if ( alt != null ) {
//            ServletOps.errorNotImplemented("Access patch by path \"log/id\": trailing part is "+alt);
//        }
        executeFetch(request, response);
    }
    
    @Override
    protected String getOpName() {
        return "patch-log";
    }
    
    protected boolean isAppendOperation(HttpServletRequest request) {
        return request.getParameter(DPConst.paramPatch) == null &&
               request.getParameter(DPConst.paramVersion) == null ;
    }

    protected boolean isFetchOperation(HttpServletRequest request) {
        return request.getParameter(DPConst.paramPatch) != null ||
               request.getParameter(DPConst.paramVersion) != null ;
    }

    private boolean isFetchOperation(DeltaAction action) {
        return isFetchOperation(action.httpArgs);
    }

    private boolean isFetchOperation(Args args) {
        return args.patchId != null || args.version != null;
    }

    private void executeFetch(HttpServletRequest request, HttpServletResponse response) throws IOException {
        // Same lifecycle - each step redirects currently.
        doCommon(request, response);
    }
        
    private void executePush(HttpServletRequest request, HttpServletResponse response) throws IOException {
        // Same lifecycle - each step redirects currently.
        doCommon(request, response);
    }

    /**
     * Get the trailing part of a request URI.
     * The URI is assumed to be in the form "/context/servlet/trailing". 
     * @return The trailing part or   
     */
    protected String getTrailing(HttpServletRequest request) {
      Log.info(this, "URI                     = '"+request.getRequestURI()) ;
      Log.info(this, "Context path            = '"+request.getContextPath()+"'") ;
      Log.info(this, "Servlet path            = '"+request.getServletPath()+"'") ;
      // Only valid for webapps.
      ServletContext cxt = this.getServletContext() ;
      Log.info(this, "ServletContext path     = '"+cxt.getContextPath()+"'") ;
        
        // URL naming version ; URI is "context/servletname/   
        String servletPath = request.getServletPath() ;
        String uri = request.getRequestURI() ;
        String x = uri ;
        if ( uri.startsWith(servletPath) )
            x = uri.substring(servletPath.length()) ;
        //log.info("uriWithoutContextPath: uri = "+uri+" contextPath="+contextPath+ "--> x="+x) ;
        return x ;
        
    } 

}
