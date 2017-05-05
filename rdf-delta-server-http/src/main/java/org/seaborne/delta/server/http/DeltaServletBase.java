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
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import javax.servlet.http.HttpServlet ;
import javax.servlet.http.HttpServletRequest ;
import javax.servlet.http.HttpServletResponse ;

import org.apache.jena.fuseki.server.RequestLog ;
import org.apache.jena.fuseki.servlets.ServletOps ;
import org.apache.jena.riot.web.HttpNames ;
import org.apache.jena.web.HttpSC;
import org.seaborne.delta.* ;
import org.seaborne.delta.link.DeltaLink;
import org.seaborne.delta.link.RegToken;
import org.slf4j.Logger ;

/** Servlet and multiplexer for DeltaLinks */
public abstract class DeltaServletBase extends HttpServlet { 

    protected static Logger logger = Delta.getDeltaLogger("DeltaServlet") ;
    // Switchable (for tests).
    protected final AtomicReference<DeltaLink> engine;
    
    //protected final DeltaLinkMgr linkMgr = new DeltaLinkMgr();
    
    // Static to catch cross contamination.
    protected final Map<Id, DeltaLink> links = new ConcurrentHashMap<>();
    // These should be unique across the server.
    // XXX [JVM-global registrations]
    protected static final Map<RegToken, Id> registrations = new ConcurrentHashMap<>();
    public static void clearRegistration(RegToken regToken) { registrations.remove(regToken) ; }  
    public static void clearAllRegistrations()              { registrations.clear(); }
    
    protected DeltaServletBase(AtomicReference<DeltaLink> engine) {
        this.engine = engine;
    }
    
//    @Override
//    public void init(ServletConfig config) throws ServletException {
//        super.init(config);
//    }
//
//    @Override
//    public ServletConfig getServletConfig() {
//        return super.getServletConfig() ;
//    }

    public DeltaLink getLink() {
        return engine.get();
    }
    
    protected abstract DeltaAction parseRequest(HttpServletRequest req, HttpServletResponse resp) throws IOException;
    protected abstract void validateAction(DeltaAction action) throws IOException;
    protected abstract void executeAction(DeltaAction action) throws IOException;

    protected void register(Id client, RegToken token) {
        registrations.put(token, client);
    }

    protected Id getRegistration(RegToken token) {
        return registrations.get(token);
    }

    protected void deregister(RegToken token) {
        registrations.remove(token);
    }
    
    protected boolean isRegistered(RegToken token) {
        if ( token == null )
            return false;
        return registrations.containsKey(token);
    }

    @Override
    protected void service(HttpServletRequest req, HttpServletResponse resp) {
        try {
            // Add PATCH
            String method = req.getMethod() ;
            if ( method.equals(HttpNames.METHOD_PATCH) ) {
                doPatch(req, resp);
            } else
                super.service(req, resp);
            if ( false ) {
                String x = RequestLog.combinedNCSA(req, resp) ;
                logger.info(x);
            }
        } catch (DeltaBadRequestException ex) {
            //ex.printStackTrace(System.err);
            Delta.DELTA_LOG.warn("Bad request: "+ex.getMessage());
            try {
                resp.sendError(ex.getStatusCode(), ex.getMessage()) ;
            } catch (IOException ex2) {}
        } catch (Exception ex) {
            ex.printStackTrace(System.err);
            Delta.DELTA_LOG.error(ex.getMessage(), ex);
            try { resp.sendError(HttpSC.INTERNAL_SERVER_ERROR_500, ex.getMessage()) ; }
            catch (IOException ex2) {}
        }
    }

    // Default actions - do nothing.
    
    protected void notSupported(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        String protocol = req.getProtocol();
        String msg = "HTTP "+req.getMethod()+" not supported" ;
        if (protocol.endsWith("1.1")) {
            ServletOps.error(HttpServletResponse.SC_METHOD_NOT_ALLOWED, msg);
        } else {
            ServletOps.error(HttpServletResponse.SC_BAD_REQUEST, msg);
        }
    }

    // Override in the actual operation to select which of GET/POST/PATCH is supported */  
    protected void doPatch(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        notSupported(req, resp);
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        notSupported(req, resp);
    }
    
    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        notSupported(req, resp);
    }
    
    @Override
    public String getServletInfo() {
        return this.getClass().getSimpleName();
    }

    @Override
    public void destroy() {}
    
    /** The common lifecycle. */
    protected void doCommon(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        try {
            DeltaAction action = parseRequest(req, resp);
            validateAction(action);
            executeAction(action);
        }
        catch (DeltaBadRequestException ex) {
            logger.error("400 Bad request : "+ex.getMessage());
            resp.sendError(HttpSC.BAD_REQUEST_400, "Bad request: "+ex.getMessage());
        }
        catch (DeltaException ex) {
            logger.error("400 Bad request : "+ex.getMessage(), ex);
            resp.sendError(HttpSC.BAD_REQUEST_400, "Bad request: "+ex.getMessage());
        }
        catch (Throwable ex) {
            logger.error("Internal server error", ex);
            ex.printStackTrace();
            resp.sendError(HttpSC.INTERNAL_SERVER_ERROR_500, "Internal server error: "+ex.getMessage());
        }
    }
}
