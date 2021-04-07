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

import java.io.IOException ;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.servlet.http.HttpServlet ;
import javax.servlet.http.HttpServletRequest ;
import javax.servlet.http.HttpServletResponse ;

import org.apache.jena.atlas.json.JSON;
import org.apache.jena.fuseki.server.RequestLog ;
import org.apache.jena.fuseki.servlets.ActionErrorException ;
import org.apache.jena.fuseki.servlets.ServletOps ;
import org.apache.jena.riot.WebContent;
import org.apache.jena.riot.web.HttpNames ;
import org.apache.jena.web.HttpSC;
import org.seaborne.delta.*;
import org.seaborne.delta.link.DeltaLink;
import org.slf4j.Logger ;

/** Servlet and multiplexer for DeltaLinks */
public abstract class DeltaServlet extends HttpServlet {

    protected static Logger logger = Delta.getDeltaLogger("DeltaServlet") ;
    // Switchable so the server does not need to be restarted (for tests and admin operations).
    protected final DeltaLink engine;

    //protected final DeltaLinkMgr linkMgr = new DeltaLinkMgr();

    // Static to catch cross contamination.
    protected final Map<Id, DeltaLink> links = new ConcurrentHashMap<>();

    /** Automatically register when a RegToken is seen that is not recorded as registered.*/
    private static final boolean AutoRegistration = false;

    protected DeltaServlet(DeltaLink engine) {
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
        return engine;
    }

    protected abstract DeltaAction parseRequest(HttpServletRequest req, HttpServletResponse resp) throws IOException;
    protected abstract void validateAction(DeltaAction action) throws IOException;
    protected abstract void executeAction(DeltaAction action) throws IOException;

    /**
     * {@code HttpServlet.service} : add PATCH, add protection for exceptions as a fallback.
     */
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
        } catch (DeltaHttpException ex) {
            // doCommon() should handle these.
            Delta.DELTA_LOG.error("HTTP exception: "+ex.getStatusCode()+ " -- "+ex.getMessage());
            try { resp.sendError(ex.getStatusCode(), ex.getMessage()) ; } catch (IOException ex2) {}
        } catch (ActionErrorException ex) {
            // Should not happen - comes from ServletOps, not DeltaAction.
            Delta.DELTA_LOG.error("HTTP exception (ActionError): "+ex.getRC()+" -- "+ex.getMessage());
            try { resp.sendError(ex.getRC(), ex.getMessage()) ; } catch (IOException ex2) {}
        } catch (Throwable ex) {
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
        catch (DeltaPatchVersionException ex) {
            resp.setStatus(ex.getStatusCode());
            resp.addHeader(HttpNames.hContentType, WebContent.contentTypeJSON);
            JSON.write(resp.getOutputStream(), ex.getBody());
        }
        catch (DeltaBadRequestException ex) {
            String msg = ex.getMessage();
            if ( msg == null )
                msg = "Bad request";
            int sc = ex.getStatusCode();
            logger.info(sc+" Bad request : "+ex.getMessage());
            resp.sendError(ex.getStatusCode(), msg);
        }
        catch (DeltaHttpException ex) {
            logger.info(ex.getStatusCode()+" "+ex.getMessage());
            resp.sendError(ex.getStatusCode(), ex.getMessage());
        }
        // Unexpected exceptions case handled by service()
    }
}
