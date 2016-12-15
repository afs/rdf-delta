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

import javax.servlet.ServletConfig ;
import javax.servlet.ServletException ;
import javax.servlet.http.HttpServlet ;
import javax.servlet.http.HttpServletRequest ;
import javax.servlet.http.HttpServletResponse ;

import org.apache.jena.fuseki.server.RequestLog ;
import org.apache.jena.fuseki.servlets.ServletOps ;
import org.apache.jena.riot.web.HttpNames ;
import org.apache.jena.web.HttpSC;
import org.seaborne.delta.Delta ;
import org.seaborne.delta.DeltaBadRequestException;
import org.seaborne.delta.link.DeltaLink;
import org.slf4j.Logger ;

public class ServletBase extends HttpServlet {

    private static Logger logger = Delta.getDeltaLogger("Servlet") ;
    protected final DeltaLink engine ; 
    
    public ServletBase(DeltaLink engine) {
        this.engine = engine;
    }

    @Override
    public void init(ServletConfig config) throws ServletException {}

    @Override
    public ServletConfig getServletConfig() {
        return null ;
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
            ex.printStackTrace(System.err);
            Delta.DELTA_LOG.warn("Bad request: "+ex.getMessage());
            try {
                resp.sendError(HttpSC.BAD_REQUEST_400, ex.getMessage()) ;
            } catch (IOException ex2) {}
        } catch (Exception ex) {
            ex.printStackTrace(System.err);
            Delta.DELTA_LOG.error(ex.getMessage(), ex);
            try { resp.sendError(HttpSC.INTERNAL_SERVER_ERROR_500, ex.getMessage()) ; }
            catch (IOException ex2) {}
        }
    }

    protected void doPatch(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        String protocol = req.getProtocol();
        String msg = "HTTP PATCH not support" ;
        if (protocol.endsWith("1.1")) {
            ServletOps.error(HttpServletResponse.SC_METHOD_NOT_ALLOWED, msg);
        } else {
            ServletOps.error(HttpServletResponse.SC_BAD_REQUEST, msg);
        }
    }

    @Override
    public String getServletInfo() {
        return "ServletBase" ;
    }

    @Override
    public void destroy() {}
    
    // Short term expediance - convert GET to POST
    // TODO Remove before release!
    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        doPost(req, resp);
    }

}
