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

package org.seaborne.delta.server;

import java.io.IOException ;

import javax.servlet.ServletConfig ;
import javax.servlet.ServletException ;
import javax.servlet.http.HttpServlet ;
import javax.servlet.http.HttpServletRequest ;
import javax.servlet.http.HttpServletResponse ;

import org.apache.jena.fuseki.server.RequestLog ;
import org.seaborne.delta.Delta ;
import org.slf4j.Logger ;

public class ServletBase extends HttpServlet {

    private static Logger logger = Delta.getDeltaLogger("Servlet") ; 
    
    @Override
    public void init(ServletConfig config) throws ServletException {}

    @Override
    public ServletConfig getServletConfig() {
        return null ;
    }

    @Override
    protected void service(HttpServletRequest req, HttpServletResponse resp) {
        try { 
            // "there is no need to override this method"!!!
            super.service(req, resp);
            if ( false ) {
                String x = RequestLog.combinedNCSA(req, resp) ;
                logger.info(x);
            }
        } catch (Exception ex) {
            
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
