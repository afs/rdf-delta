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

import javax.servlet.Servlet ;

import org.eclipse.jetty.server.Server ;
import org.eclipse.jetty.server.handler.ErrorHandler ;
import org.eclipse.jetty.servlet.ServletHandler ;
import org.eclipse.jetty.servlet.ServletHolder ;
import org.seaborne.delta.DPS ;
import org.seaborne.delta.server.handlers.PHandlerGSP ;
import org.seaborne.delta.server.handlers.PHandlerLog ;
import org.seaborne.delta.server.handlers.PHandlerOutput ;
import org.seaborne.delta.server.handlers.PHandlerToFile ;

/** A simple packaging of Jetty to provide an embeddable HTTP server that just support servlets */ 
public class DataPatchServer {
    
    private final boolean loopback = false ;
    private final Server server ;
    private ServletHandler handler ;

    public DataPatchServer(int port) {
        //Init file store.
        DPS.init() ;
        server = new Server(port) ;
        ErrorHandler eh = new ErrorHandlerDataPatch() ;
        eh.setServer(server);
        handler = new ServletHandler();
        server.setHandler(handler);
        server.addBean(eh) ;
        
        S_Patch patchMgr = new S_Patch() ;
        // Setup
        patchMgr.addHandler(new PHandlerOutput(System.out)) ;
        patchMgr.addHandler(new PHandlerGSP()) ;
        patchMgr.addHandler(new PHandlerToFile()) ;
        patchMgr.addHandler(new PHandlerLog(DPS.LOG)) ;
        
        addServlet("/fetch", new S_FetchCode.S_FetchId()) ;
        addServlet("/patch", patchMgr) ;
        addServlet("/patch/*", new S_FetchCode.S_FetchREST()) ;
    }
    
    public void addServlet(String path, Servlet servlet) {
        handler.addServletWithMapping(new ServletHolder(servlet), path);
    }
    
    public void start() {
        try {
            server.start();
            //server.dumpStdErr();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    public void join() {
        try {
            server.join();
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
