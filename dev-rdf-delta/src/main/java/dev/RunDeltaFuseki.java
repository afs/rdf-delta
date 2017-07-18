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

package dev;

import java.io.IOException;

import javax.servlet.*;

import org.apache.jena.atlas.logging.FmtLog;
import org.apache.jena.atlas.logging.LogCtl;
import org.apache.jena.fuseki.embedded.FusekiEmbeddedServer ;
import org.apache.jena.sparql.core.DatasetGraphFactory;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.slf4j.Logger ;
import org.slf4j.LoggerFactory ;

public class RunDeltaFuseki {
    static { 
        LogCtl.setCmdLogging();
        LogCtl.setJavaLogging();
    }
    
    static Logger LOG = LoggerFactory.getLogger("Main") ;
    
    public static void main(String[] args) throws Exception {
        FusekiEmbeddedServer server = FusekiEmbeddedServer.create()
            .setPort(3033)
            .add("XYZ", DatasetGraphFactory.createTxnMem())
            .build();
        
        ServletContextHandler context = (ServletContextHandler)(server.getJettyServer().getHandler());
        // Wire in a filter!
        server.getDataAccessPointRegistry().forEach((name, dap)->{
            FilterHolder fh = new FilterHolder();
            Filter patchFilter = new PatchFilter();
            fh.setFilter(patchFilter);
            if ( name.startsWith("/") )
                name = name.substring(1);
            String path = "/"+name+"/patch";
            System.err.println("Add "+path);
            // Too late
            context.addFilter(fh, path, null);
        }); 

//        FilterHolder fh = new FilterHolder();
//        Filter patchFilter = new PatchFilter();
//        fh.setFilter(patchFilter);
//        String path = "/XYZ/patch";
//        context.addFilter(fh, path, null);
//        //server.getJettyServer().setHandler(context);
        
        server
            .start()
            .join();
    }
    
    static class PatchFilter implements Filter {

        @Override
        public void init(FilterConfig filterConfig) throws ServletException {}

        @Override
        public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException, ServletException {
            FmtLog.info(LOG, "PatchFilter"); 
            
            new FusekiPatch()
            .service(request, response);
            return ;
        }

        @Override
        public void destroy() {}
        
    }
}
