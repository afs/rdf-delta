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

package org.seaborne.delta.server2.http;

import java.io.IOException ;
import java.io.OutputStream ;
import java.nio.file.Files ;
import java.nio.file.Path ;
import java.nio.file.Paths ;

import javax.servlet.http.HttpServletRequest ;
import javax.servlet.http.HttpServletResponse ;

import org.apache.jena.riot.WebContent ;
import org.apache.jena.web.HttpSC ;
import org.seaborne.delta.Delta ;
import org.seaborne.delta.server2.DPS ;
import org.slf4j.Logger ;

public class S_FetchCode extends ServletBase {
    
    static public Logger LOG = Delta.getDeltaLogger("Fetch") ;

    static abstract class FetchBase  extends ServletBase {

        protected abstract int getPatchId(HttpServletRequest req, HttpServletResponse resp) throws IOException ; 

        // Fetch a file
        @Override
        protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws IOException {
            int id = getPatchId(req, resp) ;
            if ( id < 0 )
                return ;
            String filename = DPS.patchFilename(id) ;
            Path path = Paths.get(filename) ;

            resp.setContentType("application/rdf-patch+text"); 
            resp.setCharacterEncoding(WebContent.charsetUTF8);
            OutputStream out = resp.getOutputStream() ;

            if ( ! Files.exists(path) ) {
                LOG.info("No such patch: "+filename) ;
                resp.sendError(HttpSC.NOT_FOUND_404, "No such patch file: "+filename ) ;
                return ;
            }

            LOG.info("Patch = "+filename) ;
            Files.copy(path, out) ;
            out.flush();
            resp.setStatus(HttpSC.OK_200);
        }
    }

    public static class S_FetchId extends FetchBase {
        @Override
        protected int getPatchId(HttpServletRequest req, HttpServletResponse resp) throws IOException {
            String paramId = req.getParameter("id") ;
            int id = -999 ;
            try { return Integer.parseInt(paramId) ; }
            catch (NumberFormatException ex) {
                resp.sendError(HttpSC.BAD_REQUEST_400, "Failed to parse the 'id' parameter" ) ;
                return -1 ;
            }
        }
    }

    public static class S_FetchREST extends FetchBase {
        @Override
        protected int getPatchId(HttpServletRequest req, HttpServletResponse resp) throws IOException {
            String x = req.getRequestURI() ;
            int j = x.lastIndexOf('/') ;
            if ( j < 0 ) {
                resp.sendError(HttpSC.BAD_REQUEST_400, "Failed to find the patch id" ) ;
                return -1 ;
            }
            String z = x.substring(j+1) ;
            if ( z.equals("size") ) {
                
            }
            
            try { return Integer.parseInt(z) ; }
            catch (NumberFormatException ex) {
                resp.sendError(HttpSC.BAD_REQUEST_400, "Failed to extract the id" ) ;
                return -1 ;
            }
        }
    }
}

