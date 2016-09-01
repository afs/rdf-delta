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
import java.io.InputStream ;
import java.util.ArrayList ;
import java.util.LinkedList ;
import java.util.List ;

import javax.servlet.http.HttpServletRequest ;
import javax.servlet.http.HttpServletResponse ;

import org.apache.jena.atlas.lib.Lib ;
import org.apache.jena.web.HttpSC ;
import org.seaborne.delta.Delta ;
import org.seaborne.delta.base.PatchReader ;
import org.seaborne.patch.RDFChanges ;
import org.seaborne.patch.RDFChangesCollector ;
import org.slf4j.Logger ;

/** Receive an incoming patch file and put on disk (safely : something else may try to read it while its being written. */ 
public class S_Patch extends ServletBase {
    // Push a file.
    static private Logger LOG = Delta.getDeltaLogger("Patch") ;
    
    static boolean verbose = false ;
    
    static class IncomingPatch {}
    
    private List<PatchHandler> handlers = new LinkedList<>() ;
    public List<PatchHandler> handlers() { return new ArrayList<PatchHandler>(handlers) ; }
    public void addHandler(PatchHandler ph) { handlers.add(ph) ; }    
    public void removeHandler(PatchHandler ph) { handlers.remove(ph) ; }
    
    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        LOG.info("Patch");
        InputStream in = req.getInputStream() ;
        PatchReader scr = new PatchReader(in) ;
        // Collect (scale!), log. 
        RDFChangesCollector changes = new RDFChangesCollector() ;
        // Patch set
        scr.apply(changes);
        PatchSet ps = null ;
        for ( PatchHandler patchHandler : handlers ) {
            LOG.info("Handler: "+Lib.className(patchHandler)) ;
            RDFChanges sc = patchHandler.handler() ;
            sc.start();
            changes.play(sc);
            sc.finish();
            //scr.apply(sc);
        }
        
        //resp.setContentLength(0);
      resp.setStatus(HttpSC.NO_CONTENT_204) ;
    }
}
