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

package org.seaborne.delta.fuseki;

import java.io.IOException;
import java.io.InputStream;

import org.apache.jena.fuseki.server.CounterName;
import org.apache.jena.fuseki.servlets.ActionREST;
import org.apache.jena.fuseki.servlets.HttpAction;
import org.apache.jena.fuseki.servlets.ServletOps;
import org.apache.jena.riot.RiotException;
import org.apache.jena.riot.web.HttpNames;
import org.apache.jena.sparql.core.DatasetGraph;
import org.seaborne.patch.RDFPatchOps;

/** A Fuseki service to receive and apply a patch. */
public class PatchReceiverService extends ActionREST {
    static CounterName counterPatchesGood = CounterName.register("","");
    static CounterName counterPatchesBad = CounterName.register("","");
    
    // It's an ActionRest because it accepts POST/PATCH with a content body.  
    
    public PatchReceiverService() {
        // Counters: the standard ActionREST counters per operation are enough.
    }
    
    @Override
    protected void validate(HttpAction action) {
        
        // Do everything in {@link operation} 
        //action.getEndpoint().getCounters();
        //String ct = action.getRequest().getContentType();
    }
    
    private void operation(HttpAction action) {    
        action.beginWrite();
        try { 
            applyRDFPatch(action) ;
            action.commit();
        } catch (Exception ex) {
            action.abort();
            throw ex;
        } finally { action.endWrite(); }
    }

    private void applyRDFPatch(HttpAction action) {
        try {
            String ct = action.getRequest().getContentType();
            // If triples or quads, maybe POST. 
            
            InputStream input = action.request.getInputStream();
            DatasetGraph dsg = action.getDataset();
            RDFPatchOps.applyChange(dsg, input);
        }
        catch (RiotException ex) {
            ServletOps.errorBadRequest("RDF Patch parse error: "+ex.getMessage());
        }
        catch (IOException ex) {
            ServletOps.errorBadRequest("IOException: "+ex.getMessage());
        }
    }

    @Override
    protected void doPost(HttpAction action) {
        operation(action);
    }

    @Override
    protected void doPatch(HttpAction action) {
        operation(action);
    }

    @Override
    protected void doOptions(HttpAction action) {
        setCommonHeadersForOptions(action.response) ;
        action.response.setHeader(HttpNames.hAllow, "OPTIONS,POST,PATCH");
        action.response.setHeader(HttpNames.hContentLengh, "0") ;
    }

    @Override
    protected void doHead(HttpAction action) {
        ServletOps.errorMethodNotAllowed("HEAD") ;
    }

    @Override
    protected void doPut(HttpAction action) {
        ServletOps.errorMethodNotAllowed("PUT") ;
    }

    @Override
    protected void doDelete(HttpAction action) {
        ServletOps.errorMethodNotAllowed("DELETE") ;
    }

    @Override
    protected void doGet(HttpAction action) {
        ServletOps.errorMethodNotAllowed("GET") ;
    }
}
