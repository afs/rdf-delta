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

package org.seaborne.delta.server.http.receiver;

import java.io.IOException;
import java.io.InputStream;

import org.apache.jena.fuseki.servlets.ActionREST;
import org.apache.jena.fuseki.servlets.HttpAction;
import org.apache.jena.fuseki.servlets.ServletOps;
import org.apache.jena.riot.RiotException;
import org.apache.jena.sparql.core.DatasetGraph;
import org.seaborne.patch.RDFPatchOps;

/** Receive a patch and apply it to a datasets */
public class PatchReceiverService extends ActionREST {
    
    // ServletBase -> ValidatorBase (etc), ValidatorBasJson 
    //   ActionBase
    //      ActionCtl -> ...
    //      ActionTasks
    
    //      ActionSPARQL <- dataset mapping.
    //        ActionREST
    //          REST_Quads
    //          SPARQL_GSP
    //            SPARQL_GSP_R
    //            SPARQL_GSP_RW
    
    //      ActionSPARQL : SPARQL_UberServlet
    
    
    // Naming of Action* not ideal.  No longer "SPARQL", more "Service on Dataset"
    //?? extends ActionBase -> rename to ActionService 
    //?? ActionREST - ActionHTTP?  
    

    @Override
    protected void validate(HttpAction action) {
        //action.exec(()->{});
        action.beginWrite();
        try { 
            applyRDFPatch(action) ;
            action.commit();
            //incCounter(action.getEndpoint().getCounters(), HTTPpatch) ;
        } catch (Exception ex) {
            //incCounter(action.getEndpoint().getCounters(), PatchErrors) ;
            action.abort();
            throw ex;
        } finally { action.endWrite(); }
    }
    
    private void applyRDFPatch(HttpAction action) {
        try {
            InputStream input = action.request.getInputStream();
            DatasetGraph dsg = action.getDataset();
            RDFPatchOps.applyChange(dsg, input);
        }
        catch (RiotException e) {
            ServletOps.errorBadRequest("RDF Patch parse error: "+e.getMessage());
        }
        catch (IOException e) {
            ServletOps.errorBadRequest(METHOD_DELETE);
        }
    }

    @Override
    protected void doOptions(HttpAction action) {
        ServletOps.errorMethodNotAllowed("OPTIONS") ;
    }

    @Override
    protected void doHead(HttpAction action) {
        ServletOps.errorMethodNotAllowed("HEAD") ;
    }

    @Override
    protected void doPost(HttpAction action) {
        ServletOps.errorMethodNotAllowed("POST") ;
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
    protected void doPatch(HttpAction action) {
        ServletOps.errorMethodNotAllowed("PATCH") ;
    }

    @Override
    protected void doGet(HttpAction action) {
        ServletOps.errorMethodNotAllowed("GET") ;
    }
}
