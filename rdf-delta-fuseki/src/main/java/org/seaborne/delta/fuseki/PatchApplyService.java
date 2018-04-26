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

import static java.lang.String.format;
import static org.seaborne.delta.DeltaConst.contentTypePatchBinary;
import static org.seaborne.delta.DeltaConst.ctPatchBinary;
import static org.seaborne.delta.DeltaConst.ctPatchText;

import java.io.IOException;
import java.io.InputStream;

import org.apache.jena.atlas.web.ContentType;
import org.apache.jena.fuseki.server.CounterName;
import org.apache.jena.fuseki.servlets.ActionErrorException ;
import org.apache.jena.fuseki.servlets.ActionREST;
import org.apache.jena.fuseki.servlets.HttpAction;
import org.apache.jena.fuseki.servlets.ServletOps;
import org.apache.jena.riot.RiotException;
import org.apache.jena.riot.WebContent;
import org.apache.jena.riot.web.HttpNames;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.web.HttpSC;
import org.seaborne.patch.RDFChanges;
import org.seaborne.patch.changes.RDFChangesApply;
import org.seaborne.patch.changes.RDFChangesWrapper;
import org.seaborne.patch.text.RDFPatchReaderText ;

/** A Fuseki service to receive and apply a patch. */
public class PatchApplyService extends ActionREST {
    static CounterName counterPatches = CounterName.register("RDFpatch","rdf-patch.requests");
    static CounterName counterPatchesGood = CounterName.register("RDFpatch","rdf-patch.good");
    static CounterName counterPatchesBad = CounterName.register("RDFpatchBad","rdf-patch.bad");
    
    // It's an ActionREST because it accepts POST/PATCH with a content body.  
    
    public PatchApplyService() {
        // Counters: the standard ActionREST counters per operation are enough.
    }
    
    @Override
    protected void validate(HttpAction action) {
        String method = action.getRequest().getMethod();
        switch(method) {
            case HttpNames.METHOD_POST:
            case HttpNames.METHOD_PATCH:
                break;
            default:
                ServletOps.errorMethodNotAllowed(method+" : Patch must use POST or PATCH");
        }
        String ctStr = action.request.getContentType();
        // Must be UTF-8 or unset. But this is wrong so often,
        // it is less trouble to just force UTF-8.
        String charset = action.request.getCharacterEncoding();
        if ( charset != null && ! WebContent.charsetUTF8.equals(charset) )
            ServletOps.error(HttpSC.UNSUPPORTED_MEDIA_TYPE_415, "Charset must be omitted or UTF-8, not "+charset); 

        // If no header Content-type - assume patch-text.
        ContentType contentType = ( ctStr != null ) ? ContentType.create(ctStr) : ctPatchText;
        if ( ! ctPatchText.equals(contentType) && ! ctPatchBinary.equals(contentType) ) 
            ServletOps.error(HttpSC.UNSUPPORTED_MEDIA_TYPE_415, "Allowed Content-types are "+ctPatchText+" or "+ctPatchBinary+", not "+ctStr); 
        if ( ctPatchBinary.equals(contentType) )
            ServletOps.error(HttpSC.UNSUPPORTED_MEDIA_TYPE_415, contentTypePatchBinary+" not supported yet");
    }
    
    protected void operation(HttpAction action) {
        incCounter(action.getEndpoint(), counterPatches);
        try {
            operation$(action);
            incCounter(action.getEndpoint(), counterPatchesGood) ;
        } catch ( ActionErrorException ex ) {
            incCounter(action.getEndpoint(), counterPatchesBad) ;
            throw ex ;
        }
    }
    
    private void operation$(HttpAction action) {
        action.log.info(format("[%d] RDF Patch", action.id));
        action.beginWrite();
        // Add patch handler to suppress TX-TC in the patch but allow TA. 
        try { 
            applyRDFPatch(action);
            action.commit();
        } catch (Exception ex) {
            action.abort();
            throw ex;
        } finally { action.endWrite(); }
        // Response?
    }

    private void applyRDFPatch(HttpAction action) {
        try {
            String ct = action.getRequest().getContentType();
            // If triples or quads, maybe POST. 
            
            InputStream input = action.request.getInputStream();
            DatasetGraph dsg = action.getDataset();
            
            RDFPatchReaderText pr = new RDFPatchReaderText(input);
            RDFChanges changes = new RDFChangesApply(dsg);
            // External transaction. Suppress patch recorded TX and TC.
            changes = new RDFChangesNoTxn(changes);
            
            pr.apply(changes);
            ServletOps.success(action);
        }
        catch (RiotException ex) {
            ServletOps.errorBadRequest("RDF Patch parse error: "+ex.getMessage());
        }
        catch (IOException ex) {
            ServletOps.errorBadRequest("IOException: "+ex.getMessage());
        }
    }
    
    // Counting?
    static class RDFChangesNoTxn extends RDFChangesWrapper {
        public RDFChangesNoTxn(RDFChanges other) {
            super(other);
        }
        // Ignore so external control can be applied - but allow abort.
        // Combine so multi-txn works AND   
        
        @Override
        public void txnBegin() {}
        
        @Override
        public void txnCommit() {}
        
        @Override
        public void segment() {}
    }

    // ---- POST or PATCH or OPTIONS
    
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
        setCommonHeadersForOptions(action.response);
        action.response.setHeader(HttpNames.hAllow, "OPTIONS,POST,PATCH");
        action.response.setHeader(HttpNames.hContentLengh, "0");
    }

    @Override
    protected void doHead(HttpAction action) { ServletOps.errorMethodNotAllowed("HEAD"); }

    @Override
    protected void doPut(HttpAction action) { ServletOps.errorMethodNotAllowed("PUT"); }

    @Override
    protected void doDelete(HttpAction action) { ServletOps.errorMethodNotAllowed("DELETE"); }

    @Override
    protected void doGet(HttpAction action) { ServletOps.errorMethodNotAllowed("GET"); }
}
