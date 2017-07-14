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

import static java.lang.String.format ;
import static org.seaborne.delta.DeltaConst.*;

import java.io.IOException ;
import java.io.InputStream ;

import org.apache.jena.atlas.web.ContentType;
import org.apache.jena.fuseki.servlets.ActionSPARQL ;
import org.apache.jena.fuseki.servlets.HttpAction ;
import org.apache.jena.fuseki.servlets.ServletOps ;
import org.apache.jena.riot.web.HttpNames ;
import org.apache.jena.sparql.core.DatasetGraph ;
import org.apache.jena.web.HttpSC ;
import org.seaborne.patch.RDFPatchOps ;

public class FusekiPatch extends ActionSPARQL {
    
    // ActionREST
//    @Override
//    protected void doOptions(HttpAction action) {
//        setCommonHeadersForOptions(action.response) ;
//        action.response.setHeader(HttpNames.hAllow, "OPTIONS,POST,PATCH");
//        action.response.setHeader(HttpNames.hContentLengh, "0") ;
//        ServletOps.success(action) ;
//    }
//
//    @Override
//    protected void doHead(HttpAction action) {
//        ServletOps.errorMethodNotAllowed("HEAD") ;
//    }
//
//    @Override
//    protected void doPost(HttpAction action) {
//        perform(action);
//    }
//
//    @Override
//    protected void doPut(HttpAction action) {
//        ServletOps.errorMethodNotAllowed("PUT") ;
//    }
//
//    @Override
//    protected void doDelete(HttpAction action) {
//        ServletOps.errorMethodNotAllowed("DELETE") ;
//    }
//
//    @Override
//    protected void doPatch(HttpAction action) {
//        perform(action);
//    }
//
//    @Override
//    protected void doGet(HttpAction action) {
//        ServletOps.errorMethodNotAllowed("GET") ;
//    }

    @Override
    protected void validate(HttpAction action) {
        String method = action.getRequest().getMethod() ;
        switch(method) {
            case HttpNames.METHOD_POST:
            case HttpNames.METHOD_PATCH:
                break ;
            default:
                ServletOps.errorMethodNotAllowed(method+" : Patch must use POST or PATCH");
        }
    }
    
    @Override
    protected void perform(HttpAction action) {
        action.log.info(format("[%d] Patch", action.id));
        try {
            String ctStr = action.request.getContentType() ;
            // Must be UTF-8 or unset. But this is wrong so often,
            // it is less trouble to just force UTF-8.
            String charset = action.request.getCharacterEncoding() ;

            ContentType contentType = ( ctStr != null ) 
                // Parse it.
                ? ContentType.create(ctStr, charset)
                // No header Content-type - assume patch-text.
                : ctPatchText;
            if ( ! ctPatchText.equals(contentType) && ! ctPatchBinary.equals(contentType) ) 
                ServletOps.error(HttpSC.UNSUPPORTED_MEDIA_TYPE_415, "Allowed Content-types are "+ctPatchText+" or "+ctPatchBinary+", not "+ctStr); 
            if ( ctPatchBinary.equals(contentType) )
                ServletOps.error(HttpSC.UNSUPPORTED_MEDIA_TYPE_415, contentTypePatchBinary+" not supported yet");
            
            DatasetGraph dsg = action.getActiveDSG(); 
            action.beginWrite();
            try {
                InputStream input = action.request.getInputStream();
                if ( ctPatchBinary.equals(contentType) )
                    ServletOps.error(HttpSC.UNSUPPORTED_MEDIA_TYPE_415, contentTypePatchBinary+" not supported yet");
                if ( ctPatchText.equals(contentType) )
                    RDFPatchOps.applyChange(dsg, input);
                action.commit();
            //} catch (Throwable th) {}
            } finally { action.endWrite(); }
        } catch (IOException ex) {
            ServletOps.errorOccurred("IOException: "+ex.getMessage());
        }
    }

}
