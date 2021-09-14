/*
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 *  See the NOTICE file distributed with this work for additional
 *  information regarding copyright ownership.
 */

package org.seaborne.delta.server.local.handlers;

import java.net.http.HttpRequest.BodyPublishers;
import java.util.ArrayList ;
import java.util.List ;

import org.apache.jena.atlas.io.IndentedLineBuffer ;
import org.apache.jena.atlas.web.HttpException ;
import org.apache.jena.http.HttpOp;
import org.apache.jena.query.ARQ ;
import org.apache.jena.riot.WebContent ;
import org.seaborne.delta.server.local.DPS;
import org.seaborne.delta.server.local.Patch;
import org.seaborne.delta.server.local.PatchHandler;
import org.seaborne.patch.RDFChanges ;
import org.seaborne.patch.changes.RDFChangesWriteUpdate ;

/** Convert a patch to SPARQL Update and send to some endpoints */
public class PHandlerSPARQLUpdate implements PatchHandler {

    // SPARQL Update services to poke
    private List<String> updateEndpoints = new ArrayList<>() ;
    public PHandlerSPARQLUpdate addEndpoint(String url) {
        updateEndpoints.add(url) ;
        return this ;
    }

    public PHandlerSPARQLUpdate() { }

    private Object dft = ARQ.getContext().get(ARQ.constantBNodeLabels) ;

    @Override
    public void handle(Patch patch) {
        IndentedLineBuffer x = new IndentedLineBuffer() ;
        RDFChanges scData = new RDFChangesWriteUpdate(x) ;
        patch.play(scData);
        x.flush();
        String reqStr = x.asString() ;
        updateEndpoints.forEach((ep)->{
            try { HttpOp.httpPost(ep, WebContent.contentTypeSPARQLUpdate, BodyPublishers.ofString(reqStr)) ; }
            catch (HttpException ex) { DPS.LOG.warn("Failed to send to "+ep) ; }
        }) ;
    }
}

