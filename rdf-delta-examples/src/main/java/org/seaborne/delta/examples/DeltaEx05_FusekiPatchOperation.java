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

package org.seaborne.delta.examples;

import java.net.http.HttpRequest.BodyPublishers;
import java.util.UUID ;

import org.apache.jena.atlas.lib.DateTimeUtils ;
import org.apache.jena.atlas.lib.StrUtils ;
import org.apache.jena.fuseki.main.FusekiServer;
import org.apache.jena.fuseki.server.Operation ;
import org.apache.jena.fuseki.servlets.ActionService ;
import org.apache.jena.http.HttpOp;
import org.apache.jena.query.Query ;
import org.apache.jena.query.QueryExecution ;
import org.apache.jena.query.QueryFactory ;
import org.apache.jena.query.ResultSetFormatter ;
import org.apache.jena.rdfconnection.RDFConnection ;
import org.apache.jena.sparql.core.DatasetGraph ;
import org.apache.jena.sparql.core.DatasetGraphFactory ;
import org.seaborne.delta.Delta;
import org.seaborne.delta.fuseki.DeltaFuseki ;
import org.seaborne.delta.fuseki.PatchApplyService ;
import org.seaborne.delta.lib.LogX;
import org.apache.jena.rdfpatch.RDFPatchOps ;
import org.apache.jena.rdfpatch.text.RDFChangesWriterText;

/**
 * Create Fuseki server with a "patch" service, in addition to the usual "sparql",
 * "update" etc services.
 * <p>
 * A patch file {@code POST}ed, or {@code PATCH}ed, to the url
 * {@code http://host:port/dataset/patch} will be applied as a single
 * transaction.
 */
public class DeltaEx05_FusekiPatchOperation {
    static { LogX.setJavaLogging(); }

    public static void main(String ...args) {
        try { main2(args) ; }
        finally { System.exit(0); }
    }

    public static void main2(String ...args) {
        int PORT = 2020 ;
        // In-memory dataset
        DatasetGraph dsgBase = DatasetGraphFactory.createTxnMem();
        try ( RDFChangesWriterText changeLog = RDFPatchOps.textWriter(System.out) ) {
            DatasetGraph dsg = RDFPatchOps.changes(dsgBase, changeLog);

            FusekiServer server =
                DeltaFuseki.fusekiWithPatchApply()
                    .port(PORT)
                    .add("/ds", dsg)
                    .addOperation("/ds", DeltaFuseki.patchOp)
                    .build();

            // Long hand version of the same:
            if ( false ) {
                Operation patchOp = Operation.alloc(Delta.namespace+"patch", "rdf-Patch", "RDF Patch Service");
                String patchContentType = "application/rdf-patch";
                ActionService handler = new PatchApplyService();
                FusekiServer serverAlt =  FusekiServer.create()
                    .registerOperation(patchOp, patchContentType, handler)
                    .port(PORT)
                    .add("/ds", dsg)
                    .addOperation("/ds", DeltaFuseki.patchOp)
                    .build();
            }

            server.start();

            String patch = StrUtils.strjoinNL
                ("H id <uuid:"+UUID.randomUUID()+"> ."
                ,"TB ."
                ,"A <http://example/s> <http://example/p> '"+DateTimeUtils.nowAsString()+"' ."
                ,"TC ."
                );

            // -- Send to the server.
            HttpOp.httpPost("http://localhost:"+PORT+"/ds/patch", DeltaFuseki.patchContentType, BodyPublishers.ofString(patch));

            // -- See if it is there.
            RDFConnection conn = RDFConnection.connect("http://localhost:"+PORT+"/ds");
            Query query = QueryFactory.create("SELECT * { ?s ?p ?o}");
            QueryExecution qExec = conn.query(query);
            ResultSetFormatter.out(qExec.execSelect());
            server.stop();
        }
        System.exit(0);
    }
}
