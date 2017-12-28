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

package org.seaborne.delta.examples;

import org.apache.jena.atlas.logging.LogCtl ;
import org.apache.jena.fuseki.embedded.FusekiServer ;
import org.apache.jena.fuseki.server.Operation ;
import org.apache.jena.fuseki.servlets.ActionService ;
import org.apache.jena.query.QueryExecution ;
import org.apache.jena.query.ResultSet ;
import org.apache.jena.query.ResultSetFormatter ;
import org.apache.jena.rdfconnection.RDFConnection ;
import org.apache.jena.rdfconnection.RDFConnectionFactory ;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.DatasetGraphFactory;
import org.apache.jena.system.Txn;
import org.seaborne.delta.client.RDFChangesHTTP ;
import org.seaborne.delta.fuseki.DeltaFuseki ;
import org.seaborne.delta.fuseki.PatchApplyService ;
import org.seaborne.patch.RDFChanges;
import org.seaborne.patch.RDFPatchOps;

/**
 * Set up a dataset and write changes to Fuseki using the patch operation. When
 * the two datasets, local and remote, start from the same state (e.g. are file
 * system copies of the same TDB database; blank nodes internal identifers must
 * align) the local dataset can be manipulated by API and the Fuseki-published
 * one will be identical and up to date.
 * <p>
 * Alternatively, not shown here, changes can be collected up into a series of
 * patch then sent together to the Fuseki server.
 * <p>
 * See {@link DeltaEx5_FusekiPatchOperation}.
 */
public class DeltaEx6_LocalDatasetToFuseki {
    static { LogCtl.setJavaLogging(); }
    
    public static void main(String ...args) {
        try { main2(args) ; }
        finally { System.exit(0); }
    }
        
    public static void main2(String ...args) {
        // ---- Fuseki server with patch operation.
        int PORT = 2020 ;
        // In-memory dataset
        DatasetGraph dsgFuseki = DatasetGraphFactory.createTxnMem();
        
        String serviceName = "patch";
        FusekiServer server = fusekiServerWithPatch("/ds", PORT, dsgFuseki, serviceName);
        server.start();
        
        // ---- Destination for changes is the Fuseki patch opration.
        String url = "http://localhost:"+PORT+"/ds/"+serviceName;
        RDFChanges changeSender = new RDFChangesHTTP(url, url);

        // ---- Local dataset.
        DatasetGraph dsgLocal = DatasetGraphFactory.createTxnMem();
        // Combined datasetgraph and changes. 
        DatasetGraph dsg = RDFPatchOps.changes(dsgLocal, changeSender);

        // ---- Do something. Read in data.ttl inside a transaction.
        // (data.ttl is in src/main/resources/)
        Txn.executeWrite(dsg,
            ()->RDFDataMgr.read(dsg, "data.ttl")
            );
        
        // ---- Query Fuseki
        RDFConnection conn = RDFConnectionFactory.connect("http://localhost:"+PORT+"/ds");
        try( QueryExecution qExec = conn.query("PREFIX ex: <http://example.org/> SELECT * { ?s ?p ?o }") ) {
            ResultSet rs = qExec.execSelect();
            ResultSetFormatter.out(rs, qExec.getQuery());
        }
    }

    private static FusekiServer fusekiServerWithPatch(String dsName, int port, DatasetGraph dsgFuseki, String serviceName) {
        Operation patchOp = Operation.register("Patch", "Patch Service"); 
        String patchContentType = "application/rdf-patch";
        ActionService handler = new PatchApplyService();
        FusekiServer server =  FusekiServer.create()
            .registerOperation(patchOp, patchContentType, handler)
            .setPort(port)
            .add(dsName, dsgFuseki)
            .addOperation(dsName, serviceName, DeltaFuseki.patchOp)
            .build();
        return server ;
    }    
}
