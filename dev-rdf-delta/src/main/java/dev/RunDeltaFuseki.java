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

import org.apache.http.entity.BasicHttpEntity;
import org.apache.jena.atlas.io.IO;
import org.apache.jena.atlas.logging.LogCtl;
import org.apache.jena.fuseki.embedded.FusekiServer ;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.rdfconnection.RDFConnection;
import org.apache.jena.rdfconnection.RDFConnectionFactory;
import org.apache.jena.riot.web.HttpOp;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.DatasetGraphFactory;
import org.apache.jena.sparql.util.QueryExecUtils;
import org.slf4j.Logger ;
import org.slf4j.LoggerFactory ;

public class RunDeltaFuseki {
    static { 
        LogCtl.setCmdLogging();
        LogCtl.setJavaLogging();
    }
    
    static Logger LOG = LoggerFactory.getLogger("Main") ;

    public static void main(String[] args) throws Exception {
        String MIMETYPE = "application/rdf-patch";

        DatasetGraph dsg = DatasetGraphFactory.createTxnMem();
        // **** 0.3.0
//        Operation patchOp = Operation.register("Patch", "Patch Service"); 
//        ActionService handler = new PatchApplyService();
        String EP = "patch";
        String DATASET = "ds";

        FusekiServer server = 
            FusekiServer.create()
            .setPort(2022)
            // **** 0.3.0
//            .registerOperation(patchOp, MIMETYPE, handler)
//            .add(DATASET, dsg)
//            .addOperation(DATASET, EP, patchOp)
            .build();

        try { 
            server.start();

            BasicHttpEntity entity = new BasicHttpEntity();
            entity.setContent(IO.openFile("data1.rdfp"));
            HttpOp.execHttpPost("http://localhost:2022/ds/patch", entity);

            System.out.println();
            try(RDFConnection rconn = RDFConnectionFactory.connect("http://localhost:2022/ds")) {
                try(QueryExecution qExec = rconn.query("SELECT * {?s ?p ?o}")) {
                    //ResultSet rs = qExec.execSelect();
                    QueryExecUtils.executeQuery(qExec);
                }
            }

//            RDFDataMgr.write(System.out, dsg, Lang.TRIG);
//
//            String x = HttpOp.execHttpGetString("http://localhost:2022/ds/data", "text/turtle");
//            System.out.println(x);

        } finally {
            System.out.println();
            server.stop();
        }
    }
}
