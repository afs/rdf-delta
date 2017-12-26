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
import java.io.ByteArrayInputStream;
import java.util.UUID;

import org.apache.http.entity.BasicHttpEntity;
import org.apache.jena.atlas.lib.DateTimeUtils;
import org.apache.jena.atlas.lib.StrUtils;
import org.apache.jena.atlas.logging.LogCtl;
import org.apache.jena.fuseki.embedded.FusekiServer;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.ResultSetFormatter;
import org.apache.jena.rdfconnection.RDFConnection;
import org.apache.jena.rdfconnection.RDFConnectionFactory;
import org.apache.jena.riot.web.HttpOp;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.DatasetGraphFactory;
import org.seaborne.delta.fuseki.DeltaFuseki;
import org.seaborne.patch.RDFChanges;
import org.seaborne.patch.RDFPatchOps;

public class DeltaExFuseki3_PatchOperation {
    static { LogCtl.setJavaLogging(); }
    
    public static void main(String ...args) {
        int PORT = 2020 ;
        // In-memory dataset
        DatasetGraph dsgBase = DatasetGraphFactory.createTxnMem();
        RDFChanges changeLog = RDFPatchOps.textWriter(System.out);
        DatasetGraph dsg = RDFPatchOps.changes(dsgBase, changeLog);
        
        FusekiServer server = 
            DeltaFuseki.fusekiWithPatch()
                .setPort(PORT)
                .add("/ds", dsg)
                .addOperation("/ds", "patch", DeltaFuseki.patchOp)
                .build();
        server.start();
        
        String patch = StrUtils.strjoinNL
            ("H id <uuid:"+UUID.randomUUID()+"> ."
            ,"TB ."
            ,"A <http://example/s> <http://example/p> '"+DateTimeUtils.nowAsString()+"' ."
            ,"TC ."
            );
        
        // -- Create patch operation.
        // (without library calls)
        BasicHttpEntity entity = new BasicHttpEntity();
        // See also "ByteArrayEntity"
        entity.setContentType(DeltaFuseki.patchContentType);
        // From file ..
        //entity.setContent(IO.openFile("data1.rdfp"));
        entity.setContent(new ByteArrayInputStream(StrUtils.asUTF8bytes(patch)));
        
        // -- Send to the server.
        HttpOp.execHttpPost("http://localhost:"+PORT+"/ds/patch", entity);
        
        // -- See if it is there.
        RDFConnection conn = RDFConnectionFactory.connect("http://localhost:"+PORT+"/ds");
        Query query = QueryFactory.create("SELECT * { ?s ?p ?o}");
        QueryExecution qExec = conn.query(query);
        ResultSetFormatter.out(qExec.execSelect());
        
        server.stop(); 
        System.exit(0);
    }
    
}
