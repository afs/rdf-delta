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
import org.apache.jena.atlas.logging.LogCtl;
import org.apache.jena.fuseki.embedded.FusekiServer;
import org.apache.jena.rdfconnection.RDFConnection;
import org.apache.jena.rdfconnection.RDFConnectionFactory;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.DatasetGraphFactory;
import org.apache.jena.update.UpdateFactory;
import org.apache.jena.update.UpdateRequest;
import org.seaborne.patch.RDFChanges;
import org.seaborne.patch.RDFPatchOps;

public class DeltaExFuseki1_LogChanges {
    static { LogCtl.setJavaLogging(); }
    
    public static void main(String ...args) {
        int PORT = 2020;
        DatasetGraph dsgBase = DatasetGraphFactory.createTxnMem();
        RDFChanges changeLog = RDFPatchOps.textWriter(System.out);
        DatasetGraph dsg = RDFPatchOps.changes(dsgBase, changeLog);
        
        // Create a server with the changes-enables dataset.
        // Plain server. No other registration necessary.
        FusekiServer server = 
            FusekiServer.create()
                .setPort(PORT)
                .add("/ds", dsg)
                .build();
        server.start();

        RDFConnection conn = RDFConnectionFactory.connect("http://localhost:"+PORT+"/ds");
        UpdateRequest update = UpdateFactory.create("PREFIX : <http://example/> INSERT DATA { :s :p 123 }");
        // Note - no prefix in changes. The SPARQL Update prefix is not a chnage to the dataset prefixes.
        conn.update(update);
        
        server.stop(); 
//        // Server in the background so explicitly exit.
//        System.exit(0);
    }
    
}
