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

import java.util.List;

import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.DatasetGraphFactory;
import org.apache.jena.system.Txn;
import org.seaborne.delta.DataSourceDescription;
import org.seaborne.delta.Id;
import org.seaborne.delta.client.DeltaConnection;
import org.seaborne.delta.client.DeltaLinkHTTP;
import org.seaborne.delta.link.DeltaLink;

public class RunDeltaServer {

    public static void main(String... args) {
        try {
            main$(args);
        } finally {
            System.out.println("** DONE **");
            System.exit(0);
        }
    }
    
    
    public static void main$(String... args) {
        if ( args.length == 0 )
            args = new String[] {"--base=DeltaServer"};
        
        org.seaborne.delta.server.http.CmdDeltaServer.main(args);
        System.out.println();

        
        
        String datafile = "D.ttl"; 
        String url = "http://localhost:1066/" ;
        
        // Building.
        //MethodHandles.
        Id clientId = Id.create();
        DeltaLink link = new DeltaLinkHTTP(url);
        
        //RegToken token = link.register(clientId);
        
        // Find Dataset
        // TestDeltaServer in rdf-delta-server-http
        //   AbstractTestDeltaLink in rdf-delta-test
        //   ** TestRemoteLink

        List<Id> a = link.listDatasets();
        for(Id id : a ) {
            DataSourceDescription dss = link.getDataSourceDescription(id);
            System.out.println(dss);
        }

        DatasetGraph dsg0 = DatasetGraphFactory.createTxnMem(); 
        
        Id datasourceId = Id.fromString("id:0c5943d8-2b54-11b2-801b-024218167bb0");
        
        DeltaConnection dConn = DeltaConnection.connect("D1", clientId, datasourceId, dsg0, link);
        
        DatasetGraph dsg = dConn.getDatasetGraph();
        Txn.executeRead(dsg,()->RDFDataMgr.write(System.out, dsg, Lang.TRIG));
        //Txn.executeWrite(dsg,()->RDFDataMgr.read(dsg, datafile));
        link.deregister();
        
        // Error cases
        
        System.out.println("** DONE **");
        System.exit(0);
        
        
    }

}
