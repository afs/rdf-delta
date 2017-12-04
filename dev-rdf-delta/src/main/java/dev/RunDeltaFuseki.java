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
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.web.HttpOp;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.DatasetGraphFactory;
import org.slf4j.Logger ;
import org.slf4j.LoggerFactory ;

public class RunDeltaFuseki {
    static { 
        LogCtl.setCmdLogging();
        LogCtl.setJavaLogging();
    }
    
    static Logger LOG = LoggerFactory.getLogger("Main") ;
    
    public static void main(String[] args) throws Exception {
        DatasetGraph dsg = DatasetGraphFactory.createTxnMem();
//      Operation patchOp = Operation.register("Patch"); 
//      ActionService handler = new PatchReceiverService();
//      String EP = "patch";
//      String DATASET = "/ds";
//      
      FusekiServer server = 
          FusekiServer.create()
              .setPort(2022)
//              .registerOperation(patchOp, handler)
//              .add("ds", dsg, true)
//              .addOperation(DATASET, EP, patchOp)
              .build();

      try { 
          server.start();
//          System.out.println();
//          try(RDFConnection rconn = RDFConnectionFactory.connect("http://localhost:2022/ds")) {
//              try(QueryExecution qExec = rconn.query("SELECT ?x {}")) {
//                  //ResultSet rs = qExec.execSelect();
//                  QueryExecUtils.executeQuery(qExec);
//              }
//          }
          
          BasicHttpEntity entity = new BasicHttpEntity();
          entity.setContent(IO.openFile("data1.rdfp"));
          HttpOp.execHttpPost("http://localhost:2022/ds2/patch", entity);
         
          RDFDataMgr.write(System.out, dsg, Lang.TRIG);
          
          
      } finally {
          System.out.println();
          server.stop();
      }
  }
}
