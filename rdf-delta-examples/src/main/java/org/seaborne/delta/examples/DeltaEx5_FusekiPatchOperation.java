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

/**
 * Create Fuseki server with a "patch" service,inaddition to the usual "sparql",
 * "update" etc services.
 * <p>
 * A patch file {@code POST}ed, or {@code PATCH}ed, to the url
 * {@code http://host:port/dataset/patch} wil be applied as a single
 * transaction.
 */
// **** 0.3.0


public class DeltaEx5_FusekiPatchOperation {
}

//    static { LogCtl.setJavaLogging(); }
//    
//    public static void main(String ...args) {
//        try { main2(args) ; }
//        finally { System.exit(0); }
//    }
//        
//    public static void main2(String ...args) {
//        int PORT = 2020 ;
//        // In-memory dataset
//        DatasetGraph dsgBase = DatasetGraphFactory.createTxnMem();
//        RDFChanges changeLog = RDFPatchOps.textWriter(System.out);
//        DatasetGraph dsg = RDFPatchOps.changes(dsgBase, changeLog);
//        
//        FusekiServer server = 
//            DeltaFuseki.fusekiWithPatch()
//                .setPort(PORT)
//                .add("/ds", dsg)
//                .addOperation("/ds", "patch", DeltaFuseki.patchOp)
//                .build();
//        
//        // Long hand version of the same:
//        if ( false ) {
//            Operation patchOp = Operation.register("Patch", "Patch Service"); 
//            String patchContentType = "application/rdf-patch";
//            ActionService handler = new PatchApplyService();
//            FusekiServer serverAlt =  FusekiServer.create()
//                .registerOperation(patchOp, patchContentType, handler)
//                .setPort(PORT)
//                .add("/ds", dsg)
//                .addOperation("/ds", "patch", DeltaFuseki.patchOp)
//                .build();
//        }
//        
//        server.start();
//        
//        String patch = StrUtils.strjoinNL
//            ("H id <uuid:"+UUID.randomUUID()+"> ."
//            ,"TB ."
//            ,"A <http://example/s> <http://example/p> '"+DateTimeUtils.nowAsString()+"' ."
//            ,"TC ."
//            );
//        
//        // -- Create patch operation.
//        // (without library calls)
//        BasicHttpEntity entity = new BasicHttpEntity();
//        // See also "ByteArrayEntity"
//        entity.setContentType(DeltaFuseki.patchContentType);
//        // From file ..
//        //entity.setContent(IO.openFile("data1.rdfp"));
//        entity.setContent(new ByteArrayInputStream(StrUtils.asUTF8bytes(patch)));
//        
//        // -- Send to the server.
//        HttpOp.execHttpPost("http://localhost:"+PORT+"/ds/patch", entity);
//        
//        // -- See if it is there.
//        RDFConnection conn = RDFConnectionFactory.connect("http://localhost:"+PORT+"/ds");
//        Query query = QueryFactory.create("SELECT * { ?s ?p ?o}");
//        QueryExecution qExec = conn.query(query);
//        ResultSetFormatter.out(qExec.execSelect());
//        
//        server.stop(); 
//        System.exit(0);
//    }
//    
//}
