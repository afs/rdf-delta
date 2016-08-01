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

import java.util.stream.IntStream ;

import org.apache.jena.atlas.logging.LogCtl ;
import org.apache.jena.sparql.core.DatasetGraph ;
import org.apache.jena.sparql.core.Quad ;
import org.apache.jena.sparql.sse.SSE ;
import org.apache.jena.system.Txn ;
import org.apache.jena.tdb.TDBFactory ;
import org.seaborne.delta.DP ;
import org.seaborne.delta.base.PatchReader ;
import org.seaborne.delta.client.DeltaClient ;
import org.seaborne.delta.server.DataPatchServer ;
import org.seaborne.patch.RDFChangesWriter ;

public class RunDelta {
    static { LogCtl.setJavaLogging(); }
    
    static String url = "http://localhost:"+DP.PORT+"/rpc" ; 
    
    public static void main(String... args) {
        try {
            main$();
        } catch (Throwable ex) {
            ex.printStackTrace(System.err) ;
        }
        finally { System.exit(0) ; }
    }
    
    
    public static void main$(String... args) {
        DataPatchServer server = new DataPatchServer(DP.PORT) ;
        server.start();
        
        DatasetGraph dsgBase1 = TDBFactory.createDatasetGraph() ;
        DeltaClient client1 = DeltaClient.create("http://localhost:"+DP.PORT+"/", dsgBase1) ;
        {
            int x = client1.getRemoteVersion() ;
            System.out.println("epoch = "+x) ;
        }
        
        
        DatasetGraph dsgBase2 = TDBFactory.createDatasetGraph() ;
        DeltaClient client2 = DeltaClient.create("http://localhost:"+DP.PORT+"/", dsgBase2) ;

        update(client1) ;
        sync(client2) ;
        
        System.exit(0) ;
        
    }


    private static void sync(DeltaClient client2) {
        int x = client2.getRemoteVersionLatest() ;
        System.out.println("Sync until: "+x) ;
        IntStream.rangeClosed(client2.getCurrentUpdateId()+1,x).forEach((z)->{
            System.out.println("patch = "+z) ;
            //Collect and play?
            PatchReader pr = client2.fetchPatch(z) ;
            pr.apply(new RDFChangesWriter(System.out));
        }) ;
        
    }


    private static void update(DeltaClient client) {
        Quad q = SSE.parseQuad("(_ :s :p :o)") ;
        DatasetGraph dsg = client.getDatasetGraph() ;
        Txn.execWrite(dsg, ()->{
            dsg.add(q); 
        }) ;
        // Done.
    }
}
