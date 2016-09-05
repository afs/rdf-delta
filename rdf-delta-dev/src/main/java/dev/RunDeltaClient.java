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

import java.io.IOException ;
import java.util.concurrent.Executors ;
import java.util.concurrent.ScheduledExecutorService ;
import java.util.concurrent.TimeUnit ;

import org.apache.jena.atlas.logging.FmtLog ;
import org.apache.jena.riot.Lang ;
import org.apache.jena.riot.RDFDataMgr ;
import org.apache.jena.sparql.core.DatasetGraph ;
import org.apache.jena.sparql.core.Quad ;
import org.apache.jena.sparql.sse.SSE ;
import org.apache.jena.system.Txn ;
import org.apache.jena.tdb.TDBFactory ;
import org.seaborne.delta.DP ;
import org.seaborne.delta.client.DeltaClient ;
import org.slf4j.Logger ;
import org.slf4j.LoggerFactory ;

public class RunDeltaClient {
    static { DevLib.setLogging(); }
    static Logger LOG = LoggerFactory.getLogger("Main") ;
    
    static String url = "http://localhost:"+DP.PORT+"/rpc" ; 
    
    public static void main(String... args) {
        runOrExit(()->run()) ;
        
//        DatasetGraph dsg = DatasetGraphFactory.createTxnMem() ;
//        try {
//            DataPatchServer server = new DataPatchServer(DP.PORT, Setup.handlers(dsg)) ;
//            server.start();
//            FusekiEmbeddedServer.make(3333, "/ds", dsg).start() ;
//            run(dsg);
//        } catch (Throwable ex) {
//            ex.printStackTrace(System.err) ;
//        }
//        finally { 
//            //System.exit(0) ;
//        }
    }
    
    public static void runOrExit(Runnable action) {
        try { action.run(); }
        catch (Throwable ex) { ex.printStackTrace(System.err) ; } 
        finally { System.exit(0) ; }
    }
    
    public static void run() {
        DatasetGraph dsg1 = TDBFactory.createDatasetGraph() ;
        DeltaClient client1 = DeltaClient.create("C1", "http://localhost:"+DP.PORT+"/", dsg1) ;
        //syncAgent(client1) ;
        System.out.println(url);
        
        client1.sync() ;
        Txn.execRead(dsg1, ()->{
            System.out.println() ;
            RDFDataMgr.write(System.out,  dsg1, Lang.NQ);
            System.out.println() ;
        }) ;

        System.out.println("Waiting ...");
        try { System.in.read() ; }
        catch (IOException e) { e.printStackTrace(); }
        
        update(client1) ;
        Txn.execRead(dsg1, ()->{
            System.out.println() ;
            RDFDataMgr.write(System.out,  dsg1, Lang.NQ);
            System.out.println() ;
        }) ;
        FmtLog.info(LOG, "%s", client1) ;
    }
    
    static ScheduledExecutorService executor = Executors.newScheduledThreadPool(1) ;
    
    private static void syncAgent(DeltaClient client) {
        executor.scheduleWithFixedDelay(()->{
            client.sync() ;
        },  2, 2, TimeUnit.SECONDS) ;
    }

//    private static void doOnePatchBuffered(int z, DeltaClient client) {
//        PatchReader pr = client.fetchPatch(z) ;
//        RDFChangesCollector acc = new RDFChangesCollector() ;
//        acc.start() ;
//        pr.apply(acc);
//        acc.finish() ;
//        acc.play(new RDFChangesWriter(System.out));
//        DatasetGraph dsg = client.getStorage() ;
//        //No needed if the patch includes a Txn
//        RDFChanges rc = new RDFChangesApply(dsg) ;
//        acc.play(rc) ;
//    }
//    
//    private static void doOnePatchStreamed(int z, DeltaClient client) {
//        //==> DeltaClient
//        // Synchronization (via transactions? via patch-chain)
//        PatchReader pr = client.fetchPatch(z) ;
//        DatasetGraph dsg = client.getStorage() ;
//        RDFChanges rc1 = new RDFChangesApply(dsg) ;
//        RDFChanges rc2 = new RDFChangesWriter(System.out);
//        RDFChanges rc = new RDFChangesN(rc1, rc2) ;
//        pr.apply(rc);
//        client.setLocalVersionNumber(z) ;
//    }

//    //private static void doOnePatchUnbuffered(
//
    private static void update(DeltaClient client) {
        DatasetGraph dsg = client.getDatasetGraph() ;
        int version = client.getLocalVersionNumber() ;
        Txn.execWrite(dsg, ()->{
            Quad q = SSE.parseQuad("(_ :s :p _:b)") ;

            dsg.add(q); 
        }) ;
        client.setLocalVersionNumber(version+1); 
        // Done.
    }
}
