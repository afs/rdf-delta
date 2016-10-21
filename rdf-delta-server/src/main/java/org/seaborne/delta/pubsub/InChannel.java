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

package org.seaborne.delta.pubsub;

import java.io.BufferedOutputStream ;
import java.io.InputStream ;
import java.io.OutputStream ;
import java.util.ArrayList ;
import java.util.List ;
import java.util.concurrent.atomic.AtomicLong ;

import org.apache.jena.atlas.io.IO ;
import org.apache.jena.atlas.lib.NotImplemented ;
import org.apache.jena.sparql.core.DatasetGraph ;
import org.seaborne.delta.server.PatchHandler ;
import org.seaborne.patch.PatchReader ;
import org.seaborne.patch.RDFChanges ;
import org.seaborne.patch.changes.RDFChangesCollector ;
import org.seaborne.patch.changes.RDFChangesLog ;
import org.seaborne.patch.changes.RDFChangesN ;
import org.seaborne.patch.changes.RDFChangesWriter ;
import org.seaborne.riot.tio.TokenWriter ;
import org.seaborne.riot.tio.impl.TokenWriterText ;

public class InChannel {
    /*
     * In-bound processing: parse (=check) and then write to disk. 
     * Can then reply to sender.
     * Then secondary (from collector or disk.
     * 
     */
    
    
    // Or factory that makes an in-bound processor.
    
    // The pipeline, initialized with the collector at the end.
    
    // Either 

    private List<RDFChanges> additionalProcessors = new ArrayList<>() ;
    
    public InChannel() {
        addProcessor(new RDFChangesLog(RDFChangesLog::printer)) ;
    }
    
    private String baseFilename = "Files/patch-" ;
    private AtomicLong counter = new AtomicLong(0);
    
    
    // -- Builderish.
    
    public void addProcessor(RDFChanges changes) {
        additionalProcessors.add(changes) ;
    }
    
    // -- 
    
    // Special incremental RDFChangesWriter ; start/finish triggered.
    
    public synchronized void receive(InputStream in) {
        long id = counter.incrementAndGet() ;
        String fn = String.format("%s%04d",baseFilename,id) ;
        OutputStream out = IO.openOutputFile(fn) ;
        out = new BufferedOutputStream(out, 2*1024*1024) ;
        TokenWriter tw = new TokenWriterText(out) ;

        // --- Pipeline for incoming to safe.
        // The end of pipeline is the writer to disk.
        // Just before that we keep an in-memory copy.
        RDFChanges pipeline = new RDFChangesWriter(tw) ;
        // Fresh collector.
        
        // 1 - Make bounded.
        // 2 - Make disk backed
        RDFChangesCollector collector = new RDFChangesCollector() ; // Make bounded.
        pipeline = RDFChangesN.multi(collector, pipeline) ;
        // --- Additional processors before making safe.
        
        for ( RDFChanges p : additionalProcessors )
            pipeline = RDFChangesN.multi(p, pipeline) ;

//        // If last is abort ...
//        // Add a "last capture"
//        // If all are abort, loose it.
//        
//        RDFChanges abortOrCommit = new RDFChangesBase() {
//            int commitCount = 0 ;
//            int abortCount = 0 ;
//            // And is it last?
//            
//            @Override
//            public void txnCommit() { commitCount++ ; }
//            
//            @Override
//            public void txnAbort() { abortCount++ ; }
//        } ;
        
        
        
        PatchReader scr = new PatchReader(in) ;
        pipeline.start();
        scr.apply(pipeline);
        pipeline.finish() ;
        // Ensure on-disk.
        IO.flush(out) ;
        // SAFE!
        // Reply!
        
//        
//        
//        // Schedule other work.
//        for each out queue
//          add collector to queue.
        
        
        //collector.reset() ;
    }
    
    public static PatchHandler[] handlers(DatasetGraph dsg) {
        if ( true )
            throw new NotImplemented("InChannel handlers") ;
        
        List<PatchHandler> x = new ArrayList<>() ;
//        if ( dsg != null )
//            x.add(new PHandlerLocalDB(dsg)) ;
//        x.add(new PHandlerOutput(System.out)) ;
////            x.add(new PHandlerGSPOutput()) ;
////            x.add(new PHandlerGSP().addEndpoint("http://localhost:3030/ds/update")) ;
//        x.add(new PHandlerToFile()) ;
//        x.add(new PHandlerLog(DPS.LOG)) ;

        return x.toArray(new PatchHandler[0]) ;
    }
}
