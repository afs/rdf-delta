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

import java.io.ByteArrayInputStream ;
import java.io.ByteArrayOutputStream ;

import org.apache.jena.graph.Graph ;
import org.apache.jena.graph.Node ;
import org.apache.jena.query.Dataset ;
import org.apache.jena.query.DatasetFactory ;
import org.apache.jena.riot.Lang ;
import org.apache.jena.riot.RDFDataMgr ;
import org.apache.jena.sparql.core.DatasetGraph ;
import org.apache.jena.sparql.core.DatasetGraphFactory ;
import org.apache.jena.sparql.core.Quad ;
import org.apache.jena.sparql.sse.SSE ;
import org.apache.jena.system.Txn ;
import org.apache.jena.tdb.TDBFactory ;
import org.seaborne.delta.base.* ;
import org.seaborne.delta.changes.* ;

public class DevTrack {

    public static void main(String[] args) {
        
        Quad q1 = SSE.parseQuad("(:g1 :s1 :p1 :o1)") ;
        Quad q2 = SSE.parseQuad("(_ :s1 :p1 :o1)") ;
        Quad q3 = SSE.parseQuad("(:g2 :s1 :p1 :o1)") ;
        Node s1 = SSE.parseNode(":s1") ;   
        
        if ( true ) {
            DatasetGraph dsg0 = DatasetGraphFactory.create() ;
            DatasetGraph dsg = dsg0 ;
            
            // Change log.
            StreamChanges sc = new StreamChangesWriter(System.out) ;
            dsg = new DatasetGraphChanges(dsg, sc) ;
            
            // Only real ones.
            dsg = new DatasetGraphRealChanges(dsg) ;
            
            
            dsg.add(q1) ;
            dsg.add(q1) ;            
            dsg.add(q1) ;
            dsg.delete(q1) ;
            dsg.delete(q1) ;
            dsg.add(q1) ;
            RDFDataMgr.write(System.out, dsg0, Lang.NQ) ;
            System.exit(0) ;
        }   
            
        if ( false ) {
            Dataset ds2 = DatasetFactory.createTxnMem() ;
            StreamChanges changes = new StreamChangesLog() ; 

            // Dataset with a monitor. 
            DatasetGraph dsg = new DatasetGraphChanges(ds2.asDatasetGraph(), changes) ;
            //Dataset dsMaster = DatasetFactory.wrap(new DSGMonitor(ds2.asDatasetGraph(), changes)) ;

            Txn.execWrite(dsg, ()-> {
                dsg.add(q1) ;   
                dsg.add(q2) ;
                dsg.add(q3) ;
                dsg.deleteAny(null, s1, null, null);
            }) ;
            System.exit(0) ;
        }  
        
        if ( false ) {
            // Apply now!
            Dataset ds1 = DatasetFactory.createTxnMem() ;
            Dataset ds2 = TDBFactory.createDataset() ;
            StreamChanges changes = new StreamChangesApply(ds2.asDatasetGraph()) ;
            DatasetGraph dsg = new DatasetGraphChanges(ds1.asDatasetGraph(), changes) ;
            Txn.execWrite(dsg, ()-> {
                dsg.getDefaultGraph().getPrefixMapping().setNsPrefix("", "http://example/") ;
                dsg.add(q1) ;   
                dsg.add(q2) ;
                dsg.add(q3) ;
                //dsg.deleteAny(null, s1, null, null);
            }) ;
            Txn.execRead(ds1, ()-> RDFDataMgr.write(System.out, ds1, Lang.TRIG)) ;
            System.out.println("-------------") ;
            Txn.execRead(ds2, ()-> RDFDataMgr.write(System.out, ds2, Lang.TRIG)) ;
        }
        
        if ( false ) {
            // Delayed style.
            Dataset ds1 = DatasetFactory.createTxnMem() ;
            Dataset ds2 = TDBFactory.createDataset() ;
            StreamChangesBuffering changes = new StreamChangesBuffering() ;
            DatasetGraph dsg = new DatasetGraphChanges(ds1.asDatasetGraph(), changes) ;
            Txn.execWrite(dsg, ()-> {
    //            dsg.getDefaultGraph().getPrefixMapping().setNsPrefix("", "http://example/") ;
    //            changes.addPrefix(null, "", "http://example/") ;
    //            dsg.add(q1) ;   
    //            dsg.add(q2) ;
    //            dsg.add(q3) ;
                
                Graph g = dsg.getDefaultGraph() ;
                g.getPrefixMapping().setNsPrefix("", "http://example/") ;
                g.add(SSE.parseTriple("(:sg :pg :og)")) ;
            }) ;
            StreamChanges changes2 = new StreamChangesApply(ds2.asDatasetGraph()) ;
            changes.play(changes2);
            Txn.execRead(ds1, ()-> RDFDataMgr.write(System.out, ds1, Lang.TRIG)) ;
            System.out.println("-------------") ;
            Txn.execRead(ds2, ()-> RDFDataMgr.write(System.out, ds2, Lang.TRIG)) ;
        }
        
        {
            // To "file"
            Dataset ds1 = DatasetFactory.createTxnMem() ;
            Dataset ds2 = DatasetFactory.createTxnMem() ;
            
            ByteArrayOutputStream out = new ByteArrayOutputStream() ;
            StreamChanges changes = new StreamChangesWriter(out) ;
            DatasetGraph dsg = new DatasetGraphChanges(ds1.asDatasetGraph(), changes) ;
            Txn.execWrite(dsg, ()-> {
    //            dsg.getDefaultGraph().getPrefixMapping().setNsPrefix("", "http://example/") ;
    //            changes.addPrefix(null, "", "http://example/") ;
    //            dsg.add(q1) ;   
    //            dsg.add(q2) ;
    //            dsg.add(q3) ;
                dsg.add(q1) ;
                Graph g = dsg.getDefaultGraph() ;
                g.getPrefixMapping().setNsPrefix("", "http://example/") ;
                g.add(SSE.parseTriple("(:sg :pg :og)")) ;
            }) ;
            
            byte[] bytes = out.toByteArray() ;
            ByteArrayInputStream inp = new ByteArrayInputStream(bytes) ;
            PatchReader r = new PatchReader(inp) ;
            StreamChanges changes2 = new StreamChangesApply(ds2.asDatasetGraph()) ;
            r.apply(changes2); 
            
            Txn.execRead(ds2, ()-> RDFDataMgr.write(System.out, ds2, Lang.TRIG)) ;
        }
    }
}


