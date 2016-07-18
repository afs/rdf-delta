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

import org.apache.jena.atlas.lib.DateTimeUtils ;
import org.apache.jena.atlas.lib.FileOps ;
import org.apache.jena.atlas.lib.Lib ;
import org.apache.jena.atlas.lib.StrUtils ;
import org.apache.jena.atlas.logging.LogCtl ;
import org.apache.jena.query.Dataset ;
import org.apache.jena.query.DatasetFactory ;
import org.apache.jena.riot.Lang ;
import org.apache.jena.riot.RDFDataMgr ;
import org.apache.jena.sparql.core.DatasetGraph ;
import org.apache.jena.system.Txn ;
import org.apache.jena.tdb.TDBFactory ;
import org.apache.jena.update.UpdateAction ;
import org.apache.jena.update.UpdateFactory ;
import org.apache.jena.update.UpdateRequest ;
import org.seaborne.delta.DP ;
import org.seaborne.delta.server.DataPatchServer ;

public class RunDelta {
    static { LogCtl.setJavaLogging(); }
    
    public static void main(String[] args) throws IOException {
        
        if ( true )
            FileOps.clearDirectory("Files");
        
        DataPatchServer server = new DataPatchServer(1066) ;
        server.start();
        
        // One datasets
        dev1() ;
        // Two datasets.
        //dev2() ;
    }
    
    public static void dev2() throws IOException {
        Dataset ds1 = TDBFactory.createDataset() ;
        Dataset ds2 = DatasetFactory.createTxnMem() ;
        
        String updateStr = StrUtils.strjoinNL
            ("INSERT DATA { <http://example/s> <http://example/p> 'XXX', 'X1' } ; "
            ,"DELETE DATA { <http://example/s> <http://example/p> 'X1' } ;"
            ) ;
        UpdateRequest req = UpdateFactory.create(updateStr) ;
        
        DP.syncExecW(ds1.asDatasetGraph(), () -> { 
            System.out.println("==== Before update");
            RDFDataMgr.write(System.out, ds1, Lang.TRIG) ;
            UpdateAction.execute(req, ds1) ;
            System.out.println("==== After update");
            RDFDataMgr.write(System.out, ds1, Lang.TRIG) ;
            System.out.println("====") ;
        }) ;
        
        if ( true ) {
          DP.syncData(ds2.asDatasetGraph()) ;
          System.out.println("==== Sync'ed.");
          Txn.execRead(ds2, ()->RDFDataMgr.write(System.out, ds2, Lang.TRIG)) ;
          System.out.println("== Exit") ;
      }
    }
    
    public static void dev1() throws IOException {
        Dataset ds1 = TDBFactory.createDataset() ;
        DatasetGraph dsg = DP.managedDatasetGraph(ds1.asDatasetGraph(), DP.PatchContainer) ;
        Dataset ds = DatasetFactory.wrap(dsg) ;
        
        String template = "INSERT DATA { <http://example/s> <http://example/p> 'XXX'} ";   

//        if ( true ) {
//            DP.syncData(ds.asDatasetGraph()) ;
//            System.out.println("==== Sync'ed.");
//            Txn.execRead(ds, ()->RDFDataMgr.write(System.out, ds, Lang.TRIG)) ;
//            System.out.println("== Exit") ;
//            System.exit(0) ;
//        }

        for ( int i = 0 ; i < 2 ; i++ ) {
            System.out.println("Update "+i);
//            System.in.read() ;
            String s = template.replace("XXX", DateTimeUtils.nowAsString()) ;
            UpdateRequest req = UpdateFactory.create(s) ;
            
            
            DP.syncExecW(ds.asDatasetGraph(), () -> { 
                System.out.println("==== Before update");
                RDFDataMgr.write(System.out, ds, Lang.TRIG) ;
                UpdateAction.execute(req, ds) ;
                System.out.println("==== After update");
                RDFDataMgr.write(System.out, ds, Lang.TRIG) ;
                System.out.println("====") ;
            }) ;
        }
        
        Lib.sleep(500) ;
        System.out.println("== Exit") ;
        System.exit(0) ;
    }

}
