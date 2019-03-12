/*
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 *  See the NOTICE file distributed with this work for additional
 *  information regarding copyright ownership.
 */

package org.seaborne.delta.tools;

import org.apache.jena.atlas.lib.FileOps ;
import org.apache.jena.atlas.lib.Timer ;
import org.apache.jena.atlas.logging.FmtLog ;
import org.apache.jena.atlas.logging.LogCtl ;
import org.apache.jena.riot.RDFDataMgr ;
import org.apache.jena.riot.lang.StreamRDFCounting ;
import org.apache.jena.riot.system.StreamRDF ;
import org.apache.jena.riot.system.StreamRDFLib ;
import org.apache.jena.sparql.core.DatasetGraph ;
import org.apache.jena.system.Txn ;
import org.apache.jena.tdb.base.file.Location ;
import org.seaborne.delta.Id ;
import org.seaborne.delta.client.* ;
import org.seaborne.delta.link.DeltaLink ;
import org.slf4j.Logger ;
import org.slf4j.LoggerFactory ;

public class load {
    static {
        LogCtl.setCmdLogging() ;
        LogCtl.setJavaLogging();
    }

    // Load a file into a managed dataset with timing.
    public static void main(String...argv) {
        Logger LOG = LoggerFactory.getLogger("Load");

        if ( argv.length == 0 ) {
            System.err.println("Usage: load FILE...");
            System.exit(1);
        }

        String[] args = argv; //new String[] {"/home/afs/Datasets/BSBM/bsbm-5m.nt.gz"};

        String DIR = "ZoneX";
        String URL = "http://localhost:1066/";
        String DS  = "DS";

        FileOps.ensureDir(DIR);
        FileOps.clearDirectory(DIR);
        Zone zone = Zone.connect(Location.create(DIR));
        DeltaLink dLink = DeltaLinkHTTP.connect(URL);

        DeltaClient dClient = DeltaClient.create(zone, dLink);
        Id dsRef = dClient.createDataSource(DS, "http://example/"+DS, LocalStorageType.TDB, SyncPolicy.TXN_RW);

        long count = -99;
        Timer timer = new Timer();
        timer.startTimer();
        try ( DeltaConnection dConn = dClient.get(dsRef) ) {
            DatasetGraph dsg = dConn.getDatasetGraph();
            StreamRDF dest = StreamRDFLib.dataset(dsg);
            StreamRDFCounting cdest = StreamRDFLib.count(dest);
            Txn.executeWrite(dsg, ()->{
                for ( String fn : args ) {
                    System.out.printf("File: %s\n", fn);
                    RDFDataMgr.parse(cdest, fn);
                }
            });
            count = cdest.count();
        }
        long x = timer.endTimer();
        double seconds = x/1000.0;
        FmtLog.info(LOG, "Time  = %.2fs\n", seconds);
        FmtLog.info(LOG, "Count = %,d\n", count);
        FmtLog.info(LOG, "Rate  = %,.2f TPS\n", count/seconds);
    }
}
