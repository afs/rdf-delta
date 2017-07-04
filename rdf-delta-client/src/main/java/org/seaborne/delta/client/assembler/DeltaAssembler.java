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

package org.seaborne.delta.client.assembler;

import static org.apache.jena.sparql.util.graph.GraphUtils.exactlyOneProperty ;
import static org.apache.jena.sparql.util.graph.GraphUtils.getAsStringValue ;
import static org.seaborne.delta.client.assembler.VocabDelta.pDeltaChanges ;
import static org.seaborne.delta.client.assembler.VocabDelta.pPatchLog ;
import static org.seaborne.delta.client.assembler.VocabDelta.pStorage ;
import static org.seaborne.delta.client.assembler.VocabDelta.pZone ;
import static org.seaborne.delta.DeltaConst.*;

import java.io.InputStream ;
import java.util.Arrays ;
import java.util.List ;
import java.util.concurrent.Executors ;
import java.util.concurrent.ScheduledExecutorService ;
import java.util.concurrent.TimeUnit ;

import org.apache.jena.assembler.Assembler ;
import org.apache.jena.assembler.Mode ;
import org.apache.jena.assembler.assemblers.AssemblerBase ;
import org.apache.jena.assembler.exceptions.AssemblerException ;
import org.apache.jena.atlas.io.IO ;
import org.apache.jena.atlas.lib.NotImplemented ;
import org.apache.jena.atlas.logging.FmtLog ;
import org.apache.jena.atlas.logging.Log ;
import org.apache.jena.query.DatasetFactory ;
import org.apache.jena.rdf.model.Resource ;
import org.apache.jena.sparql.core.DatasetGraph ;
import org.apache.jena.sparql.util.Context ;
import org.apache.jena.sparql.util.Symbol ;
import org.seaborne.delta.DataSourceDescription ;
import org.seaborne.delta.Delta ;
import org.seaborne.delta.DeltaConst ;
import org.seaborne.delta.Id ;
import org.seaborne.delta.client.* ;
import org.seaborne.delta.link.DeltaLink;
import org.seaborne.patch.RDFChanges ;
import org.seaborne.patch.changes.RDFChangesN ;
import org.slf4j.Logger ;
import org.slf4j.LoggerFactory ;

public class DeltaAssembler extends AssemblerBase implements Assembler {
    static private Logger log = LoggerFactory.getLogger(DeltaAssembler.class) ;
    
    /* 
     * Assembler:
     * 
     * <#dataset> rdf:type delta:DeltaDataset ;
     *     delta:changes  "http://localhost:1066/" ;
     *     delta:patchlog "ABC"
     *     delta:zone "file path"
     *     delta:storage "mem", "file", "tdb" zone info.
     *   or
     *      delta:dataset <#daatsetOther>
     */
    
    @Override
    public Object open(Assembler a, Resource root, Mode mode) {
        // delta:changes.
        if ( ! exactlyOneProperty(root, pDeltaChanges) )
            throw new AssemblerException(root, "No destination for changes given") ;
        String destURL = getAsStringValue(root, pDeltaChanges);
        // Future - multiple outputs. 
        List<String> xs = Arrays.asList(destURL);

        // delta:zone.
        if ( ! exactlyOneProperty(root, pDeltaChanges) )
            throw new AssemblerException(root, "No location for state manangement (zone)") ;
        String zoneLocation = getAsStringValue(root, pZone);

        // Should be part of changes URL.
        // delta:patchlog
        if ( ! exactlyOneProperty(root, pPatchLog) )
            throw new AssemblerException(root, "No patch log name") ;
        String dsName = getAsStringValue(root, pPatchLog);

        // delta:storage
        if ( ! exactlyOneProperty(root, pDeltaChanges) )
            throw new AssemblerException(root, "No location for state manangement (zone)") ;
        String storageTypeStr = getAsStringValue(root, pStorage);
        LocalStorageType storage = LocalStorageType.fromString(storageTypeStr);
        if ( storage == null )
            throw new AssemblerException(root, "Unrecognized storage type '"+storageTypeStr+"'");
        RDFChanges streamChanges = null ;
        
        for ( String dest : xs ) {
            FmtLog.info(log, "Destination: '%s'", dest) ;
            RDFChanges sc = DeltaLib.destination(dest+DeltaConst.EP_Append) ;
            streamChanges = RDFChangesN.multi(streamChanges, sc) ;
        }
        
       Zone zone = Zone.get();
       zone.init(zoneLocation);
       
       // *****
       DeltaLink deltaLink = DeltaLinkHTTP.connect(destURL);
       Id clientId = Id.create();
       deltaLink.register(clientId);
       
       DeltaClient deltaClient = DeltaClient.create(zone, deltaLink);
       Id dsRef =  setup(deltaClient, dsName, "delta:"+dsName, storage);
       DeltaConnection deltaConnection = deltaClient.get(dsRef);
       DatasetGraph dsg = deltaConnection.getDatasetGraph();
       
       // Put state into dsg Context.
        
//        // And someday tap into services to add a "sync before operation" step.
//        if ( root.hasProperty(pPollForChanges) ) {
//            if ( ! exactlyOneProperty(root, pPollForChanges) )
//                throw new AssemblerException(root, "Multiple places to poll for chnages") ;
//            String source = getStringValue(root, pPollForChanges) ;
//            forkUpdateFetcher(source, dsgSub) ;
//        }
        
       Context cxt = dsg.getContext();
       cxt.set(symDeltaClient, deltaClient);
       cxt.set(symDeltaConnection, deltaConnection);
       cxt.set(symDeltaZone, zone);
        
       return DatasetFactory.wrap(dsg);
    }

    static Id setup(DeltaClient dClient, String datasourceName, String baseURI, LocalStorageType storageType) {
        DataSourceDescription dsd = dClient.getLink().getDataSourceDescriptionByURI(baseURI);
        if ( dsd == null )
            return dClient.newDataSource(datasourceName, baseURI, storageType);
        Id dsRef = dsd.getId();
        if ( dClient.getZone().exists(dsRef) )
            // New locally, exists in server.
            dClient.connect(dsRef);
        else
            dClient.attach(dsRef, storageType);    
        return dsRef;
    }
    
    private InputStream openChangesSrc(String x) {
        // May change to cope with remote source
        return IO.openFile(x) ;
    }

    private static void forkUpdateFetcher(String source, DatasetGraph dsg) {
        if ( true ) {
            Log.warn(DeltaAssembler.class, "forkUpdateFetcher not set up");
            if ( true ) return;
            throw new NotImplemented("NEED THE STATE AREA; NEED THE DATASOURCE ID; NEED THE CLIENT ID");
        }
        DeltaLink dc = DeltaLinkHTTP.connect(source) ;
        DeltaConnection client = null;
        Runnable r = ()->{
            try { client.sync(); }
            catch (Exception ex) { 
                Delta.DELTA_LOG.warn("Failed to sync with the change server: "+ex.getMessage()) ;
//                // Delay this task ; extra 3s + the time interval of 2s.
//                Dones not work as expected.
//                Lib.sleep(5*1000);
            }
        } ;
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(1) ;
        executor.scheduleWithFixedDelay(r, 2, 2, TimeUnit.SECONDS) ;
    }
}
