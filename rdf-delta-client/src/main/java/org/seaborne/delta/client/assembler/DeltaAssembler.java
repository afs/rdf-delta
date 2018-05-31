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

import static org.apache.jena.sparql.util.graph.GraphUtils.exactlyOneProperty;
import static org.apache.jena.sparql.util.graph.GraphUtils.getAsStringValue;
import static org.seaborne.delta.DeltaConst.symDeltaClient;
import static org.seaborne.delta.DeltaConst.symDeltaConnection;
import static org.seaborne.delta.DeltaConst.symDeltaZone;
import static org.seaborne.delta.client.assembler.VocabDelta.pDeltaChanges;
import static org.seaborne.delta.client.assembler.VocabDelta.pDeltaPatchLog;
import static org.seaborne.delta.client.assembler.VocabDelta.pDeltaStorage;
import static org.seaborne.delta.client.assembler.VocabDelta.pDeltaZone;

import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.jena.assembler.Assembler;
import org.apache.jena.assembler.Mode;
import org.apache.jena.assembler.assemblers.AssemblerBase;
import org.apache.jena.assembler.exceptions.AssemblerException;
import org.apache.jena.atlas.io.IO;
import org.apache.jena.atlas.lib.NotImplemented;
import org.apache.jena.atlas.logging.FmtLog;
import org.apache.jena.atlas.logging.Log;
import org.apache.jena.atlas.web.HttpException;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.util.Context;
import org.apache.jena.tdb.base.file.Location;
import org.seaborne.delta.DataSourceDescription;
import org.seaborne.delta.Delta;
import org.seaborne.delta.Id;
import org.seaborne.delta.client.*;
import org.seaborne.delta.link.DeltaLink;
import org.seaborne.patch.RDFChanges;
import org.seaborne.patch.changes.RDFChangesN;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Assembler for a locally managed dataset with changes set to a remote log */ 
public class DeltaAssembler extends AssemblerBase implements Assembler {
    static private Logger log = LoggerFactory.getLogger(DeltaAssembler.class) ;
    
    /* 
     * Assembler:
     * Type 1 : changes sender.
     * 
     * Type 2: Managed (inc version).
     * 
     * 
     * <#dataset> rdf:type delta:DeltaDataset ;
     *     delta:changes  "http://localhost:1066/" ;
     *     delta:patchlog "ABC"
     *     delta:zone "file path"
     *
     *  and
     *     delta:storage "mem", "file", "tdb" zone info. 
     *     TDB(false, "TDB"), TDB2(false, "TDB2"), FILE(false, "file"), MEM(true, "mem"), EXTERNAL(false, "external"), NONE(true, "none");
     *     
     *     (not implemented)
     *  or
     *     delta:dataset <#datasetOther>, for external.
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
        if ( ! exactlyOneProperty(root, pDeltaZone) )
            throw new AssemblerException(root, "No location for state manangement (zone)") ;
        String zoneLocationStr = getAsStringValue(root, pDeltaZone);
        Location zoneLocation = Location.create(zoneLocationStr);

        // Name of the patch log.
        // delta:patchlog
        if ( ! exactlyOneProperty(root, pDeltaPatchLog) )
            throw new AssemblerException(root, "No patch log name") ;
        // XXX
        String dsName = getAsStringValue(root, pDeltaPatchLog);

        // delta:storage
        if ( ! exactlyOneProperty(root, pDeltaStorage) )
            throw new AssemblerException(root, "No location for state manangement (zone)") ;
        String storageTypeStr = getAsStringValue(root, pDeltaStorage);
        LocalStorageType storage = LocalStorageType.fromString(storageTypeStr);
        if ( storage == null )
            throw new AssemblerException(root, "Unrecognized storage type '"+storageTypeStr+"'");
        
        // Build the RDFChanges: URLs to send each patch log entry. 
        RDFChanges streamChanges = null ;
        for ( String dest : xs ) {
            FmtLog.info(log, "Destination: %s", dest) ;
            RDFChanges sc = DeltaLib.destination(dest);
            streamChanges = RDFChangesN.multi(streamChanges, sc) ;
        }
        Dataset dataset = setupDataset(root, dsName, zoneLocation, storage, destURL);
        
        //  Poll for changes as well.
//      if ( root.hasProperty(pPollForChanges) ) {
//          if ( ! exactlyOneProperty(root, pPollForChanges) )
//              throw new AssemblerException(root, "Multiple places to poll for chnages") ;
//          String source = getStringValue(root, pPollForChanges) ;
//          forkUpdateFetcher(source, dsgSub) ;
//      }
        
        return dataset;
    }
    
    static Dataset setupDataset(Resource root, String dsName, Location zoneLocation, LocalStorageType storage, String destURL) {
        // Link to log server.
        DeltaLink deltaLink = DeltaLinkHTTP.connect(destURL);
        Zone zone = Zone.connect(zoneLocation);
        DeltaClient deltaClient = DeltaClient.create(zone, deltaLink);
        SyncPolicy syncPolicy = SyncPolicy.TXN_RW;
        // Use this as effectively a "ping" of the patch log server.
        registerMaybe(deltaLink);

        Id dsRef = zone.getIdForName(dsName);
        
        if ( dsRef == null ) {
            //DEV System.err.println("Does not exist locally");
            // new locally - need to wait for the server
            try { 
                DataSourceDescription dsd = deltaLink.getDataSourceDescriptionByName(dsName);
                if ( dsd == null )
                    dsRef = deltaClient.newDataSource(dsName, "delta:"+dsName); // CRASH
                else
                    dsRef = dsd.getId();  
                //Id dsRef0 = deltaClient.connect(dsName, syncPolicy);
                // dsRef = deltaClient.newDataSource(dsName, "delta:"+dsName); // CRASH
                deltaClient.register(dsRef, storage, syncPolicy);
            } catch (HttpException ex) {
                throw new AssemblerException(root, "Can't create the dataset with the patch log server: "+ex.getMessage());
            }
        } else {
            //DEV System.err.println("Exists locally");
            // Exists locally.
            try {
                DataSourceDescription dsd = deltaLink.getDataSourceDescriptionByName(dsName);
                if ( dsd == null )
                    throw new AssemblerException(root, "Local dataset has no patch log: "+dsName);
                if ( ! dsd.getId().equals(dsRef) )
                    throw new AssemblerException(root, "Local dataset does not match remote patch log: "+dsName);
            } catch (HttpException ex) {
                // Ignore
            }
            deltaClient.connect(dsRef, syncPolicy);
        }
//        // OLD Local client.
//        Id dsRef = setup(deltaClient, dsName, "delta:"+dsName, storage);
        
        DeltaConnection deltaConnection = deltaClient.getLocal(dsRef);
        DatasetGraph dsg = deltaConnection.getDatasetGraph();
        if ( dsg == null )
            System.err.println("NPE");

        // This DatasetGraph syncs on transaction so it happens, and assumes, a transaction for any Fuseki operation. 
        // And someday tap into services to add a "sync before operation" step.

       // Put state into dsg Context "for the record".
       Context cxt = dsg.getContext();
       cxt.set(symDeltaClient, deltaClient);
       cxt.set(symDeltaConnection, deltaConnection);
       cxt.set(symDeltaZone, zone);
        
       return DatasetFactory.wrap(dsg);
    }
    
    static void registerMaybe(DeltaLink deltaLink) {
        try { 
            deltaLink.ping();
            Id clientId = Id.create();
            deltaLink.register(clientId);
        } catch (HttpException ex) {
            // rc < 0 : failed to connect - ignore.
            if ( ex.getResponseCode() > 0 )
                throw ex;
        }
    }
    
    private InputStream openChangesSrc(String x) {
        // May change to cope with remote source
        return IO.openFile(x) ;
    }

    // Poll for changes.
    private static void UNUSED_CURRENTLY_forkUpdateFetcher(String source, DatasetGraph dsg) {
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
