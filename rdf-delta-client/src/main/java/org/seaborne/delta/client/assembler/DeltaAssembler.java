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
import org.apache.jena.atlas.lib.ListUtils;
import org.apache.jena.atlas.lib.NotImplemented;
import org.apache.jena.atlas.logging.FmtLog;
import org.apache.jena.atlas.logging.Log;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.RDFList;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.tdb.base.file.Location;
import org.seaborne.delta.Delta;
import org.seaborne.delta.client.DeltaConnection;
import org.seaborne.delta.client.DeltaLinkHTTP;
import org.seaborne.delta.client.LocalStorageType;
import org.seaborne.delta.link.DeltaLink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Assembler for a locally managed dataset with changes set to a remote log */
public class DeltaAssembler extends AssemblerBase implements Assembler {
    static private Logger LOG = LoggerFactory.getLogger(DeltaAssembler.class) ;

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
     *
     * If delta:change is a list with more than one element, then that is used to build a
     * switchable DelatLink to replicated delta servers.
     */
    @Override
    public Object open(Assembler a, Resource root, Mode mode) {
        // delta:changes ; list or multiple properties.
        List<String> deltaServers = getAsMultiStringValue(root, pDeltaChanges);
        if ( deltaServers.isEmpty() )
            throw new AssemblerException(root, "No destination for changes given");
        FmtLog.info(Delta.DELTA_CLIENT, "Delta Patch Log Servers: "+deltaServers) ;

        // Name of the patch log.
        // delta:patchlog
        if ( ! exactlyOneProperty(root, pDeltaPatchLog) )
            throw new AssemblerException(root, "No patch log name") ;
        String dsName = getAsStringValue(root, pDeltaPatchLog);

        // delta:storage
        if ( ! exactlyOneProperty(root, pDeltaStorage) )
            throw new AssemblerException(root, "No storge type given.") ;
        String storageTypeStr = getAsStringValue(root, pDeltaStorage);
        LocalStorageType storage = LocalStorageType.fromString(storageTypeStr);
        if ( storage == null )
            throw new AssemblerException(root, "Unrecognized storage type '"+storageTypeStr+"'");

        // delta:zone.
        // The zone is ephemeral if the storage is ephemeral.

        Location zoneLocation;
        if ( storage.isEphemeral() )
            zoneLocation = Location.mem();
        else {
            if ( !exactlyOneProperty(root, pDeltaZone) )
                throw new AssemblerException(root, "No location for state manangement (zone)");
            String zoneLocationStr = getAsStringValue(root, pDeltaZone);
            zoneLocation = Location.create(zoneLocationStr);
        }

        // Build the RDFChanges: URLs to send each patch log entry.
        // This would multiplex the changes to all RDFChanges
//        RDFChanges streamChanges = null ;
//        for ( String dest : deltaServers ) {
//            FmtLog.info(log, "Destination: %s", dest) ;
//            RDFChanges sc = DeltaLib.destination(dest);
//            streamChanges = RDFChangesN.multi(streamChanges, sc) ;
//        }
        DatasetGraph dsg = LibBuildDC.setupDataset(dsName, zoneLocation, storage, deltaServers);
        Dataset dataset = DatasetFactory.wrap(dsg);

        //  Poll for changes as well. Not implemented (yet).
//      if ( root.hasProperty(pPollForChanges) ) {
//          if ( ! exactlyOneProperty(root, pPollForChanges) )
//              throw new AssemblerException(root, "Multiple places to poll for chnages") ;
//          String source = getStringValue(root, pPollForChanges) ;
//          forkUpdateFetcher(source, dsgSub) ;
//      }

        return dataset;
    }

    private List<String> getAsMultiStringValue(Resource r, Property p) {
        Statement stmt = r.getProperty(p) ;
        if ( stmt == null )
            return null ;
        RDFNode obj = stmt.getObject();
        if ( obj.isLiteral() )
            return Arrays.asList(obj.asLiteral().getLexicalForm());
        if ( obj.isURIResource() )
            return Arrays.asList(obj.asResource().getURI());

        RDFList rdfList = obj.asResource().as( RDFList.class );
        List<RDFNode> x = rdfList.asJavaList();
        List<String> xs = ListUtils.toList(x.stream().map(n->{
            if ( n.isLiteral() )
                return n.asLiteral().getLexicalForm();
            if ( n.isURIResource() )
                return n.asResource().getURI();
            throw new AssemblerException(r, "Not a string or URI: "+n);
        }));
        return xs;
    }

//        Id dsRef = zone.getIdForName(dsName);
//        // Given a Id, setup
//        dsRef = setupDeltaClient(root, deltaClient, dsName, dsRef, storage, syncPolicy);
//
//        DeltaConnection deltaConnection = deltaClient.getLocal(dsRef);
//        DatasetGraph dsg = deltaConnection.getDatasetGraph();
//
//        // This DatasetGraph syncs on transaction so it happens, and assumes, a transaction for any Fuseki operation.
//        // And someday tap into services to add a "sync before operation" step.
//
//       // Put state into dsg Context "for the record".
//       Context cxt = dsg.getContext();
//       cxt.set(symDeltaClient, deltaClient);
//       cxt.set(symDeltaConnection, deltaConnection);
//       cxt.set(symDeltaZone, zone);
//
//       return dsg;
//    }
//
//    private static Id setupDeltaClient(Resource root, DeltaClient deltaClient, String dsName, Id dsRef, LocalStorageType storage, SyncPolicy syncPolicy) {
//        // Given a Id, setup the client.
//        DeltaLink deltaLink = deltaClient.getLink();
//        if ( dsRef == null )
//            dsRef = localNew(root, deltaClient, dsName, storage, syncPolicy);
//        else {
//            localExisting(root, deltaClient, dsName, dsRef, syncPolicy);
//        }
//        return dsRef;
//    }
//
//    /** Does not exists locally - may be new remote o an existing remote patch log */
//    private static Id localNew(Resource root, DeltaClient deltaClient, String dsName, LocalStorageType storage, SyncPolicy syncPolicy) {
//        DeltaLink deltaLink = deltaClient.getLink();
//        // new - not in local zone.
//        try {
//
//            DataSourceDescription dsd = deltaLink.getDataSourceDescriptionByName(dsName);
//            Id dsRef;
//            if ( dsd == null )
//                // new remote
//                dsRef = deltaClient.newDataSource(dsName, "delta:"+dsName);
//            else
//                // Exists remote
//                dsRef = dsd.getId();
//            deltaClient.register(dsRef, storage, syncPolicy);
//            return dsRef;
//        } catch (HttpException ex) {
//            throw new AssemblerException(root, "Can't create the dataset with the patch log server: "+ex.getMessage(), ex);
//        } catch (Exception ex) {
//            throw new AssemblerException(root, "Can't create the dataset with the patch log server: exception: "+ex.getMessage(), ex);
//        }
//    }
//
//    /** Exists locally - should exist remotely */
//    private static void localExisting(Resource root, DeltaClient deltaClient, String dsName, Id dsRef, SyncPolicy syncPolicy) {
//        // Existing
//        DeltaLink deltaLink = deltaClient.getLink();
//        try {
//            DataSourceDescription dsd = deltaLink.getDataSourceDescriptionByName(dsName);
//            if ( dsd == null )
//                throw new AssemblerException(root, "Local dataset has no patch log: "+dsName);
//            if ( ! dsd.getId().equals(dsRef) )
//                throw new AssemblerException(root, "Local dataset does not match remote patch log: "+dsName);
//        } catch (HttpException ex) {
//            // Ignore
//        }
//        deltaClient.connect(dsRef, syncPolicy);
//
//    }

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
