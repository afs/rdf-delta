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

package org.seaborne.delta.client.assembler;

import static org.apache.jena.sparql.util.graph.GraphUtils.exactlyOneProperty;
import static org.apache.jena.sparql.util.graph.GraphUtils.getAsStringValue;
import static org.seaborne.delta.client.assembler.VocabDelta.*;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.jena.assembler.Assembler;
import org.apache.jena.assembler.Mode;
import org.apache.jena.assembler.assemblers.AssemblerBase;
import org.apache.jena.assembler.exceptions.AssemblerException;
import org.apache.jena.atlas.lib.ListUtils;
import org.apache.jena.atlas.lib.NotImplemented;
import org.apache.jena.atlas.logging.FmtLog;
import org.apache.jena.atlas.logging.Log;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.rdf.model.*;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.util.graph.GraphUtils;
import org.apache.jena.dboe.base.file.Location;
import org.seaborne.delta.Delta;
import org.seaborne.delta.client.DeltaConnection;
import org.seaborne.delta.client.DeltaLinkHTTP;
import org.seaborne.delta.client.LocalStorageType;
import org.seaborne.delta.link.DeltaLink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Assembler for a locally managed dataset with changes sent to a remote log */
public class DeltaAssembler extends AssemblerBase implements Assembler {
    static private Logger LOG = LoggerFactory.getLogger(DeltaAssembler.class) ;

    /*
     * Assembler:
     * <#dataset> rdf:type delta:DeltaDataset ;
     *     delta:changes  "http://localhost:1066/" ;
     *     delta:patchlog "ABC"
     *     delta:zone "file path"
     *     delta:storage "mem", "file", "tdb" zone info.
     *
     * If delta:changes is a list with more than one element, then that is used to build a
     * switchable DeltaLink to replicated delta servers.
     */
    @Override
    public Dataset open(Assembler a, Resource root, Mode mode) {
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
        // delta:dataset, which is implicitly delta:storage "external".
        // Checking stage.

        boolean hasExternalDataset = root.hasProperty(pDataset);
        DatasetGraph externalDataset = null;
        if ( hasExternalDataset ) {
            Resource datasetR = GraphUtils.getResourceValue(root, pDataset) ;
            Dataset ds = (Dataset)a.open(datasetR) ;
            externalDataset = ds.asDatasetGraph();
        }

        if ( ! root.hasProperty(pDeltaStorage) && ! hasExternalDataset )
            throw new AssemblerException(root, "Must have storage type (delta:storage) or an external dataset description (delat:dataset)");

        String storageTypeStr = getAsStringValue(root, pDeltaStorage);
        LocalStorageType storage = LocalStorageType.fromString(storageTypeStr);

        // dataset implicitly means external.
        if ( storage == null ) {
            if ( hasExternalDataset )
                storage = LocalStorageType.EXTERNAL;
            else
                throw new AssemblerException(root, "Unrecognized storage type '"+storageTypeStr+"'");
        }

        if ( hasExternalDataset && ( storage != LocalStorageType.EXTERNAL ) )
            throw new AssemblerException(root, "Storage type must be 'external' or absent when using delta:dataset");
        if ( ( storage == LocalStorageType.EXTERNAL ) && (externalDataset == null) )
            throw new AssemblerException(root, "Storage type 'external' but no delta:dataset provided");

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

        // Build.
        DatasetGraph dsg = (externalDataset == null)
                ? LibBuildDC.setupDataset(dsName, zoneLocation, storage, deltaServers)
                : LibBuildDC.setupDataset(dsName, zoneLocation, externalDataset, deltaServers);
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

    // Poll for changes.
    // Currently, polling happens in the DeltaConnection, initiated from DeltaConnection.start
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
//                Does not work as expected.
//                Lib.sleep(5*1000);
            }
        } ;
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(1) ;
        executor.scheduleWithFixedDelay(r, 2, 2, TimeUnit.SECONDS) ;
    }
}
