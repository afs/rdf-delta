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

package org.seaborne.delta.examples;

import java.io.OutputStream;

import org.apache.jena.atlas.logging.LogCtl;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.DatasetGraphFactory;
import org.apache.jena.system.Txn;
import org.seaborne.patch.RDFChanges;
import org.seaborne.patch.RDFPatch;
import org.seaborne.patch.RDFPatchOps;
import org.seaborne.patch.changes.RDFChangesCollector;

/** Set up a dataset, collect the changes as they happen, then write the changes.
 * <p>
 * See {@link DeltaEx01_DatasetWithPatchLog} for a similar example but
 * emitting the change log as changes occur in the transaction.
 */
public class DeltaEx02_DatasetCollectPatch {
    static { LogCtl.setJavaLogging(); }

    public static void main(String ...args) {
        // -- Base dataset
        DatasetGraph dsgBase = DatasetGraphFactory.createTxnMem();

        // -- Destination for changes.
        // Text form of output.
        OutputStream out = System.out;
        // Create an RDFChanges that writes to "out".
        RDFChanges changeLog = RDFPatchOps.textWriter(out);


        // ---- Collect up changes.
        //RDFPatchOps.collect();
        RDFChangesCollector rcc = new RDFChangesCollector();
        DatasetGraph dsg = RDFPatchOps.changes(dsgBase, rcc);
        Dataset ds = DatasetFactory.wrap(dsg);
        Txn.executeWrite(ds,
                         ()->RDFDataMgr.read(dsg, "data.ttl")
                         );
        // Again - different bnodes.
        // Note all changes are recorded - even if they have no effect
        // (e.g the prefix, the triple "ex:s ex:p ex:o").
        Txn.executeWrite(ds,
                         ()->RDFDataMgr.read(dsg, "data.ttl")
                         );

        // Collected (in-memory) patch.
        RDFPatch patch = rcc.getRDFPatch();
        // Write it.
        patch.apply(changeLog);
    }
}
