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

package org.seaborne.delta.link;

import java.util.function.Function;

import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.sparql.core.DatasetGraph;
import org.seaborne.delta.Id;
import org.seaborne.delta.lib.DSG;
import org.apache.jena.rdfpatch.RDFChanges;
import org.apache.jena.rdfpatch.changes.RDFChangesNoOp;

/** Event related operations on a {@link DeltaLink}. */
public class DeltaLinkEvents {

    /**
     * Listen patches on the {@linkplain DeltaLink} for the patch log with given id.
     * Play the patch to the {@linkplain RDFChanges} after it has been applied to the target dataset.
     * The listener should not update the dataset for which this patch log.
     */
    public static void listenToPatches(DeltaLink dLink, Id dsRef, RDFChanges listener) {
        DeltaLinkListener dLinkListener = new DeltaLinkPatchEvents(dsRef, listener);
        dLink.addListener(dLinkListener);
    }

    /**
     * Enable graph events from applying patches to this dataset.
     * Depends on the DatasetGraph returning the same (or at least "right")
     * graph java object each time which is
     */
    public static void enableGraphEvents(DeltaLink dLink, Id dsRef, DatasetGraph dsg) {
        DatasetGraph dsg2 = DSG.stableViewGraphs(dsg);
        Function<Node, Graph> router = gn->
            (gn==null) ? dsg2.getDefaultGraph() : dsg2.getGraph(gn);
        enableGraphEvents(dLink, dsRef, router);
    }

    /**
     * Enable graph events from applying patches to this dataset.
     * The function maps graph name to Java object (graph name may be null indicating default graph).
     */
    public static void enableGraphEvents(DeltaLink dLink, Id dsRef, Function<Node, Graph> router) {
        RDFChanges replay = new RDFChangesGraphEvents(new RDFChangesNoOp(), router);
        listenToPatches(dLink, dsRef, replay);
    }
}
