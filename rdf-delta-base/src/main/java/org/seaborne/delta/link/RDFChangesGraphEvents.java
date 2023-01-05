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
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.core.DatasetGraph;
import org.seaborne.delta.Id;
import org.apache.jena.rdfpatch.RDFChanges;
import org.apache.jena.rdfpatch.RDFPatch;
import org.apache.jena.rdfpatch.changes.RDFChangesWrapper;

/**
 * Wrapper for another {@link RDFChanges}
 * that triggers Jena API graph listener events.
 *
 * Use with
 * @see DeltaLinkEvents#enableGraphEvents(DeltaLink, Id, DatasetGraph)
 */
public class RDFChangesGraphEvents extends RDFChangesWrapper {

    private final Function<Node, Graph> graphFinder;

    /**
     * Construct a {@code RDFChangesGraphEvents}. The function argument is the mapping
     * from graph name node in the {@linkplain RDFPatch} to the graph to notify. Because
     * graphs that are views over datasets are light-weight the graph object might change.
     * The function is responsible for finding the "right" graph object;
     * it probably has a registry of name lookups.
     */
    public RDFChangesGraphEvents(RDFChanges other, Function<Node, Graph> graphFinder) {
        super(other);
        this.graphFinder = graphFinder;
    }

    @Override
    public void add(Node g, Node s, Node p, Node o) {
        super.add(g, s, p, o);
        Graph graph = findGraph(g);
        if ( graph != null )
            graph.getEventManager().notifyAddTriple(graph, Triple.create(s, p, o));
    }

    @Override
    public void delete(Node g, Node s, Node p, Node o) {
        super.delete(g, s, p, o);
        Graph graph = findGraph(g);
        if ( graph != null )
            graph.getEventManager().notifyDeleteTriple(graph, Triple.create(s, p, o));
    }

    private Graph findGraph(Node g) {
        return graphFinder.apply(g);
    }
}
