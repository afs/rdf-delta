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

package org.seaborne.patch.system;

import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.query.TxnType;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.DatasetGraphTriplesQuads;
import org.apache.jena.sparql.core.GraphView;
import org.apache.jena.sparql.core.Quad;
import org.seaborne.patch.RDFChanges;

// UNFINISHED

/** A {@link DatasetGraph} implementation that buffers adds/deletes and prefix changes
 * then plays them to another {@link DatasetGraph} or to a {@link RDFChanges}.
 * <p>
 * This has the capacity to add {@link DatasetGraph#abort} functionality to a {@link DatasetGraph}
 * that does not provide it.
 */
public class DatasetGraphBuffering1 extends DatasetGraphTriplesQuads {

    // Structurally safe but not thread safe.
    private Set<Triple> addTriples      = ConcurrentHashMap.newKeySet();
    private Set<Triple> deleteTriples   = ConcurrentHashMap.newKeySet();
    private Set<Quad> addQuads          = ConcurrentHashMap.newKeySet();
    private Set<Quad> deleteQuads       = ConcurrentHashMap.newKeySet();
    // Necessary to check to avoid duplicate suppression in find*
    private final DatasetGraph dsg;
    protected DatasetGraph get() { return dsg ; }

    public DatasetGraphBuffering1(DatasetGraph dsg) {
        this.dsg = dsg;
    }

    // Ensure we loop back here
    @Override
    public Graph getDefaultGraph() {
        return GraphView.createDefaultGraph(this);
    }

    @Override
    public Graph getGraph(Node graphNode) {
        return GraphView.createNamedGraph(this, graphNode);
    }

    @Override
    public Iterator<Node> listGraphNodes() {
        return null;
    }

    @Override
    public boolean supportsTransactions() {
        return false;
    }

    @Override
    public void begin(TxnType type) {}

    @Override
    public void begin(ReadWrite readWrite) {}

    @Override
    public boolean promote(Promote mode) {
        return false;
    }

    @Override
    public void commit() {}

    @Override
    public void abort() {}

    @Override
    public ReadWrite transactionMode() {
        return null;
    }

    @Override
    public TxnType transactionType() {
        return null;
    }

    @Override
    public boolean isInTransaction() {
        return false;
    }

    @Override
    public void end() {}

    @Override
    protected void addToDftGraph(Node s, Node p, Node o) {
        if ( get().contains(Quad.defaultGraphIRI, s, p ,o) )
            return;
        Triple triple = Triple.create(s, p, o);
        if ( addTriples.add(triple) )
            deleteTriples.remove(triple);
    }

    @Override
    protected void addToNamedGraph(Node g, Node s, Node p, Node o) {
        if ( get().contains(Quad.defaultGraphIRI, s, p ,o) )
            return;
        Triple triple = Triple.create(s, p, o);
        if ( addTriples.add(triple) )
            deleteTriples.remove(triple);
    }

    @Override
    protected void deleteFromDftGraph(Node s, Node p, Node o) {}

    @Override
    protected void deleteFromNamedGraph(Node g, Node s, Node p, Node o) {}

    @Override
    protected Iterator<Quad> findInDftGraph(Node s, Node p, Node o) {
        return null;
    }

    @Override
    protected Iterator<Quad> findInSpecificNamedGraph(Node g, Node s, Node p, Node o) {
        return null;
    }

    @Override
    protected Iterator<Quad> findInAnyNamedGraphs(Node s, Node p, Node o) {
        return null;
    }
}
