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

import org.apache.jena.graph.Graph;
import org.apache.jena.graph.GraphUtil;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.TransactionHandler;
import org.apache.jena.graph.Triple;
import org.apache.jena.graph.impl.TransactionHandlerBase;
import org.apache.jena.shared.PrefixMapping;
import org.apache.jena.sparql.graph.GraphWrapper;
import org.seaborne.patch.RDFChanges;

/**
 * Connect a {@link Graph} with {@link RDFChanges}. All operations on the {@link Graph}
 * that cause changes have the change sent to the {@link RDFChanges}.
 * <p>
 * The graph name is settable and
 * <p>
 * Use {@link GraphRealChanges} to get a graph that logs only changes that have a real
 * effect - that makes the changes log reversible (play delete for each add) to undo a
 * sequence of changes.
 * <p>
 * If the graph is a graph from a dataset and the same graph name is to be used, then
 * instead of this class, get a graph from {@link DatasetGraphChanges}. Using this class
 * does not preseve the graph name, it uses the name explciit set in the constructor.
 * 
 * @see DatasetGraphChanges
 * @see RDFChanges
 */
public class GraphChanges extends GraphWrapper /*implements GraphWithPerform*/ // Or WrappedGraph
{
    private final RDFChanges changes;
    protected final Node graphName;
    private final PrefixMapping prefixMapping;
    private final TransactionHandler transactionHandler;

    /** 
     * Send changes to a graph to a {@link RDFChanges} with null for the graph name. 
     */
    public GraphChanges(Graph graph, RDFChanges changes) {
        this(graph, null, changes);
    }
    
    /**
     * Send changes to a graph to a {@link RDFChanges} with the specified graph name.
     * The graph name may be null for "no name".
     */
    public GraphChanges(Graph graph, Node graphName, RDFChanges changes) {
        super(graph);
        this.graphName = graphName;
        this.changes = changes;
        this.prefixMapping = new PrefixMappingChanges(graph.getPrefixMapping(), graphName, changes);
        this.transactionHandler = new TransactionHandlerMonitor(graph.getTransactionHandler(), changes);
    }
    
    @Override
    public void add(Triple t) { 
        changes.add(graphName,  t.getSubject(), t.getPredicate(), t.getObject());
        super.add(t);
    }

    @Override
    public void delete(Triple t) {
        changes.delete(graphName,  t.getSubject(), t.getPredicate(), t.getObject());
        super.delete(t);
    }

//    @Override public void performAdd( Triple t )    { add(t); }
//    @Override public void performDelete( Triple t ) { delete(t); }
    
    @Override
    public void clear() {
        remove(Node.ANY, Node.ANY, Node.ANY);
    }

    @Override
    public void remove(Node s, Node p, Node o) {
        // Convert to calls to delete. 
        GraphUtil.remove(this, s, p, o);
    }
    
    
    @Override
    public TransactionHandler getTransactionHandler() {
        return transactionHandler;
    }
    
    static class TransactionHandlerMonitor extends TransactionHandlerBase {

        private final TransactionHandler handler;
        private final RDFChanges changes;

        public TransactionHandlerMonitor(TransactionHandler handler, RDFChanges changes) {
            this.handler = handler;
            this.changes = changes;
        }
        
        @Override
        public boolean transactionsSupported() {
            return handler.transactionsSupported();
        }

        @Override
        public void begin() {
            changes.txnBegin();
            handler.begin();
        }

        @Override
        public void commit() {
            // Must be in this order - log the commit, then do it.
            // Recovery from the log wil replay the changes do the log is more important
            // than the graph as the source of truth.
            handler.commit();
            changes.txnCommit();
        }

        @Override
        public void abort() {
            // Must be in this order - record the abort then do it
            // (a crash between will cause an abort).
            changes.txnAbort();
            handler.abort();
        }
    }
    
    @Override
    public PrefixMapping getPrefixMapping() {
        return prefixMapping;
    }
}
