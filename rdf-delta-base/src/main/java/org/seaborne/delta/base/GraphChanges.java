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

package org.seaborne.delta.base;

import java.util.Map ;

import org.apache.jena.graph.Graph ;
import org.apache.jena.graph.Node ;
import org.apache.jena.graph.Triple ;
import org.apache.jena.shared.PrefixMapping ;
import org.apache.jena.sparql.graph.GraphWrapper ;
import org.seaborne.patch.RDFChanges ;

// Needed? Or graphView over a dataset?
public class GraphChanges extends GraphWrapper {
    private final RDFChanges changes ;
    private final Graph graph ;
    protected final Node graphName ;

    public GraphChanges(Graph graph, Node graphName, RDFChanges changes) {
        super(graph) ;
        this.graph = graph ;
        this.graphName = graphName ;
        this.changes = changes ;
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

    // TransactionHandler
    
    
    private PrefixMapping pm = null ;
    
    @Override
    public PrefixMapping getPrefixMapping() {
        if ( pm == null ) {
            PrefixMapping pmap = graph.getPrefixMapping() ;
            pm = new PrefixMappingMonitor(pmap) {
                @Override
                protected void set(String prefix, String uri) {
                    changes.addPrefix(graphName, prefix, uri);
                }

                @Override
                protected void remove(String prefix) {
                    changes.deletePrefix(graphName, prefix);
                } 
            } ;
        }
        return pm ;
    }
    
    static class PrefixMappingMonitor implements PrefixMapping {
        
        private final PrefixMapping pmap ;

        public PrefixMappingMonitor(PrefixMapping pmap) { this.pmap = pmap ; }

        protected PrefixMapping get() { return pmap ; }
        
        // Event triggers.
        protected void set(String prefix, String uri) { }

        //protected String get(String prefix) { }

        protected void remove(String prefix) { }
        
        @Override
        public PrefixMapping setNsPrefix(String prefix, String uri) {
            set(prefix, uri) ;
            return get().setNsPrefix(prefix, uri) ;
        }

        @Override
        public PrefixMapping removeNsPrefix(String prefix) {
            remove(prefix); 
            return get().removeNsPrefix(prefix) ;
        }

        @Override
        public PrefixMapping clearNsPrefixMap() {
            get().getNsPrefixMap().forEach((prefix,uri)->remove(prefix)) ;
            return get().clearNsPrefixMap() ;
        }

        @Override
        public PrefixMapping setNsPrefixes(PrefixMapping other) {
            other.getNsPrefixMap().forEach((p,u) -> set(p,u)) ;
            return get().setNsPrefixes(other) ;
        }

        @Override
        public PrefixMapping setNsPrefixes(Map<String, String> map) {
            map.forEach((p,u) -> set(p,u)) ;
            return get().setNsPrefixes(map) ;
        }

        @Override
        public PrefixMapping withDefaultMappings(PrefixMapping map) {
            // fake
            map.getNsPrefixMap().forEach((p,u) -> set(p,u)) ;
            return get().withDefaultMappings(map) ;
        }

        @Override
        public String getNsPrefixURI(String prefix) {
            return get().getNsPrefixURI(prefix) ;
        }

        @Override
        public String getNsURIPrefix(String uri) {
            return get().getNsURIPrefix(uri) ;
        }

        @Override
        public Map<String, String> getNsPrefixMap() {
            return get().getNsPrefixMap() ;
        }

        @Override
        public String expandPrefix(String prefixed) {
            return get().expandPrefix(prefixed) ;
        }

        @Override
        public String shortForm(String uri) {
            return get().shortForm(uri) ;
        }

        @Override
        public String qnameFor(String uri) {
            return get().qnameFor(uri) ;
        }

        @Override
        public PrefixMapping lock() {
            return get().lock() ;
        }

        @Override
        public boolean samePrefixMappingAs(PrefixMapping other) {
            return get().samePrefixMappingAs(other) ;
        }
        
    }
}
