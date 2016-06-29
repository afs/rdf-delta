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

import java.util.Iterator ;

import org.apache.jena.graph.Graph ;
import org.apache.jena.graph.Node ;
import org.apache.jena.query.ReadWrite ;
import org.apache.jena.sparql.core.DatasetGraph ;
import org.apache.jena.sparql.core.DatasetGraphWrapper ;
import org.apache.jena.sparql.core.Quad ;

public class DatasetGraphChanges extends DatasetGraphWrapper {
    
    protected StreamChanges monitor ;

    public DatasetGraphChanges(DatasetGraph dsg, StreamChanges monitor) { 
        super(dsg) ; 
        this.monitor = monitor ;
    }
    
    @Override
    public void add(Quad quad) {
        add(quad.getGraph(), quad.getSubject(), quad.getPredicate(), quad.getObject());
    }
    
    @Override
    public void delete(Quad quad) {
        delete(quad.getGraph(), quad.getSubject(), quad.getPredicate(), quad.getObject());
    }
    
    @Override
    public void add(Node g, Node s, Node p, Node o) {
        monitor.add(g, s, p, o);
        super.add(g, s, p, o) ;
    }
    
    @Override
    public void delete(Node g, Node s, Node p, Node o) {
        monitor.delete(g, s, p, o);
        super.delete(g, s, p, o) ;
    }
    
    @Override
    public Graph getDefaultGraph()
    { return new GraphChanges(get().getDefaultGraph(), null, monitor) ; }

    @Override
    public Graph getGraph(Node graphNode)
    { return new GraphChanges(get().getGraph(graphNode), graphNode, monitor) ; }
    
    private static final int DeleteBufferSize = 10000 ;
    @Override
    /** Simple implementation but done without assuming iterator.remove() */
    public void deleteAny(Node g, Node s, Node p, Node o) {
        Quad[] buffer = new Quad[DeleteBufferSize];
        while (true) {
            Iterator<Quad> iter = find(g, s, p, o);
            // Get a slice
            int len = 0;
            for ( ; len < DeleteBufferSize ; len++ ) {
                if ( !iter.hasNext() )
                    break;
                buffer[len] = iter.next();
            }
            // Delete them.
            for ( int i = 0 ; i < len ; i++ ) {
                delete(buffer[i]);
                buffer[i] = null;
            }
            // Finished?
            if ( len < DeleteBufferSize )
                break;
        }
    }
    
    @Override
    public void begin(ReadWrite readWrite) {
        super.begin(readWrite);
        monitor.txnBegin(readWrite);
    }

    @Override
    public void commit() {
        // Assume commit will work - signal first.
        monitor.txnCommit();
        super.commit();
    }
    
    @Override
    public void abort() {
        // Assume abort will work - signal first.
        super.abort();
        monitor.txnAbort();
    }

//    @Override
//    public void end() {
//        super.end();
//    }
    
}
