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

package org.seaborne.patch.system;

import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.seaborne.patch.RDFChanges;

/**
 * A {@link Graph} and {@link RDFChanges} that check whether a triple change is real or
 * not and only passes the chnage on if the add(triple)/delete(triple) causes an actual
 * change to the graph.
 *
 * @see GraphChanges
 */
public class GraphRealChanges extends GraphChanges
{
    public GraphRealChanges(Graph graph, RDFChanges changes) {
        super(graph, changes);
    }

    public GraphRealChanges(Graph graph, Node graphName, RDFChanges changes) {
        super(graph, graphName, changes);
    }

    @Override
    public void add(Triple t) {
        if ( ! super.contains(t) )
            super.add(t);
    }

    @Override
    public void delete(Triple t) {
        if ( super.contains(t) )
            super.delete(t);
    }
}
