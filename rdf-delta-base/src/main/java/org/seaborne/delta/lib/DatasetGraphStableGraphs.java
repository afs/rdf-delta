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

package org.seaborne.delta.lib;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.DatasetGraphWrapper;
import org.apache.jena.sparql.core.Quad;

/**
 * A {@link DatasetGraph} that returns the same Java object for each graph when the graph
 * is request from the dataset. Otherwise, different GraphView java objects may be
 * returned by different calls for the same graph. However, it needs to retain the java
 * objects so risks growing. When these java objects are GrapView, they are small. See
 * {{@link #expel} and {@link #reset()}.
 */
public class DatasetGraphStableGraphs extends DatasetGraphWrapper {
    private Graph defaultGraph;
    private Map<Node, Graph> namedGraphs = new ConcurrentHashMap<>();

    public DatasetGraphStableGraphs(DatasetGraph dsg) {
        super(dsg);
        defaultGraph = get().getDefaultGraph();
    }

    @Override
    public Graph getDefaultGraph() {
        return defaultGraph;
    }

    @Override
    public Graph getGraph(Node graphName) {
        return namedGraphs.computeIfAbsent(graphName, (gn)->get().getGraph(gn));
    }

    /**
     * Forget about a graph.
     *
     * If called with argument "null", it will reset the
     * cached copy of the default graph.
     */
    public void expel(Node graphName) {
        if ( graphName == null || Quad.isDefaultGraph(graphName) )
            defaultGraph = get().getDefaultGraph();
        namedGraphs.remove(graphName);
    }


    /** Start again. */
    public void reset() {
        defaultGraph = get().getDefaultGraph();
        namedGraphs.clear();
    }
}
