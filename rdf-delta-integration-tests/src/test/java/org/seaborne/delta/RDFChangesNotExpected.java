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

package org.seaborne.delta;

import org.apache.jena.graph.Node;
import org.apache.jena.rdfpatch.RDFChanges;

/** Any call to the class causes a runtime exception */
class RDFChangesNotExpected implements RDFChanges {
    static class NotExpected extends RuntimeException {}

    @Override public void start()           { throw new NotExpected(); }
    @Override public void finish()          { throw new NotExpected(); }

    @Override public void segment()         { throw new NotExpected(); }

    @Override public void header(String field, Node value)        { throw new NotExpected(); }

    @Override public void add(Node g, Node s, Node p, Node o)     { throw new NotExpected(); }
    @Override public void delete(Node g, Node s, Node p, Node o)  { throw new NotExpected(); }

    @Override public void addPrefix(Node graph, String prefix, String uriStr) { throw new NotExpected(); }
    @Override public void deletePrefix(Node graph, String prefix)             { throw new NotExpected(); }

    @Override public void txnBegin()        { throw new NotExpected(); }
    @Override public void txnCommit()       { throw new NotExpected(); }
    @Override public void txnAbort()        { throw new NotExpected(); }
}