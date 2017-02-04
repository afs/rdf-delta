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

package org.seaborne.patch;

import java.util.Map;
import java.util.function.BiConsumer;

import org.apache.jena.graph.Node;

/** RDF Patch header */ 
public class PatchHeader {
    // Currently, a read-only wrapper on a Map
    private Map<String, Node> header;

    public PatchHeader(Map<String, Node> header) {
        // Isolate => copy.
        this.header = header;
    }
    
    public Node getId() { 
        return get(RDFPatch.ID) ;
    }

    public Node getParent() {
        return get(RDFPatch.PARENT) ;
    }
    
    public Node get(String field) {
        return header.get(field) ;
    }

    public void forEach(BiConsumer<String, Node> action) {
        header.forEach(action);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((header == null) ? 0 : header.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if ( this == obj )
            return true;
        if ( obj == null )
            return false;
        if ( getClass() != obj.getClass() )
            return false;
        PatchHeader other = (PatchHeader)obj;
        if ( header == null ) {
            if ( other.header != null )
                return false;
        } else if ( !header.equals(other.header) )
            return false;
        return true;
    }
}
