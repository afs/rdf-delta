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

import java.util.HashMap;
import java.util.Map;

import org.apache.jena.graph.Node;
import org.seaborne.patch.changes.RDFChangesWrapper;

/**
 * An {@link RDFPatch} where the previous field is overridden.
 */
public class RDFPatchAltPrevious extends RDFPatchWrapper {
    private final Node previous;

    public RDFPatchAltPrevious(RDFPatch body, Node previous) {
        super(body);
        this.previous = previous;
    }
    
    @Override
    public Node getHeader(String field) {
        switch(field) {
            case RDFPatchConst.PREV:
            case RDFPatch.PREVIOUS:
                return previous;
            default:
                return get().getHeader(field);
        }
    }
    
    @Override
    public PatchHeader header() {
        Map<String, Node> header = new HashMap<>();
        super.header().forEach((k,v)->{
            Node n = getHeader(k);
            if ( n != null )
                header.put(k, getHeader(k));
        });
        return new PatchHeader(header);
    }

    @Override
    public void apply(RDFChanges base) {
        RDFChanges changes = new RDFChangesWrapper(base) {
            @Override
            public void header(String field, Node value) {
                Node v = getHeader(field);
                base.header(field, value);
            }
        };
        
        get().apply(changes);
    }
}