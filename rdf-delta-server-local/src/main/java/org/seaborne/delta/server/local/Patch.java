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

package org.seaborne.delta.server.local;

import org.apache.jena.graph.Node;
import org.seaborne.delta.Id;
import org.seaborne.patch.PatchHeader;
import org.seaborne.patch.RDFChanges ;
import org.seaborne.patch.RDFPatch ;

/** Holder for an {@link RDFPatch}. This carries with it system information
 * such as where the patch is stored and any local version number. 
 */
public class Patch implements RDFPatch {
    // PatchWrapper
    
    private final RDFPatch patch;
    private final DataSource dataSource;
    private final FileEntry fileEntry;
    
    public Patch(boolean unused, RDFPatch patch, DataSource source, FileEntry entry) {
        this.patch = patch;
        this.dataSource = source;
        this.fileEntry = entry;
    }
    
    public RDFPatch get() { return patch ; }
    
    @Override
    public Node getId() {
        return patch.getId() ;
    }

    @Override
    public Node getPrevious() {
        return patch.getPrevious() ;
    }

    public Id getIdAsId() {
        return Id.fromNode(patch.getId()) ;
    }

    public Id getPreviousIdAsId() {
        return Id.fromNode(patch.getPrevious()) ;
    }

    @Override
    // XXX "apply" - wrong name?
    public void apply(RDFChanges changes) {
        patch.apply(changes) ;
    }
    
    @Override
    public PatchHeader header() {
        return patch.header() ;
    }

    public void play(RDFChanges changes) {
        patch.apply(changes) ;
    }

    public DataSource getDataSource() {
        return dataSource;
    }

    public long getSourceVersion() {
        return fileEntry.version;
    }
}